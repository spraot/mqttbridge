#!/usr/bin/env python3

"""A MQTT to InfluxDB Bridge
This script receives MQTT data and saves those to InfluxDB.
"""

from datetime import datetime, timezone
import os
import sys
import re
import json
from urllib3 import PoolManager
from urllib3.util import Retry
import yaml
import threading
import signal
import logging
import atexit
from flatdict import FlatDict
from jsonpath_ng import parse
import paho
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

class GracefulKiller:
  def __init__(self):
    self.kill_now = threading.Event()
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, *args):
    self.kill_now.set()
    
def safe_serialize(obj):
  default = lambda o: f"<<non-serializable: {type(o).__qualname__}>>"
  return json.dumps(obj, default=default)

class MqttBridge():
    config_file = 'config.yml'
    mqtt_server_ip = 'localhost'
    mqtt_server_port = 1883
    mqtt_server_user = ''
    mqtt_server_password = ''
    client_id = ''
    mqtt_base_topic = 'influxbridge'
    state_topic = ''
    write_apis = []

    topics = []
    
    def __init__(self):
        logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'), format='%(asctime)s;<%(levelname)s>;%(message)s')
        logging.info('Init')

        self.killer = GracefulKiller()

        retries = Retry(total=5,
                        backoff_factor=4,
                        status_forcelist=[ 500, 502, 503, 504 ])
        self.http = PoolManager(retries=retries)

        self.influxdb2 = []

        if len(sys.argv) > 1:
            self.config_file = sys.argv[1]

        self.load_config()

        #influxdb init
        self.influxdb2_clients = []
        for db in self.influxdb2:
            logging.info('Influx v2 server at {}'.format(db['url']))
            client = InfluxDBClient(**{k: v for k, v in db.items() if k != 'bucket'})
            self.influxdb2_clients.append(client)
        
        #MQTT init
        logging.info('MQTT server at {}:{}'.format(self.mqtt_server_ip, self.mqtt_server_port))
        self.mqttclient = mqtt.Client(client_id=self.client_id, protocol=paho.mqtt.client.MQTTv5)
        self.mqttclient.on_connect = self.mqtt_on_connect
        self.mqttclient.on_message = self.mqtt_on_message

        #Register program end event
        atexit.register(self.programend)

        logging.info('init done')

    def load_config(self):
        logging.info('Reading config from '+self.config_file)

        def get_string_or_array(cfg, key, throw=True):
            if not throw and key not in cfg:
                return []
            
            val = cfg[key]
            if not isinstance(val, list):
                return [val]
            return val
        
        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)

        for key in ['mqtt_base_topic', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'httpsink_url', 'client_id']:
            try:
                val = config[key]
                if key.startswith('influxdb'):
                    if not isinstance(val, list):
                        val = [val]
                    if key == 'influxdb':
                        key += '2'
                    self.__getattribute__(key).extend(val)
                else:
                    self.__setattr__(key, val)

                    if key not in ['mqtt_server_password']:
                        logging.debug(f"{key}={val}")
            except KeyError:
                pass

        self.state_topic = self.mqtt_base_topic + '/state'

        logging.debug('Found {} influx db sinks'.format(len(self.influxdb2)))

        for input in config['input']:
            mqtt_topic = input['topic']

            def get(key, default):
                try:
                    return input[key]
                except KeyError:
                    return default

            topic = {
                'mqtt_topic': re.sub(r'\{\w+\}', '+', mqtt_topic),
                'regex': '^'+re.sub(r'\{\w+\}', '([^/]+)', mqtt_topic)+'$',
                'tags': get('tags', {}),
                'topic_tags': [m.group(1) for m in re.finditer(r'\{(\w+)\}', mqtt_topic)],
                'measurement': get('measurement', 'from_json_keys'),
                'fields': get('fields', 'value'),
                'key_map': get('key_map', {}),
                'value_map': get('value_map', {})
            }

            topic['regex'] = re.compile(topic['regex'])

            try:
                topic['jsonpath'] = parse(input['jsonpath'])
            except KeyError:
                pass
            
            topic['json_keys_include'] = get_string_or_array(input, 'json_keys_include', False)
            topic['json_keys_exclude'] = get_string_or_array(input, 'json_keys_exclude', False)

            if topic['json_keys_include'] and topic['json_keys_exclude']:
                raise ValueError('Only one of json_keys_include and json_keys_exclude can be given')

            def compile_regexes(typ):
                re_arr = [re.compile(s) for s in [
                        *get_string_or_array(config, typ, False),
                        *get_string_or_array(input, typ, False)
                    ]]
                def do_test(key):
                    for r in re_arr:
                        if r.fullmatch(key):
                            return True
                    return False
                return do_test
            
            topic['integer_keys'] = compile_regexes('integer_keys')
            topic['float_keys'] = compile_regexes('float_keys')
            topic['boolean_keys'] = compile_regexes('boolean_keys')
            topic['string_keys'] = compile_regexes('string_keys')
            
            topic['tags_from_json'] = get_string_or_array(input, 'tags_from_json', False)
            if not topic['json_keys_include']:
                topic['json_keys_exclude'].extend(topic['tags_from_json'])

            if topic['tags_from_json'] and topic['measurement'] == 'from_json_keys':
                raise ValueError('Only one of tags_from_json and measurement=from_json_keys can be given')

            if topic['fields'] == 'from_json_keys' and topic['measurement'] == 'from_json_keys':
                raise ValueError('Only one of fields=from_json_keys and measurement=from_json_keys can be given')
            
            logging.debug(safe_serialize(topic))

            self.topics.append(topic)

    def start(self):
        logging.info('starting')

        self._init_influxdb_database()

        connect_args = {}

        if self.client_id:
            connect_args['clean_start'] = False

        #MQTT startup
        logging.info('Starting MQTT client')
        self.mqttclient.username_pw_set(self.mqtt_server_user, password=self.mqtt_server_password)
        self.mqttclient.connect_async(self.mqtt_server_ip, self.mqtt_server_port, 60, **connect_args)
        self.mqttclient.loop_start()
        logging.info('MQTT client started')

        logging.info('Starting main thread')
        self.main()

    def main(self):
        logging.info('started')
        while not self.killer.kill_now.is_set():
            self.killer.kill_now.wait(10)

        logging.info('stopping')
        self.mqttclient.disconnect()
        for write_api in self.write_apis:
            write_api.close()
        sys.exit()

    def programend(self):
        logging.info('stopped')

    def mqtt_on_connect(self, client, userdata, flags, reasonCode, properties):
        try:
            logging.info('MQTT client connected with reason code '+str(reasonCode))

            for topic in self.topics:
                logging.debug(f"Subscribing to topic: {topic['mqtt_topic']}")
            client.subscribe([(t['mqtt_topic'], 2) for t in self.topics])

            self.mqttclient.publish(self.state_topic, payload='{"state": "online"}', qos=1, retain=True)
            self.mqttclient.will_set(self.state_topic, payload='{"state": "offline"}', qos=1, retain=True)
        except Exception as e:
            logging.error('Encountered error in mqtt connect handler: '+str(e))

    def mqtt_on_message(self, client, userdata, msg):
        dt = datetime.now(timezone.utc)
        try:
            payload_as_string = msg.payload.decode('utf-8')
            logging.debug('Received MQTT message on topic: ' + msg.topic + ', payload: ' + payload_as_string + ', retained: ' + str(msg.retain))

            if msg.retain:
                return

            for topic in self.topics:
                match = topic['regex'].match(msg.topic)
                if match:
                    tags = {**topic['tags']}
                    for i, tag in enumerate(topic['topic_tags']):
                        tags[tag] = match.group(i+1)

                    if 'jsonpath' in topic:
                        payload_json = json.loads(payload_as_string)
                        for m in topic['jsonpath'].find(payload_json):
                            self._parse_payload(topic, m.value, tags, dt, True)
                    else:
                        self._parse_payload(topic, payload_as_string, tags, dt, False)

        except Exception as e:
            logging.exception('Encountered error in mqtt message handler for topic "{}": {}'.format(msg.topic, e))

    def _parse_payload(self, topic, payload, tags, dt, already_json_parsed=False):
        tags = {**tags}

        def map_key(val):
            try:
                return topic['key_map'][val]
            except (KeyError, TypeError):
                return val

        def map_value(val):
            try:
                return topic['value_map'][val]
            except (KeyError, TypeError):
                return val
            
        def include_filter(val):
            return not topic['json_keys_include'] or val in topic['json_keys_include']
            
        def exclude_filter(val):
            return not topic['json_keys_exclude'] or val not in topic['json_keys_exclude']
        
        def apply_type(key, val):
            try:
                if topic['integer_keys'](key):
                    logging.debug(f'{key} will be typecast to int')
                    return int(val)
            except KeyError:
                pass

            try:
                if topic['string_keys'](key):
                    logging.debug(f'{key} will be typecast to str')
                    return str(val)
            except KeyError:
                pass
            
            try:
                if topic['float_keys'](key):
                    logging.debug(f'{key} will be typecast to float')
                    return float(val)
            except KeyError:
                pass
            
            try:
                if isinstance(val, bool) or topic['boolean_keys'](key):
                    logging.debug(f'{key} will be typecast to bool')
                    return bool(val)
            except KeyError:
                pass

            if isinstance(val, dict):
                tags.update({k: v for k, v in val.items() if k in topic['tags_from_json']})
                return {map_key(k): apply_type(k, map_value(v)) for k, v in val.items() if include_filter(k) and exclude_filter(k)}
            
            logging.debug(f'{key} will be typecast to float')
            return float(val)

        if topic['measurement'] == 'from_json_keys' or topic['fields'] == 'from_json_keys':
            if not already_json_parsed:
                try:
                    payload = json.loads(payload)
                except json.decoder.JSONDecodeError:
                    raise ValueError('could not parse payload as JSON')

            if not isinstance(payload, dict):
                raise ValueError('payload is not a JSON object, could not parse with from_json_keys')
            
        payload = apply_type('@root', map_value(payload))

        if topic['measurement'] == 'from_json_keys':
            for k, v in payload.items():
                self._send_sensor_data_to_influxdb(k, tags, v, dt)
        else:
            self._send_sensor_data_to_influxdb(topic['measurement'], tags, map_value(payload), dt)

    def _send_sensor_data_to_influxdb(self, measurement, tags, value, dt):
        point = Point(measurement)
        point.time(dt, write_precision='ms')
        for k,v in tags.items():
            point.tag(k, v)
        if isinstance(value, dict):
            point._fields.update(FlatDict(value, '.').as_dict())
        else:
            point.field('value', value)

        point_str = point.to_line_protocol()
        logging.debug('Adding data point to db: '+point_str)
        if self.httpsink_url:
            self.http.request('POST', self.httpsink_url, body=point_str)

        for db, write_api in zip(self.influxdb2, self.write_apis):
            write_api.write(bucket=db['bucket'], org=db['org'], record=point)

    def _init_influxdb_database(self):
        self.write_apis = [client.write_api(write_options=WriteOptions(batch_size=100,
                                                      flush_interval=30_000,
                                                      jitter_interval=2_000,
                                                      retry_interval=5_000,
                                                      max_retries=7,
                                                      max_retry_delay=3_000_000,
                                                      max_retry_time=6_000_000,
                                                      exponential_base=3)) for client in self.influxdb2_clients]

if __name__ == '__main__':
    mqttBridge =  MqttBridge()
    mqttBridge.start()
