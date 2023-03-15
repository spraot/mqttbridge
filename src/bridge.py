#!/usr/bin/env python3

"""A MQTT to InfluxDB Bridge
This script receives MQTT data and saves those to InfluxDB.
"""

from datetime import datetime, timedelta, timezone
from math import floor
import numbers
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
from jsonpath_ng import parse
import paho
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from payload_parser import PayloadParser

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
    httpsink_url = None
    mqtt_base_topic = 'influxbridge'
    state_topic = ''
    write_apis = []

    schemas = []
    
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

        for key in ['mqtt_base_topic', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'httpsink_url', 'client_id', 'influxdb', 'influxdb2']:
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

            schema = {
                'mqtt_topic': re.sub(r'\{\w+\}', '+', mqtt_topic),
                'regex': '^'+re.sub(r'\{\w+\}', '([^/]+)', mqtt_topic)+'$',
                'tags': get('tags', {}),
                'topic_tags': [m.group(1) for m in re.finditer(r'\{(\w+)\}', mqtt_topic)],
                'measurement': get('measurement', 'from_json_keys'),
                'fields': get('fields', 'value'),
                'key_map': get('key_map', {}),
                'value_map': get('value_map', {}),
                'last_dt': datetime(1,1,1)
            }

            schema['regex'] = re.compile(schema['regex'])

            try:
                schema['last_points'] = [Point.from_dict(x) for x in input['last_points']]
            except KeyError:
                pass

            try:
                schema['repeat_last'] = input['repeat_last']
                if not isinstance(schema['repeat_last'], numbers.Number) or schema['repeat_last'] <= 0:
                    raise ValueError('repeat_last must be a number larger than 0')
            except KeyError:
                pass

            try:
                schema['jsonpath'] = parse(input['jsonpath'])
            except KeyError:
                pass
            
            schema['json_keys_include'] = get_string_or_array(input, 'json_keys_include', False)
            schema['json_keys_exclude'] = get_string_or_array(input, 'json_keys_exclude', False)

            if schema['json_keys_include'] and schema['json_keys_exclude']:
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
            
            schema['integer_keys'] = compile_regexes('integer_keys')
            schema['float_keys'] = compile_regexes('float_keys')
            schema['boolean_keys'] = compile_regexes('boolean_keys')
            schema['string_keys'] = compile_regexes('string_keys')
            
            schema['tags_from_json'] = get_string_or_array(input, 'tags_from_json', False)
            if not schema['json_keys_include']:
                schema['json_keys_exclude'].extend(schema['tags_from_json'])

            if schema['tags_from_json'] and schema['measurement'] == 'from_json_keys':
                raise ValueError('Only one of tags_from_json and measurement=from_json_keys can be given')

            if schema['fields'] == 'from_json_keys' and schema['measurement'] == 'from_json_keys':
                raise ValueError('Only one of fields=from_json_keys and measurement=from_json_keys can be given')
            
            logging.debug(safe_serialize(schema))

            self.schemas.append(schema)

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
            now = datetime.now()
            for schema in self.schemas:
                try:
                    total_seconds = (now - schema['last_dt']).total_seconds()
                    if total_seconds >= schema['repeat_last']:
                        self._send_points(schema['last_points'], datetime.now(timezone.utc))
                        schema['last_dt'] = schema['last_dt'] + timedelta(seconds=schema['repeat_last']*floor(total_seconds / schema['repeat_last']))
                except Exception as e:
                    if not isinstance(e, KeyError):
                        logging.exception('When repeating last point, encountered error '+e)

            self.killer.kill_now.wait(0.5)

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

            for schema in self.schemas:
                logging.debug(f"Subscribing to topic: {schema['mqtt_topic']}")
            self.mqttclient.subscribe([(t['mqtt_topic'], 2) for t in self.schemas])

            self.mqttclient.publish(self.state_topic, payload='{"state": "online"}', qos=1, retain=True)
            self.mqttclient.will_set(self.state_topic, payload='{"state": "offline"}', qos=1, retain=True)
        except Exception as e:
            logging.error('Encountered error in mqtt connect handler: '+str(e))

    def mqtt_on_message(self, client, userdata, msg):
        try:
            payload_as_string = msg.payload.decode('utf-8')
            logging.debug('Received MQTT message on topic: ' + msg.topic + ', payload: ' + payload_as_string + ', retained: ' + str(msg.retain))

            for schema in self.schemas:
                parser = PayloadParser(schema)
                parser.parse(msg.topic, payload_as_string)

                if not msg.retain:
                    self._send_points(parser.points)

                schema['last_points'] = parser.points
                schema['last_dt'] = datetime.now()

        except Exception as e:
            logging.exception('Encountered error in mqtt message handler for topic "{}": {}'.format(msg.topic, e))

    def _send_points(self, points, dt=None):
        for point in points:
            if dt:
                point.time(dt)

        point_strs = '\n'.join([point.to_line_protocol() for point in points])

        logging.debug('Adding data points to db: '+point_strs)
        if self.httpsink_url:
            self.http.request('POST', self.httpsink_url, body=point_strs)

        for db, write_api in zip(self.influxdb2, self.write_apis):
            write_api.write(bucket=db['bucket'], org=db['org'], record=point_strs)

    def _init_influxdb_database(self):
        # self.write_apis = [client.write_api(write_options=SYNCHRONOUS) for client in self.influxdb2_clients]
        self.write_apis = [client.write_api() for client in self.influxdb2_clients]

if __name__ == '__main__':
    mqttBridge =  MqttBridge()
    mqttBridge.start()
