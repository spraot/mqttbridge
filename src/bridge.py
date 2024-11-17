#!/usr/bin/env python3

"""A MQTT to InfluxDB Bridge
This script receives MQTT data and saves those to InfluxDB.
"""

from datetime import datetime, timezone
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
from pythonjsonlogger import jsonlogger
import atexit
import paho
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient

from payload_parser import PayloadParser

logger = logging.getLogger()
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

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
        logger.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'), format='%(asctime)s;<%(levelname)s>;%(message)s')
        logger.info('Init')

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
            logger.info('Influx v2 server at {}'.format(db['url']))
            client = InfluxDBClient(**{k: v for k, v in db.items() if k != 'bucket'})
            self.influxdb2_clients.append(client)
        
        #MQTT init
        logger.info('MQTT server at {}:{}'.format(self.mqtt_server_ip, self.mqtt_server_port))
        self.mqttclient = mqtt.Client(client_id=self.client_id, protocol=paho.mqtt.client.MQTTv5)
        self.mqttclient.on_connect = self.mqtt_on_connect
        self.mqttclient.on_message = self.mqtt_on_message

        #Register program end event
        atexit.register(self.programed)

        logger.info('init done')

    def load_config(self):
        logger.info('Reading config from '+self.config_file)

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
                        logger.debug(f"{key}={val}")
            except KeyError:
                pass

        self.state_topic = self.mqtt_base_topic + '/state'

        logger.debug('Found {} influx db sinks'.format(len(self.influxdb2)))

        for input in config['input']:
            mqtt_topic = input['topic']

            def get(key, default):
                try:
                    return input[key]
                except KeyError:
                    return default
                
            topic_meas = mqtt_topic.find('{@measurement}') != -1

            schema = {
                'mqtt_topic': re.sub(r'\{(\w+|\@measurement)\}', '+', mqtt_topic),
                'regex': '^'+re.sub(r'\{(\w+|\@measurement)\}', '([^/]+)', mqtt_topic)+'$',
                'tags': get('tags', {}),
                'topic_matches': [m.group(1) for m in re.finditer(r'\{(\w+|\@measurement)\}', mqtt_topic)],
                'measurement': get('measurement', 'from_topic' if topic_meas else 'from_json_keys'),
                'fields': get('fields', 'value'),
                'key_map': get('key_map', {}),
                'value_map': get('value_map', {}),
                'last': {},
                'repeat_last_expiry': 3600*24*7,
            }

            schema['regex'] = re.compile(schema['regex'])

            try:
                schema['repeat_last'] = input['repeat_last']
                if not isinstance(schema['repeat_last'], numbers.Number) or schema['repeat_last'] <= 0:
                    raise ValueError('repeat_last must be a number larger than 0')
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
            
            logger.debug(safe_serialize(schema))

            self.schemas.append(schema)

    def start(self):
        logger.info('starting')

        self._init_influxdb_database()

        connect_args = {}

        if self.client_id:
            connect_args['clean_start'] = False

        #MQTT startup
        logger.info('Starting MQTT client')
        self.mqttclient.username_pw_set(self.mqtt_server_user, password=self.mqtt_server_password)
        self.mqttclient.connect_async(self.mqtt_server_ip, self.mqtt_server_port, 60, **connect_args)
        self.mqttclient.loop_start()
        logger.info('MQTT client started')

        logger.info('Starting main thread')
        self.main()

    def main(self):
        logger.info('started')
        while not self.killer.kill_now.is_set():
            now = datetime.now()
            n_repeated = 0
            for schema in self.schemas:
                if 'last' in schema:
                    for last in schema['last'].values():
                        try:
                            if 'repeat_last' in schema and schema['repeat_last'] > 0 \
                                and (now - last['dt']).total_seconds() < schema['repeat_last_expiry']:
                                
                                if 'dt_repeated' not in last:
                                    last['dt_repeated'] = last['dt']

                                total_seconds_since_repeat = (now - last['dt_repeated']).total_seconds()
                                if total_seconds_since_repeat >= schema['repeat_last']:
                                    self._send_points(last['points'], datetime.now(timezone.utc))
                                    last['dt'] = now
                                    n_repeated += len(last['points'])
                                    logger.debug(f'Repeating points:'+' - '.join([str(p) for p in last["points"]]))
                                    
                        except Exception as e:
                            last['dt'] = now
                            if not isinstance(e, KeyError):
                                logger.exception(f'When repeating last point for {schema["mqtt_topic"]}, encountered error {str(e)}')

            if n_repeated > 0:
                logger.debug(f'Repeated {n_repeated} data points due to repeat_last setting')

            self.killer.kill_now.wait(1)

        logger.info('stopping')
        self.mqttclient.disconnect()
        for write_api in self.write_apis:
            write_api.close()
        sys.exit()

    def programed(self):
        logger.info('stopped')

    def mqtt_on_connect(self, client, userdata, flags, reasonCode, properties):
        try:
            logger.info('MQTT client connected with reason code '+str(reasonCode))

            for schema in self.schemas:
                logger.debug(f"Subscribing to topic: {schema['mqtt_topic']}")
            self.mqttclient.subscribe([(t['mqtt_topic'], 2) for t in self.schemas])

            self.mqttclient.publish(self.state_topic, payload='{"state": "online"}', qos=1, retain=True)
            self.mqttclient.will_set(self.state_topic, payload='{"state": "offline"}', qos=1, retain=True)
        except Exception as e:
            logger.error('Encountered error in mqtt connect handler: '+str(e))

    def mqtt_on_message(self, client, userdata, msg):
        try:
            payload_as_string = msg.payload.decode('utf-8')
            logger.debug('Received MQTT message on topic: ' + msg.topic + ', payload: ' + payload_as_string + ', retained: ' + str(msg.retain))

            for schema in self.schemas:
                parser = PayloadParser(schema)
                parser.parse(msg.topic, payload_as_string)

                if parser.points:
                    if not msg.retain:
                        self._send_points(parser.points)

                    tag_str = json.dumps(parser.tags)
                    schema['last'][tag_str] = {
                        'points': parser.points,
                        'dt': datetime.now()
                    }

        except Exception as e:
            if e.__class__.__name__ == 'TypeError':
                logger.error('Encountered error in mqtt message handler for topic "{}": {}'.format(msg.topic, e))
            logger.exception('Encountered error in mqtt message handler for topic "{}": {}'.format(msg.topic, e))

    def _send_points(self, points, dt=None):
        for point in points:
            if dt:
                point.time(dt)

        point_strs = ' - '.join([point.to_line_protocol() for point in points])

        logger.debug('Adding data points to db: '+point_strs)
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
