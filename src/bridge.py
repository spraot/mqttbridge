#!/usr/bin/env python3

"""A MQTT to InfluxDB Bridge
This script receives MQTT data and saves those to InfluxDB.
"""

import os
import sys
import re
import json
import yaml
import time
import logging
import atexit
from jsonpath_ng import parse
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


class MqttBridge():
    config_file = 'config.yml'
    mqtt_server_ip = 'localhost'
    mqtt_server_port = 1883
    mqtt_server_user = ''
    mqtt_server_password = ''
    mqtt_base_topic = 'influxbridge'
    state_topic = ''

    topics = []
    
    def __init__(self):
        logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'), format='%(asctime)s;<%(levelname)s>;%(message)s')
        logging.info('Init')

        self.influxdb = []

        if len(sys.argv) > 1:
            self.config_file = sys.argv[1]

        self.load_config()

        #influxdb init
        self.influxdb_clients = []
        for db in self.influxdb:
            logging.info('Influx server at {}'.format(db['url']))
            client = InfluxDBClient(**{k: v for k, v in db.items() if k != 'bucket'})
            self.influxdb_clients.append(client)
        
        #MQTT init
        logging.info('MQTT server at {}:{}'.format(self.mqtt_server_ip, self.mqtt_server_port))
        self.mqttclient = mqtt.Client()
        self.mqttclient.on_connect = self.mqtt_on_connect
        self.mqttclient.on_message = self.mqtt_on_message

        #Register program end event
        atexit.register(self.programend)

        logging.info('init done')

    def load_config(self):
        logging.info('Reading config from '+self.config_file)

        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)

        for key in ['mqtt_base_topic', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'influxdb']:
            try:
                self.__setattr__(key, config[key])
            except KeyError:
                pass

        self.state_topic = self.mqtt_base_topic + '/state'

        if not isinstance(self.influxdb, list):
            self.influxdb = [self.influxdb]

        logging.debug('Found {} influx db sinks'.format(len(self.influxdb)))

        for input in config['input']:
            mqtt_topic = input['topic']

            def get(key, default):
                try:
                    return input[key]
                except KeyError:
                    return default

            def get_string_or_array(key):
                val = input[key]
                if not isinstance(val, list):
                    return [val]
                return val

            topic = {
                'mqtt_topic': re.sub(r'\{\w+\}', '+', mqtt_topic),
                'regex': '^'+re.sub(r'\{\w+\}', '([^/]+)', mqtt_topic)+'$',
                'tags': get('tags', {}),
                'topic_tags': [m.group(1) for m in re.finditer(r'\{(\w+)\}', mqtt_topic)],
                'measurement': get('measurement', 'from_json_keys'),
                'key_map': get('key_map', {}),
                'value_map': get('value_map', {})
            }

            logging.debug(json.dumps(topic))

            topic['regex'] = re.compile(topic['regex'])

            try:
                topic['jsonpath'] = parse(input['jsonpath'])
            except KeyError:
                pass
            
            try:
                topic['json_keys_include'] = get_string_or_array('json_keys_include')
            except KeyError:
                pass
            
            try:
                topic['json_keys_exclude'] = get_string_or_array('json_keys_exclude')
            except KeyError:
                pass

            if 'json_keys_include' in topic and 'json_keys_exclude' in topic:
                raise ValueError('Only one of json_keys_include and json_keys_exclude can be given')

            def compile_regexes(typ):
                re_arr = [re.compile(s) for s in get_string_or_array(typ)]
                def do_test(key):
                    for r in re_arr:
                        if r.fullmatch(key):
                            return True
                    return False
                return do_test
            
            try:
                topic['integer_keys'] = compile_regexes('integer_keys')
            except KeyError:
                topic['integer_keys'] = lambda key: False
            
            try:
                topic['string_keys'] = compile_regexes('string_keys')
            except KeyError:
                topic['string_keys'] = lambda key: False
            
            self.topics.append(topic)

    def start(self):
        logging.info('starting')

        self._init_influxdb_database()

        #MQTT startup
        logging.info('Starting MQTT client')
        self.mqttclient.username_pw_set(self.mqtt_server_user, password=self.mqtt_server_password)
        self.mqttclient.connect(self.mqtt_server_ip, self.mqtt_server_port, 60)
        logging.info('MQTT client started')

        self.mqttclient.loop_forever()

    def programend(self):
        logging.info('stopping')

        if self.state_topic:
            self.mqttclient.publish(self.state_topic, payload='stopped', qos=0, retain=True)   

        self.mqttclient.disconnect()
        time.sleep(0.5)
        logging.info('stopped')

    def mqtt_on_connect(self, client, userdata, flags, rc):
        try:
            logging.info('MQTT client connected with result code '+str(rc))

            for topic in self.topics:
                client.subscribe(topic['mqtt_topic'])

            self.mqttclient.publish(self.state_topic, payload='{"state": "online"}', qos=1, retain=True)
            self.mqttclient.will_set(self.state_topic, payload='{"state": "offline"}', qos=1, retain=True)
        except Exception as e:
            logging.error('Encountered error in mqtt connect handler: '+str(e))

    def mqtt_on_message(self, client, userdata, msg):
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
                            self._parse_payload(topic, m.value, tags, True)
                    else:
                        self._parse_payload(topic, payload_as_string, tags, False)

        except Exception as e:
            logging.error('Encountered error in mqtt message handler for topic "{}": {}'.format(msg.topic, str(e)))

    def _parse_payload(self, topic, payload, tags, already_json_parsed=False):
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
            return 'json_keys_include' not in topic or val in topic['json_keys_include']
            
        def exclude_filter(val):
            return 'json_keys_exclude' not in topic or val not in topic['json_keys_exclude']
        
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

            logging.debug(f'{key} will be typecast to float')
            return float(val)

        if topic['measurement'] == 'from_json_keys':
            if not already_json_parsed:
                payload = json.loads(payload)
            for k,v in payload.items():
                try:
                    if include_filter(k) and exclude_filter(k):
                        v = map_value(v)
                        k = map_key(k)
                        try:
                            self._send_sensor_data_to_influxdb(k, tags, apply_type(k, v))
                        except ValueError:
                            pass
                except Exception as e:
                    logging.error(f'Error processing measurement "{k}" from topic "{topic}": {str(e)}')
        else:
            self._send_sensor_data_to_influxdb(topic['measurement'], tags, apply_type(k, map_value(payload)))

    def _send_sensor_data_to_influxdb(self, measurement, tags, value):
        point = Point(measurement)
        point.field('value', value)
        for k,v in tags.items():
            point.tag(k, v)
        logging.debug('Adding data point to db: '+str(point))

        for db, write_api in zip(self.influxdb, self.write_apis):
            write_api.write(bucket=db['bucket'], org=db['org'], record=point)

    def _init_influxdb_database(self):
        self.write_apis = [client.write_api(write_options=SYNCHRONOUS) for client in self.influxdb_clients]

if __name__ == '__main__':
    mqttBridge =  MqttBridge()
    mqttBridge.start()
