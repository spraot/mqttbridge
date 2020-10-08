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
from typing import NamedTuple
import logging
import numbers
import atexit
from jsonpath_ng import jsonpath, parse
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient


class SensorData(NamedTuple):
    location: str
    measurement: str
    value: float


class MqttBridge():
    config_file = 'config.yml'
    mqtt_server_ip = 'localhost'
    mqtt_server_port = 1883
    mqtt_server_user = ''
    mqtt_server_password = ''
    state_topic = ''

    influxdb_address = 'influxdb'
    influxdb_user = 'root'
    influxdb_password = 'root'
    influxdb_database = 'house_db'

    topics = []
    
    def __init__(self):
        logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'), format='%(asctime)s;<%(levelname)s>;%(message)s')
        logging.info('Init')

        if len(sys.argv) > 1:
            self.config_file = sys.argv[1]

        self.load_config()

        #influxdb init
        self.influxdb_client = InfluxDBClient(self.influxdb_address, 8086, self.influxdb_user, self.influxdb_password, None)
        
        #MQTT init
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

        for key in ['state_topic', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'influxdb_address', 'influxdb_user', 'influxdb_password', 'influxdb_database']:
            try:
                self.__setattr__(key, config[key])
            except KeyError:
                pass

        for input in config['input']:
            mqtt_topic = input['topic']

            def get(key, default):
                try:
                    return input[key]
                except KeyError:
                    return default

            topic = {
                'mqtt_topic': re.sub(r'\{\w+\}', '+', mqtt_topic),
                'regex': re.sub(r'\{\w+\}', '([^/]+)', mqtt_topic),
                'tags': get('tags', {}),
                'topic_tags': [m.group(1) for m in re.finditer(r'\{(\w+)\}', mqtt_topic)],
                'measurement': get('measurement', 'from_json_keys'),
                'value_map': get('value_map', {})
            }

            logging.debug(json.dumps(topic))

            topic['regex'] = re.compile(topic['regex'])

            try:
                topic['jsonpath'] = parse(input['jsonpath'])
            except KeyError:
                pass
            
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

            if self.state_topic:
                self.mqttclient.publish(self.state_topic, payload='running', qos=0, retain=True)
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
            logging.error('Encountered error in mqtt message handler: '+str(e))

    def _parse_payload(self, topic, payload, tags, already_json_parsed=False):
        def map_value(val):
            try:
                return topic['value_map'][val]
            except KeyError:
                return val

        if topic['measurement'] == 'from_json_keys':
            if not already_json_parsed:
                payload = json.loads(payload)
            for k,v in payload.items():
                v = map_value(v)
                if isinstance(v, numbers.Number):
                    self._send_sensor_data_to_influxdb(k, tags, v)
        else:
            self._send_sensor_data_to_influxdb(topic['measurement'], tags, map_value(payload))

    def _send_sensor_data_to_influxdb(self, measurement, tags, value):
        json_body = [
            {
                'measurement': measurement,
                'tags': tags,
                'fields': {
                    'value': float(value)
                }
            }
        ]
        logging.debug('Adding data point to db: '+json.dumps(json_body))
        self.influxdb_client.write_points(json_body)

    def _init_influxdb_database(self):
        databases = self.influxdb_client.get_list_database()
        if len(list(filter(lambda x: x['name'] == self.influxdb_database, databases))) == 0:
            self.influxdb_client.create_database(self.influxdb_database)
        self.influxdb_client.switch_database(self.influxdb_database)

if __name__ == '__main__':
    mqttBridge =  MqttBridge()
    mqttBridge.start()
