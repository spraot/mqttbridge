import json
import logging
from math import nan
from datetime import datetime, timezone
from influxdb_client import Point
from flatdict import FlatDict

class PayloadParser:
    tags = {}
    def __init__(self, schema) -> None:
        self.points = []
        self.dt = datetime.now(timezone.utc)
        self.schema = schema

    def parse(self, topic, payload):
        match = self.schema['regex'].match(topic)
        if match:
            self.tags = {**self.schema['tags']}
            for i, tag in enumerate(self.schema['topic_tags']):
                self.tags[tag] = match.group(i+1)

            self._parse_payload(payload, False)

    def _parse_payload(self, payload, already_json_parsed=False):
        if self.schema['measurement'] == 'from_json_keys' or self.schema['fields'] == 'from_json_keys':
            if not already_json_parsed:
                try:
                    payload = json.loads(payload)
                except json.decoder.JSONDecodeError:
                    raise ValueError('could not parse payload as JSON')

            if not isinstance(payload, dict):
                raise ValueError('payload is not a JSON object, could not parse with from_json_keys')
            
        payload = self.apply_type('@root', self.map_value(payload))

        if self.schema['measurement'] == 'from_json_keys':
            for k, v in payload.items():
                self._add_points(k, v)
        else:
            self._add_points(self.schema['measurement'], self.map_value(payload))

    def _add_points(self, measurement, value):
        point = Point(measurement)
        point.time(self.dt)
        for k,v in self.tags.items():
            point.tag(k, v)
        if isinstance(value, dict):
            logging.debug('Flattening dict structure')
            point._fields.update(FlatDict(value, '.'))
        else:
            point.field('value', value)

        self.points.append(point)

    def map_key(self, val):
        try:
            return self.schema['key_map'][val]
        except (KeyError, TypeError):
            return val

    def map_value(self, val):
        try:
            return self.schema['value_map'][val]
        except (KeyError, TypeError):
            return val
        
    def include_filter(self, val):
        return not self.schema['json_keys_include'] or val in self.schema['json_keys_include']
        
    def exclude_filter(self, val):
        return not self.schema['json_keys_exclude'] or val not in self.schema['json_keys_exclude']
    
    def apply_type(self, key, val):
        try:
            try:
                if self.schema['integer_keys'](key):
                    logging.debug(f'{key} will be typecast to int')
                    return int(val)
            except KeyError:
                pass

            try:
                if self.schema['string_keys'](key):
                    logging.debug(f'{key} will be typecast to str')
                    return str(val)
            except KeyError:
                pass
            
            try:
                if self.schema['float_keys'](key):
                    logging.debug(f'{key} will be typecast to float')
                    if val is None:
                        return nan
                    else:
                        return float(val)
            except KeyError:
                pass
            
            try:
                if isinstance(val, bool) or self.schema['boolean_keys'](key):
                    logging.debug(f'{key} will be typecast to bool')
                    return bool(val)
            except KeyError:
                pass

            if isinstance(val, dict):
                self.tags.update({k: v for k, v in val.items() if k in self.schema['tags_from_json']})
                return {self.map_key(k): self.apply_type(k, self.map_value(v)) for k, v in val.items() if self.include_filter(k) and self.exclude_filter(k)}
            
            logging.debug(f'{key} will be typecast to float')
            if val is None:
                return nan
            else:
                return float(val)
        except Exception as e:
            e.message += f' for key {key}'
            raise e