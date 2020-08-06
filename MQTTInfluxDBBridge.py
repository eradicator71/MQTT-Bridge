import re
from typing import NamedTuple

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

INFLUXDB_ADDRESS = ''
INFLUXDB_USER = ''
INFLUXDB_PASSWORD = ''
INFLUXDB_DATABASE = ''

MQTT_ADDRESS = ''
MQTT_USER = ''
MQTT_PASSWORD = ''
MQTT_TOPIC = 'home/800L/gpscoords'
MQTT_REGEX = '(^.*),(.*$)'
MQTT_CLIENT_ID = 'MQTTInfluxDBBridge'

influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)

def on_connect(client, userdata, flags, rc):
    print('on_connect')
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    print('in on_message')
    """The callback for when a PUBLISH message is received from the server."""
    print(msg.topic + ':: ::' + str(msg.payload))
    latitude, longitude = _parse_mqtt_message(msg.topic, msg.payload.decode('utf-8'))
    print('back to on_message')
    print ('was lat returned? latitude: ' + latitude)
    print ('was lon returned? longitude: ' + longitude)
    if latitude is not None:
        print('sending data to _send_to_influx')
        _send_sensor_data_to_influxdb(latitude, longitude)


def _parse_mqtt_message(topic, payload):
    print('in mqtt parse')
    print ('topic: ' + topic)
    print ('payload: ' + payload)
    match = re.match(MQTT_REGEX, payload)
    if match:
        print('match is true')
        print('matchgroup 1: ' + match.group(1))
        print('matchgroup 2: ' + match.group(2))
        latitude = match.group(1)
        longitude = match.group(2)
        print ('latitude: ' + latitude)
        print ('longitude: ' + longitude)
        if match.group(0) == 'status':
            return None
        return latitude, longitude
    else:
        print('match is false')
        return None


def _send_sensor_data_to_influxdb(latitude, longitude):
    print('in send data to influx '+latitude+longitude)
    measurement = 'location1'
    json_body = [
        {
            'measurement': measurement,
            'fields': {
                'latitude': latitude,
                'longitude': longitude
            }
        }
    ]
    print(json_body)
    influxdb_client.write_points(json_body)


def _init_influxdb_database():
    databases = influxdb_client.get_list_database()
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        influxdb_client.create_database(INFLUXDB_DATABASE)
    influxdb_client.switch_database(INFLUXDB_DATABASE)


def main():
    _init_influxdb_database()

    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_forever()


if __name__ == '__main__':
    print('MQTT to InfluxDB bridge')
    main()

