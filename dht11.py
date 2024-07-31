import os
import board
import time
import adafruit_dht
import csv
import json
from datetime import datetime
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')
MQTT_HOST = os.getenv('MQTT_HOST')
MQTT_PUB_TOPIC = os.getenv('MQTT_PUB_TOPIC')
KAFKA_PUB_TOPIC = os.getenv('KAFKA_PUB_TOPIC')
DEVICE_UUID = os.getenv('DEVICE_UUID')

def on_connect(client, data, flags, rc):
    print("CONNACK received with code %d" % (rc))
    client.subscribe("/smarthouse/duke/temphum")

def on_message(client, userdata, msg):
    print("received", msg.topic + " "+str(msg.payload))

def on_publish(client, data, mid):
    print("published", str(mid))

dht_device = adafruit_dht.DHT11(board.D4)
fieldnames = ['datetime', 'timestamp', 'temperature', 'humidity']
client = mqtt.Client("rasp4b", True, None, mqtt.MQTTv31)
client.on_connect = on_connect
client.on_message = on_message
client.tls_set(tls_version=mqtt.ssl.PROTOCOL_TLS)
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.connect(MQTT_HOST, 8883, 60)
client.loop_start()

with open(f'data/{datetime.now().date()}.csv', 'w') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    
while True:
    try:
        temperature, humidity = dht_device.temperature, dht_device.humidity
        with open(f'data/{datetime.now().date()}.csv', 'a') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            data = {
                'datetime': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'timestamp': time.time() * 10000000,
                'temperature': temperature,
                'humidity': humidity,
                'uuid':str(DEVICE_UUID)
                }
            writer.writerow(data)
            client.publish(f'/{MQTT_PUB_TOPIC}', json.dumps(data))
        print(f"Temp {temperature}; himidity {humidity}")
    except Exception as e:
        print(f'Fatal Error; {str(e)}')
    time.sleep(60)
