import board
import time
import adafruit_dht
import csv
import json
from datetime import datetime
import paho.mqtt.client as paho

def on_connect(client, data, flags, rc):
    print("CONNACK received with code %d" % (rc))

def on_publish(client, data, mid):
    print(str(mid))

dht_device = adafruit_dht.DHT11(board.D4)
fieldnames = ['datetime', 'temperature', 'humidity']
# hive_client = Client()
client = paho.Client()
client.on_connect = on_connect
client.username_pw_set("duke__", "dukeHiveMQ8")
client.connect("3221fc9b1a7e4e76ad7cce10b8489e96.s1.eu.hivemq.cloud", 8883)
client.loop_start()

with open(f'{datetime.now().date()}.csv', 'w') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    
while True:
    try:
        humidity, temperature = dht_device.temperature, dht_device.humidity
        with open(f'{datetime.now().date()}.csv', 'a') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            data = {
                'datetime': str(datetime.now()),
                'temperature': temperature,
                'humidity': humidity
                }
            writer.writerow(data)
            client.publish('smarthouse/duke/temphum', json.dumps(data))
        print(f"Temp {temperature}; himidity {humidity}")
    except Exception as e:
        print(f'Fatal Error; {str(e)}')
    time.sleep(60)
