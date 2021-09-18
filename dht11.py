import board
import time
import adafruit_dht
import csv
from datetime import datetime

dht_device = adafruit_dht.DHT11(board.D4)
fieldnames = ['datetime', 'temperature', 'humidity']

with open(f'{datetime.now().date()}.csv', 'w') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    
while True:
    try:
        humidity, temperature = dht_device.temperature, dht_device.humidity
        with open(f'{datetime.now().date()}.csv', 'a') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writerow({
                'datetime': str(datetime.now()),
                'temperature': temperature,
                'humidity': humidity
                })
        print(f"Temp {temperature}; himidity {humidity}")
    except Exception as e:
        print(f'Fatal Error; {str(e)}')
    time.sleep(60)
