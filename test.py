import paho.mqtt.client as paho
import time

client = paho.Client("local_test")
topic = "TEMPERATURE"

def on_log(client, userdata, level, buff):  # mqtt logs function
    print(buff)

def on_connect(client, userdata, flags, rc):  # connect to mqtt broker function
    if rc == 0:
        client.connected_flag = True  # set flags
        print("Connected Info")
    else:
        print("Bad connection returned code = " + str(rc))
        client.loop_stop()

def on_disconnect(client, userdata, rc):  # disconnect to mqtt broker function
    print("Client disconnected OK")

def on_publish(client, userdata, mid):  # publish to mqtt broker
    print("In on_pub callback mid=" + str(mid))

def on_subscribe(client, userdata, mid, granted_qos):  # subscribe to mqtt broker
    print("Subscribed", userdata)

def on_message(client, userdata, message):  # get message from mqtt broker 
    print("New message received: ", str(message.payload.decode("utf-8")), "Topic : %s ", message.topic, "Retained : %s", message.retain)

def connectToMqtt():  # connect to MQTT broker main function
    print("Connecting to MQTT broker")
    client.username_pw_set(username="duke__", password="dukeHiveMQ8")
    client.on_log = on_log
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.connect("3221fc9b1a7e4e76ad7cce10b8489e96.s1.eu.hivemq.cloud", 8883, keepalive=600)
    ret = client.subscribe(topic, qos=0)
    print("Subscribed return = " + str(ret))
    

connectToMqtt()  # connect to mqtt broker
client.loop_forever()
client.subscribe(topic, qos=0)