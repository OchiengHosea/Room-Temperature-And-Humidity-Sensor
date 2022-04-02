import json
import paho.mqtt.client as paho
from email import message

# public static final String hiveMQURL = "3221fc9b1a7e4e76ad7cce10b8489e96.s1.eu.hivemq.cloud";
    # public static final int hiveMQPort = 8883;
    # public static final String hiveCloudUsername = "duke__";
    # public static final String hiveCloudPass = "dukeHiveMQ8";

class Client():
    def __init__(self) -> None:
        self.client = paho.Client()
        self.client.on_connect = self.on_connect
        self.client.username_pw_set("duke__", "dukeHiveMQ8")
        self.client.connect("3221fc9b1a7e4e76ad7cce10b8489e96.s1.eu.hivemq.cloud", 8883)
        self.client.loop_start()


    def on_connect(self, client, data, flags, rc):
        print("CONNACK received with code %d" % (rc))

    def publish_message(self, topic, data):
        if type(data) == 'str':
            message = data
        else:
            message = json.dumps(data)
        self.client.publish(topic, message, qos=1)
