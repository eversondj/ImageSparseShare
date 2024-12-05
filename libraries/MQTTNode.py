import paho.mqtt.client as mqtt
import uuid
import hashlib
from time import sleep

def generate_client_id():
    return str(uuid.uuid4())

class MQTTNode:
    def __init__(self, broker, topic_pub, topic_sub, username=None, password=None, port=1883, keepalive=60):
        self.broker = broker
        self.port = port
        self.topic_pub = topic_pub
        self.topic_sub = topic_sub if isinstance(topic_sub, list) else [topic_sub]
        self.username = username
        self.password = password
        self.keepalive = keepalive
        self.client_id = generate_client_id()
        self.client = mqtt.Client(client_id=self.client_id, clean_session=True, protocol=mqtt.MQTTv311)
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)
        self.client.max_queued_messages_set(0)
        self.client.max_inflight_messages_set(5)
        self.client.reconnect_delay_set(min_delay=1, max_delay=120)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.enable_logger()
        self.client.connect(self.broker, self.port, self.keepalive)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected successfully with client ID {self.client_id}.")
            for topic in self.topic_sub:
                self.client.subscribe(topic)
        else:
            print(f"Connection failed with code {rc}")

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print(f"Unexpected disconnection. rc={rc}")
        else:
            print("Disconnected successfully.")

    def on_message(self, client, userdata, msg):
        try:
            print(f"Message received on topic {msg.topic}: {msg.payload.decode()}")
        except Exception as e:
            print(f"Exception in on_message: {e}")
            traceback.print_exc()

    def publish(self, message, topic=None):
        pub_topic = topic if topic else self.topic_pub
        while True:
            result = self.client.publish(topic=pub_topic, payload=message, qos=1)
            status = result[0]
            if status == mqtt.MQTT_ERR_SUCCESS:
                break
            else:
                print(f"Failed to send message to topic {pub_topic}, return code {status}")
            sleep(10)



    def add_topic(self, topic):
        if topic not in self.topic_sub:
            self.topic_sub.append(topic)
            self.client.subscribe(topic)
            print(f"Subscribed to topic: {topic}")

    def remove_topic(self, topic):
        if topic in self.topic_sub:
            self.client.unsubscribe(topic)
            self.topic_sub.remove(topic)
            print(f"Unsubscribed from topic: {topic}")
    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()
