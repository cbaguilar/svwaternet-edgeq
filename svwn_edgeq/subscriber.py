import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, reason_code, properties):
    print("Connected with result code:", reason_code)
    client.subscribe("test_topic", 1)

def on_subscribe(client, userdata, mid, granted_qos, properties):
    print("Subscribed to topic, mid:", mid, "granted QoS:", granted_qos)

def on_message(client, userdata, msg):
    print(f"Message received on topic {msg.topic}: {msg.payload.decode()}")

# Create an MQTT client with MQTT v5
client = mqtt.Client(client_id="recv", protocol=mqtt.MQTTv5)

# Assign the callbacks
client.on_connect = on_connect
client.on_subscribe = on_subscribe
client.on_message = on_message

client.connect("localhost", 1883)
client.loop_forever()
