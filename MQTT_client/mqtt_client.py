from Database.database import Database
import paho.mqtt.client as mqtt

# Blocking call that processes network traffic, dispatches callbacks and handles reconnecting.
# mqttc.loop_forever()

class Client:
    def __init__(self, broker, port, username, password, topic):
        self.broker = broker
        self.port = port
        self.topic = topic
        self.mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.mqttc.username_pw_set(username, password)
        self.mqttc.connect(self.broker, self.port)
        self.mqttc.subscribe(self.topic, qos=0)
        self.mqttc.on_message = self.on_message
    
    def on_message(client, userdata, message):
        print("message received:")
        print("message: ", message.payload.decode())
        print("\n")

    
    def set_up(self, topic):
        self.mqttc.connect(self.broker, self.port)
        self.mqttc.subscribe(topic, qos=0)
        self.mqttc.on_message = self.on_message
        

#while True:
#    mqttc.loop(0.5)