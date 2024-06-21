# import paho.mqtt.client as mqtt
# broker = "158.180.44.197"
# port = 1883
# topic = "iot1/teaching_factory_fast/temperature"
# payload = "on"

# # create function for callback
# def on_message(client, userdata, message):
#     print("message received:")
#     print("message: ", message.payload.decode())
#     print("\n")

# # create client object
# mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
# mqttc.username_pw_set("bobm", "letmein")              

# # assign function to callback
# mqttc.on_message = on_message                          

# # establish connection
# mqttc.connect(broker,port)                                 

# # subscribe
# mqttc.subscribe(topic, qos=0)

# # Blocking call that processes network traffic, dispatches callbacks and handles reconnecting.
# #mqttc.loop_forever()

# while True:
#     mqttc.loop(0.5)

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
        

if __name__ == "__main__":
    broker = "158.180.44.197"
    port = 1883
    topic = "iot1/teaching_factory_fast/temperature"
    payload = "on"

    db = Database('teaching_factory_test.db')
    db.create_table('Bottles', {'bottle': 'INTEGER PRIMARY KEY', 'final_weight': 'FLOAT', 'is_cracked': 'BOOLEAN', 'time': 'INTEGER'})
    db.create_table('Dispenser_red', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
    db.create_table('Dispenser_blue', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
    db.create_table('Dispenser_green', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
    db.create_table('Temperature', {'time stamp': 'INTEGER PRIMARY KEY', 'temperature_C1': 'FLOAT', 'temperature_C2': 'FLOAT', 'temperature_C3': 'FLOAT'})
    db.create_table('Vibrations', {'id': 'INTEGER', 'index_value': 'INTEGER', 'vibration': 'FLOAT'})

    while (True):
        continue
