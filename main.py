from Database.database import Database
from MQTT_client.mqtt_client import Client
import paho.mqtt.client as mqtt

# MQTT broker settings
broker = "158.180.44.197"
port = 1883
topic = "iot1/teaching_factory_fast/temperature"
payload = "on"

# Create the database and tables
db = Database('./Database/teaching_factory.db')
db.create_table('Bottles', {'bottle': 'INTEGER PRIMARY KEY', 'final_weight': 'FLOAT', 'is_cracked': 'BOOLEAN', 'time': 'INTEGER'})
db.create_table('Dispenser_red', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
db.create_table('Dispenser_blue', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
db.create_table('Dispenser_green', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
db.create_table('Temperature', {'time stamp': 'INTEGER PRIMARY KEY', 'temperature_C1': 'FLOAT', 'temperature_C2': 'FLOAT', 'temperature_C3': 'FLOAT'})
db.create_table('Vibrations', {'id': 'INTEGER', 'index_value': 'INTEGER', 'vibration': 'FLOAT'})

mqtt_client = Client(broker, port, "bobm", "letmein", topic)
#mqtt_client.set_up(topic)
while(True):
    mqtt_client.mqttc.loop(0.5)