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
db.create_table('Bottles', {'id': 'INTEGER PRIMARY KEY', 'final_weight': 'FLOAT', 'is_cracked': 'BOOLEAN'})
db.create_table('Dispenser', {'color': 'TEXT', 'time_stamp': 'INTEGER PRIMARY KEY', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT', 'temp1': 'FLOAT', 'temp2': 'FLOAT', 'temp3': 'FLOAT'})
db.create_table('Vibrations', {'id': 'INTEGER', 'index_value': 'INTEGER', 'vibration': 'FLOAT'})

mqtt_client = Client(broker, port, "bobm", "letmein", topic)
#mqtt_client.set_up(topic)
while(True):
    mqtt_client.mqttc.loop(0.5)