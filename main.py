import paho.mqtt.client as mqtt
from Database.database import Database
from Config.config import read_config
import json


# MQTT Broker Informationen aus der Konfigurationsdatei lesen
#topics = ["iot1/teaching_factory_fast/ground_truth", "iot1/teaching_factory_fast/dispenser_red", "iot1/teaching_factory_fast/dispenser_blue", "iot1/teaching_factory_fast/dispenser_green", "iot1/teaching_factory_fast/temperature", "iot1/teaching_factory_fast/scale/final_weight", "iot1/teaching_factory_fast/drop_vibration"]

broker, port, topics = read_config('config.ini')

# Create the database and tables
db = Database('./Database/teaching_factory.db')
#db.create_table('Bottles', {'bottle': 'INTEGER PRIMARY KEY', 'final_weight': 'FLOAT', 'is_cracked': 'BOOLEAN', 'time': 'INTEGER'})
#db.create_table('Dispenser_red', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
#db.create_table('Dispenser_blue', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
#db.create_table('Dispenser_green', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
#db.create_table('Temperature', {'time stamp': 'INTEGER PRIMARY KEY', 'temperature_C1': 'FLOAT', 'temperature_C2': 'FLOAT', 'temperature_C3': 'FLOAT'})
#db.create_table('Vibrations', {'id': 'INTEGER', 'index_value': 'INTEGER', 'vibration': 'FLOAT'})
db.create_table('Drop_Vibration', {'bottle': 'INTEGER', 'index_value': 'FLOAT'})
db.create_table('Cracked', {'bottle': 'INTEGER', 'is_cracked': 'BOOLEAN'})

# Callback-Funktion für empfangene Nachrichten
def on_message(client, userdata, message):
    try:
        topic = message.topic
        
        if topic == "iot1/teaching_factory_fast/drop_vibration":
            msg = message.payload.decode()
            msg_dict = json.loads(msg)
            print(msg_dict["bottle"])
            for element in (msg_dict["drop_vibration"]):
                db.insert_record("INSERT INTO Drop_Vibration (bottle, index_value) VALUES (?, ?)", (msg_dict["bottle"], element))
        elif topic == "iot1/teaching_factory_fast/ground_truth":
            msg = message.payload.decode()
            msg_dict = json.loads(msg)
            db.insert_record("INSERT INTO Cracked (bottle, is_cracked) VALUES (?, ?)", (msg_dict["bottle"], msg_dict["is_cracked"]))
        #elif topic == "iot1/teaching_factory_fast/scale/final_weight":
        #    db.insert_record("INSERT INTO Bottles (final_weight, is_cracked, time) VALUES (?, ?, ?)", message.payload.decode())

        #elif topic == "iot1/teaching_factory_fast/ground_truth":
        #    db.insert_record("INSERT INTO Bottles (final_weight, is_cracked, time) VALUES (?, ?, ?)", message.payload.decode())
        #
        #elif topic == "iot1/teaching_factory_fast/dispenser_red":
        #    db.insert_record("INSERT INTO Dispenser_red (time, vibration_avg, fill_level_gram) VALUES (?, ?, ?)", message.payload.decode())
        #
        #elif topic == "iot1/teaching_factory_fast/dispenser_blue":
        #    db.insert_record("INSERT INTO Dispenser_blue (time, vibration_avg, fill_level_gram) VALUES (?, ?, ?)", message.payload.decode())
        #
        #elif topic == "iot1/teaching_factory_fast/dispenser_green":
        #    db.insert_record("INSERT INTO Dispenser_green (time, vibration_avg, fill_level_gram) VALUES (?, ?, ?)", message.payload.decode())
        #
        #elif topic == "iot1/teaching_factory_fast/drop_vibration":
        #    db.insert_record("INSERT INTO Vibrations (index_value, vibration) VALUES (?, ?)", message.payload.decode())

    except Exception as e:
        print(f"Error processing message: {e}")

# Client-Objekt erstellen
mqttc = mqtt.Client()
mqttc.username_pw_set("bobm", "letmein")

# Callback-Funktion zuweisen
mqttc.on_message = on_message

# Verbindung herstellen
mqttc.connect(broker, port, 60)

# Zu den Themen abonnieren
for topic in topics:
    mqttc.subscribe(topic, qos=0)

# Netzwerkverkehr verarbeiten
mqttc.loop_forever()
