import paho.mqtt.client as mqtt
from Database.database import Database



# MQTT Broker Informationen
broker = "158.180.44.197"
port = 1883
topics = ["iot1/teaching_factory_fast/ground_truth", "iot1/teaching_factory_fast/dispenser_red", "iot1/teaching_factory_fast/dispenser_blue", "iot1/teaching_factory_fast/dispenser_green", "iot1/teaching_factory_fast/temperature", "iot1/teaching_factory_fast/scale/final_weight", "iot1/teaching_factory_fast/drop_vibration"]

# Create the database and tables
db = Database('./Database/teaching_factory.db')
db.create_table('Bottles', {'bottle': 'INTEGER PRIMARY KEY', 'final_weight': 'FLOAT', 'is_cracked': 'BOOLEAN', 'time': 'INTEGER'})
db.create_table('Dispenser_red', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
db.create_table('Dispenser_blue', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
db.create_table('Dispenser_green', {'bottle': 'INTEGER PRIMARY KEY', 'time': 'INTEGER', 'vibration_avg': 'FLOAT', 'fill_level_gram': 'FLOAT'})
db.create_table('Temperature', {'time stamp': 'INTEGER PRIMARY KEY', 'temperature_C1': 'FLOAT', 'temperature_C2': 'FLOAT', 'temperature_C3': 'FLOAT'})
db.create_table('Vibrations', {'id': 'INTEGER', 'index_value': 'INTEGER', 'vibration': 'FLOAT'})

# Callback-Funktion für empfangene Nachrichten
def on_message(client, userdata, message):
    try:
        topic = message.topic
        
        
        if topic == "iot1/teaching_factory_fast/temperature":
            db.insert_record("INSERT INTO Temperature (time, temperature_C1, temperature_C2, temperature_C3) VALUES (?, ?, ?, ?)", message.payload.decode())

        elif topic == "iot1/teaching_factory_fast/scale/final_weight":
            db.insert_record("INSERT INTO Bottles (final_weight, is_cracked, time) VALUES (?, ?, ?)", message.payload.decode())

        elif topic == "iot1/teaching_factory_fast/ground_truth":
            db.insert_record("INSERT INTO Bottles (final_weight, is_cracked, time) VALUES (?, ?, ?)", message.payload.decode())

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
