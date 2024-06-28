import paho.mqtt.client as mqtt
import time

# Callback-Funktion für Verbindungsabbruch
def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected disconnection. Reconnecting...")
        reconnect(client)

# Funktion für Wiederverbindung
def reconnect(client):
    while True:
        try:
            client.reconnect()
            print("Reconnected successfully")
            break
        except Exception as e:
            print(f"Reconnection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)