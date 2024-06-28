import configparser

def read_config(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)

    broker = config.get('MQTT', 'broker')
    port = config.getint('MQTT', 'port')
    topics = config.get('MQTT', 'topics').split(',')

    return broker, port, topics

if __name__ == "__main__":
    config_file = 'config.ini'
    broker, port, topics = read_config(config_file)

    print(f"Broker: {broker}")
    print(f"Port: {port}")
    print(f"Topics: {topics}")
