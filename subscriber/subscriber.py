import paho.mqtt.client as mqtt
import psycopg
import os

# Database Configuration
DB_HOST = os.getenv('DB_HOST', 'postgres_dw')
DB_NAME = os.getenv('DB_NAME', 'laboratorio')
DB_USER = os.getenv('DB_USER', 'ae_user')
DB_PASS = os.getenv('DB_PASS', 'ae_password')
MQTT_BROKER = os.getenv('MQTT_BROKER', 'broker')
MQTT_TOPIC = 'lab/prt/temperatura'

# Database initialization
conn = psycopg.connect(
    f"host={DB_HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASS}"
)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_temperatura (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        valor_crudo NUMERIC,
        topico VARCHAR(50)
    );
""")

def on_connect(client, userdata, flags, rc):
    print(f"Connected to Broker. Subscribing to: {MQTT_TOPIC}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    topic = msg.topic
    
    try:
        cursor.execute(
            "INSERT INTO raw_temperatura (valor_crudo, topico) VALUES (%s, %s)",
            (payload, topic)
        )
    except Exception as e:
        print(f"Database insertion error: {e}")

client = mqtt.Client(client_id="postgres_loader")
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, 1883, 60)
client.loop_forever()