import serial
import time
import paho.mqtt.client as mqtt
import os

# Configuration (Use environment variables for production)
COM_PORT = os.getenv('INSTRUMENT_PORT', 'COM4')
BAUD_RATE = int(os.getenv('BAUD_RATE', 9600))
MQTT_BROKER = os.getenv('MQTT_BROKER', 'localhost')
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))
MQTT_TOPIC = 'lab/prt/temperatura'

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Successfully connected to MQTT Broker")
    else:
        print(f"Connection failed with code: {rc}")

client = mqtt.Client(client_id="fluke_1524_producer")
client.on_connect = on_connect

while True:
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()

        with serial.Serial(COM_PORT, BAUD_RATE, timeout=2) as ser:
            print(f"Serial connection established on {COM_PORT}")
            
            while True:
                # Query instrument
                ser.write("MEAS?\r\n".encode('ascii'))
                line = ser.readline().decode('ascii', errors='ignore').strip()
                
                if line:
                    try:
                        value = float(line)
                        if value != 0.0:
                            client.publish(MQTT_TOPIC, str(value), qos=1)
                    except ValueError:
                        continue
                
                time.sleep(0.1)
                        
    except serial.SerialException as e:
        print(f"Serial error: {e}")
        client.loop_stop()
        time.sleep(5) 
        
    except Exception as e:
        print(f"Unexpected error: {e}")
        client.loop_stop()
        time.sleep(5)