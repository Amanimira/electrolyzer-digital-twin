import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json
from threading import Thread
import time

# InfluxDB settings
INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = "my-super-secret-token"
INFLUX_ORG = "my_org"
INFLUX_BUCKET = "electrolyzer_data"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# MQTT settings
MQTT_BROKER = "mosquitto"
MQTT_PORT = 1883
MQTT_TOPIC = "electrolyzer/data"

# Anomaly detection thresholds from the physics team (from the presentation)
THRESHOLD_VOLTAGE_HIGH = 2.2 # V
THRESHOLD_CONDUCTIVITY_HIGH = 0.5 # mS/cm
THRESHOLD_TEMP_HIGH = 90 # C

def on_connect(client, userdata, flags, rc):
    print("Backend connected to MQTT Broker!")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode('utf-8'))
    print(f"Received: {payload}")
    
    # 1. Simple Anomaly Detection based on thresholds
    status = "normal"
    if payload["conductivity"] > THRESHOLD_CONDUCTIVITY_HIGH:
        status = "water_contamination_alert"
        print("ALERT: High conductivity detected. Possible contamination!")
    elif payload["voltage"] > THRESHOLD_VOLTAGE_HIGH:
        status = "voltage_anomaly_alert"
        print("ALERT: High voltage detected. Possible degradation!")
    elif payload["temperature"] > THRESHOLD_TEMP_HIGH:
        status = "overheating_alert"
        print("ALERT: High temperature detected. Cooling issue!")
    
    # 2. Integrate AI Model Prediction (Placeholder)
    # This is where the AI team's code will go
    # For now, we use a placeholder function
    ai_prediction_status = "Normal"
    health_score = 99
    if status != "normal":
        ai_prediction_status = "Anomaly Detected"
        health_score = 45 # Lower the score for demo

    # Write sensor data to InfluxDB
    sensor_point = Point("electrolyzer_metrics") \
        .tag("host", "electrolyzer_1") \
        .field("temperature", payload["temperature"]) \
        .field("voltage", payload["voltage"]) \
        .field("current", payload["current"]) \
        .field("conductivity", payload["conductivity"]) \
        .time(payload["timestamp"], write_precision="s")
    write_api.write(bucket=INFLUX_BUCKET, record=sensor_point)

    # Write AI prediction to a separate measurement
    ai_point = Point("ai_predictions") \
        .tag("host", "electrolyzer_1") \
        .field("predicted_status", ai_prediction_status) \
        .field("health_score", health_score) \
        .time(time.time(), write_precision="s")
    write_api.write(bucket=INFLUX_BUCKET, record=ai_point)
    
    print("Data written to InfluxDB")

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

def start_mqtt_listener():
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
    mqtt_client.loop_forever()

if __name__ == "__main__":
    mqtt_thread = Thread(target=start_mqtt_listener)
    mqtt_thread.start()
    print("Backend is running...")