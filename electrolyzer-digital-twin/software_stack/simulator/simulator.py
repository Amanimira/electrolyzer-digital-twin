import os
import time
import json
import random
import numpy as np
from datetime import datetime
import paho.mqtt.client as mqtt
from paho.mqtt import CallbackAPIVersion
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = "electrolyzer/sensors"
MQTT_USER = os.getenv("MQTT_USER", "admin")
MQTT_PASS = os.getenv("MQTT_PASS", "secret")
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "influx-token-2024")
INFLUX_ORG = os.getenv("INFLUX_ORG", "electrolyzer")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "electrolyzer_data")

mqtt_client = None
influx_client = None
write_api = None
mqtt_connected = False
influx_connected = False

# MQTT setup
def on_mqtt_connect(client, userdata, flags, rc, properties=None):
    global mqtt_connected
    if rc == 0:
        logger.info("MQTT متصل بنجاح في Simulator!")
        mqtt_connected = True
    else:
        logger.error(f"فشل اتصال MQTT (code: {rc})")
        mqtt_connected = False

try:
    mqtt_client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="simulator_client")
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=120)
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()
    logger.info("MQTT setup في Simulator.")
except Exception as e:
    logger.error(f"فشل MQTT في Simulator: {e}")
    mqtt_client = None

# InfluxDB setup
try:
    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = influx_client.write_api(write_options=ASYNCHRONOUS)
    influx_connected = True
    logger.info("InfluxDB متصل بنجاح في Simulator!")
except Exception as e:
    logger.error(f"فشل InfluxDB في Simulator: {e}")
    influx_client = None
    write_api = None

def generate_sensor_data():
    # توليد بيانات واقعية لـ Electrolyzer
    base_temp = np.random.normal(70, 5)  # درجة حرارة
    base_pressure = np.random.normal(30, 2)  # ضغط
    base_voltage = np.random.normal(48, 2)  # جهد
    base_current = np.random.normal(100, 10)  # تيار
    base_flow = np.random.normal(5, 0.5)  # تدفق H2
    
    # Anomalies واقعية (15% احتمال)
    if random.random() < 0.15:
        anomaly_type = random.choice(['voltage_drop', 'temp_spike', 'pressure_leak'])
        if anomaly_type == 'voltage_drop':
            base_voltage -= 10  # انخفاض جهد (عطل)
            logger.warning(f"Anomaly: voltage_drop")
        elif anomaly_type == 'temp_spike':
            base_temp += 20  # ارتفاع حرارة
            logger.warning(f"Anomaly: temp_spike")
        elif anomaly_type == 'pressure_leak':
            base_pressure -= 8  # تسرب
            logger.warning(f"Anomaly: pressure_leak")
    
    timestamp = datetime.utcnow().isoformat()
    data = {
        "temperature": round(base_temp, 2),
        "pressure": round(base_pressure, 2),
        "voltage": round(base_voltage, 2),
        "current": round(base_current, 2),
        "flow_rate": round(base_flow, 2),
        "timestamp": timestamp
    }
    
    # إرسال MQTT
    if mqtt_client and mqtt_connected:
        try:
            payload = json.dumps(data)
            result = mqtt_client.publish(MQTT_TOPIC, payload)
            if result.rc == 0:
                logger.info(f"Sent data via MQTT: {data}")
            else:
                logger.error(f"فشل MQTT publish: {result.rc}")
        except Exception as e:
            logger.error(f"فشل إرسال MQTT: {e}")
    else:
        logger.warning(f"MQTT غير متاح – البيانات محليًا: {data}")
    
    # حفظ InfluxDB
    if influx_connected and write_api:
        try:
            point = Point("sensor_data") \
                .tag("source", "simulator") \
                .field("temperature", data["temperature"]) \
                .field("pressure", data["pressure"]) \
                .field("voltage", data["voltage"]) \
                .field("current", data["current"]) \
                .field("flow_rate", data["flow_rate"]) \
                .time(timestamp)
            write_api.write(bucket=INFLUX_BUCKET, record=point)
            logger.info("تم حفظ البيانات في InfluxDB بنجاح.")
        except Exception as e:
            logger.error(f"فشل في حفظ InfluxDB: {e}")
    else:
        logger.warning("InfluxDB غير متاح – البيانات محليًا فقط.")
    
    logger.info("-" * 50)  # فاصل للوضوح

if __name__ == "__main__":
    logger.info("Starting Electrolyzer Simulator...")
    logger.info(f"حالة MQTT: {'متصل' if mqtt_connected else 'غير متصل'}")
    logger.info(f"حالة InfluxDB: {'متصل' if influx_connected else 'غير متصل'}")
    try:
        while True:
            generate_sensor_data()
            time.sleep(5)  # كل 5 ثوانٍ
    except KeyboardInterrupt:
        logger.info("\nSimulator stopped by user.")
    finally:
        if mqtt_client:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        if influx_client:
            if write_api:
                write_api.close()
            influx_client.close()
        logger.info("تم إغلاق الاتصالات.")