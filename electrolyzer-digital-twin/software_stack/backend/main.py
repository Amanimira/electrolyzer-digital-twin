import os
import json
import logging
from datetime import datetime
from typing import Dict, Any
import asyncio
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS  # محسن للأداء
import paho.mqtt.client as mqtt
from paho.mqtt import CallbackAPIVersion
from sklearn.ensemble import IsolationForest  # للـ AI
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from jose import JWTError, jwt  # لـ auth بسيط

# تحميل env vars
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Electrolyzer Backend API")
security = HTTPBearer()

# Config من env (أمان)
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = "electrolyzer/sensors"
MQTT_USER = os.getenv("MQTT_USER", "admin")
MQTT_PASS = os.getenv("MQTT_PASS", "secret")
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "influx-token-2024")
INFLUX_ORG = os.getenv("INFLUX_ORG", "electrolyzer")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "electrolyzer_data")
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key")  # لـ JWT

# Global vars
mqtt_client = None
influx_client = None
write_api = None
latest_data: Dict[str, Any] = {}
model = None  # AI model

# InfluxDB setup مع retry
async def setup_influxdb():
    global influx_client, write_api
    for attempt in range(3):  # retry 3 مرات
        try:
            influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
            write_api = influx_client.write_api(write_options=ASYNCHRONOUS)
            logger.info("InfluxDB متصل بنجاح في Backend!")
            return True
        except Exception as e:
            logger.warning(f"محاولة {attempt+1}: فشل الاتصال بـ InfluxDB: {e}")
            await asyncio.sleep(5)
    logger.error("فشل نهائي في InfluxDB")
    return False

# MQTT setup مع auth و retry
def on_mqtt_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info("MQTT متصل بنجاح في Backend!")
        client.subscribe(MQTT_TOPIC)
    else:
        logger.error(f"فشل اتصال MQTT (code: {rc})")

def on_mqtt_message(client, userdata, msg, properties=None):
    global latest_data
    try:
        data = json.loads(msg.payload.decode())
        logger.info(f"Received data via MQTT: {data}")
        latest_data = data  # تحديث آخر بيانات (استخدم lock في production)

        # حفظ في InfluxDB
        if write_api:
            timestamp = data.get("timestamp", datetime.utcnow().isoformat())
            point = Point("sensor_data") \
                .tag("source", "backend") \
                .field("temperature", data.get("temperature", 0)) \
                .field("pressure", data.get("pressure", 0)) \
                .field("voltage", data.get("voltage", 0)) \
                .field("current", data.get("current", 0)) \
                .field("flow_rate", data.get("flow_rate", 0)) \
                .time(timestamp)
            write_api.write(bucket=INFLUX_BUCKET, record=point)
            logger.info("تم حفظ البيانات في InfluxDB من Backend.")
        else:
            logger.warning("InfluxDB غير متاح في Backend.")
    except Exception as e:
        logger.error(f"فشل في معالجة MQTT message: {e}")

async def setup_mqtt():
    global mqtt_client
    for attempt in range(3):
        try:
            mqtt_client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="backend_client")
            mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)  # auth
            mqtt_client.on_connect = on_mqtt_connect
            mqtt_client.on_message = on_mqtt_message
            mqtt_client.reconnect_delay_set(min_delay=1, max_delay=120)  # retry auto
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
            mqtt_client.loop_start()
            logger.info("MQTT setup مكتمل في Backend.")
            return True
        except Exception as e:
            logger.warning(f"محاولة {attempt+1}: فشل setup MQTT: {e}")
            await asyncio.sleep(5)
    logger.error("فشل نهائي في MQTT")
    return False

# تحميل AI Model (Isolation Forest لكشف anomalies)
def load_ai_model():
    global model
    try:
        # افتراض: model محفوظ من clean_data.py
        model = IsolationForest(contamination=0.1, random_state=42)
        # إذا لم يكن موجود، درب على بيانات افتراضية (أو load من file)
        sample_data = np.array([[70, 30, 48, 100, 5], [80, 35, 50, 110, 6]])  # temp, pressure, etc.
        model.fit(sample_data)
        logger.info("AI Model محمل بنجاح.")
    except Exception as e:
        logger.error(f"فشل تحميل AI Model: {e}")
        model = None

# FastAPI startup
@app.on_event("startup")
async def startup_event():
    await setup_influxdb()
    await setup_mqtt()
    load_ai_model()

# Auth dependency (بسيط: JWT token)
def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=["HS256"])
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Endpoints
@app.get("/")
def read_root():
    return {"message": "Electrolyzer Backend API جاهز! استخدم /sensors/latest أو /predict."}

@app.get("/sensors/latest")
def get_latest_sensors(current_user: dict = Depends(verify_token)):
    if latest_data:
        return latest_data
    raise HTTPException(status_code=404, detail="لا بيانات بعد. شغل Simulator.")

@app.get("/predict")
def predict_anomaly(current_user: dict = Depends(verify_token)):
    if not latest_data or not model:
        raise HTTPException(status_code=400, detail="لا بيانات أو model غير محمل.")
    
    # استخراج features
    features = np.array([[latest_data.get("temperature", 0),
                          latest_data.get("pressure", 0),
                          latest_data.get("voltage", 0),
                          latest_data.get("current", 0),
                          latest_data.get("flow_rate", 0)]])
    
    prediction = model.predict(features)[0]
    prob = model.decision_function(features)[0]  # أقرب للـ anomaly أقل
    health_score = max(0, min(100, (1 - abs(prob)) * 100))  # تحويل إلى %
    status = "anomaly" if prediction == -1 else "normal"
    
    # حفظ في InfluxDB
    if write_api:
        point = Point("ai_predictions") \
            .field("health_score", health_score) \
            .field("predicted_status", status) \
            .time(datetime.utcnow().isoformat())
        write_api.write(bucket=INFLUX_BUCKET, record=point)
    
    return {"status": status, "health_score": health_score, "probability": abs(prob)}

# Shutdown
@app.on_event("shutdown")
async def shutdown_event():
    global mqtt_client, influx_client, write_api
    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    if write_api:
        write_api.close()
    if influx_client:
        influx_client.close()
    logger.info("تم إغلاق الاتصالات في Backend.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    