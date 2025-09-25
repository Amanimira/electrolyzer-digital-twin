import os
import json
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator
from contextlib import asynccontextmanager
import asyncio
from fastapi import FastAPI
import uvicorn
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS
from influxdb_client.client.query_api import QueryApi
import paho.mqtt.client as mqtt

# محاولة import CallbackAPIVersion (لـ v2.x) مع fallback
try:
    from paho.mqtt import CallbackAPIVersion
    CALLBACK_VERSION = CallbackAPIVersion.VERSION2
    mqtt_version_msg = "Using paho-mqtt v2.x API (CallbackAPIVersion)"
except ImportError:
    CALLBACK_VERSION = None
    mqtt_version_msg = "paho-mqtt v1.x detected. Using legacy API (VERSION1). Upgrade to v2.x for better features."

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(mqtt_version_msg)  # Log للنسخة في البداية

# Global variables
mqtt_client = None
influx_client = None
write_api = None

# InfluxDB setup
async def setup_influxdb():
    global influx_client, write_api
    
    INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "influx-token-2024")
    if not INFLUX_TOKEN or INFLUX_TOKEN == "influx-token-2024":
        logger.error("INFLUX_TOKEN is not set properly in .env! Please update it.")
        return False
    
    INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
    INFLUX_ORG = os.getenv("INFLUX_ORG", "electrolyzer")
    
    for attempt in range(5):
        try:
            influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
            write_api = influx_client.write_api(write_options=ASYNCHRONOUS)
            logger.info("Successfully connected to InfluxDB from Backend!")
            return True
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}: Failed to connect to InfluxDB: {e}")
            if attempt < 4:
                await asyncio.sleep(5)
    logger.error("Final failure in connecting to InfluxDB")
    return False

# MQTT setup
def on_mqtt_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info("Successfully connected to MQTT Broker from Backend!")
        MQTT_TOPIC_SENSORS = os.getenv("MQTT_TOPIC", "electrolyzer/sensors")
        client.subscribe(MQTT_TOPIC_SENSORS)
    else:
        logger.error(f"MQTT connection failed (code: {rc})")

def on_mqtt_message(client, userdata, msg, properties=None):
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)
        logger.info(f"Received sensor data: {data}")
        
        if write_api:
            ts_str = data.get("timestamp")
            if ts_str:
                if 'Z' in ts_str:
                    ts_str = ts_str.replace('Z', '+00:00')
                point_time = datetime.fromisoformat(ts_str)
            else:
                point_time = datetime.now(timezone.utc)
            
            point = Point("electrolyzer_metrics") \
                .tag("host", "electrolyzer_1") \
                .field("temperature", data.get("temperature", 0)) \
                .field("voltage", data.get("voltage", 0)) \
                .field("current", data.get("current", 0)) \
                .field("conductivity", data.get("conductivity", 0)) \
                .time(point_time)
            write_api.write(bucket=INFLUX_BUCKET, record=point)
            logger.info("Sensor data written to InfluxDB.")

            predicted_status = "normal"
            health_score = 100
            if data.get("conductivity", 0) > 0.5 or data.get("voltage", 0) > 2.05:
                predicted_status = "anomaly"
                health_score = 45
            
            ai_point = Point("ai_predictions") \
                .tag("host", "electrolyzer_1") \
                .field("predicted_status", predicted_status) \
                .field("health_score", health_score) \
                .time(datetime.now(timezone.utc))
            write_api.write(bucket=INFLUX_BUCKET, record=ai_point)
            logger.info("AI prediction written to InfluxDB.")

        else:
            logger.warning("InfluxDB API is not available.")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in MQTT payload: {e}")
    except ValueError as e:
        logger.error(f"Invalid timestamp in data: {e}")
    except Exception as e:
        logger.error(f"Failed to process MQTT message: {e}")

async def setup_mqtt():
    global mqtt_client
    MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
    MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
    MQTT_USER = os.getenv("MQTT_USER", "admin")
    MQTT_PASS = os.getenv("MQTT_PASS", "secret")
    
    if CALLBACK_VERSION:
        mqtt_client = mqtt.Client(callback_api_version=CALLBACK_VERSION, client_id="backend_client")
    else:
        mqtt_client = mqtt.Client(client_id="backend_client")  # v1.x
    
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message
    
    for attempt in range(5):
        try:
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
            mqtt_client.loop_start()
            logger.info("MQTT setup completed.")
            return True
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}: MQTT setup failed: {e}")
            if attempt < 4:
                await asyncio.sleep(5)
    logger.error("Final MQTT connection failure.")
    return False

# Lifespan events (بديل لـ on_event)
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    # Startup
    influx_success = await setup_influxdb()
    mqtt_success = await setup_mqtt()
    INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "electrolyzer_data")
    if not influx_success or not mqtt_success:
        logger.error("One or more services failed to start. Check logs and .env file.")
    else:
        logger.info("All services started successfully!")
    yield  # يستمر التطبيق هنا
    # Shutdown
    global mqtt_client, influx_client, write_api
    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        logger.info("MQTT client disconnected.")
    if write_api:
        write_api.close()
        logger.info("InfluxDB write API closed.")
    if influx_client:
        influx_client.close()
        logger.info("InfluxDB client closed.")
    logger.info("Backend connections closed.")

# Config globals (يجب تعريفها هنا للوصول في endpoints)
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "electrolyzer_data")
INFLUX_ORG = os.getenv("INFLUX_ORG", "electrolyzer")

app = FastAPI(title="Electrolyzer Backend API", lifespan=lifespan)  # إضافة lifespan

@app.get("/")
def read_root():
    return {"message": "Electrolyzer Backend API is running!"}

@app.get("/health")
def health_check():
    status = {"status": "ok"}
    
    if influx_client:
        try:
            query_api = influx_client.query_api()
            query_api.query(f'from(bucket: "{INFLUX_BUCKET}") |> limit(n:1)')
            status["influxdb"] = "connected"
        except Exception as e:
            logger.warning(f"InfluxDB health check failed: {e}")
            status["influxdb"] = "not_connected"
    else:
        status["influxdb"] = "not_connected"
    
    if mqtt_client and mqtt_client.is_connected():
        status["mqtt"] = "connected"
    else:
        status["mqtt"] = "not_connected"
    
    if "not_connected" in status.values():
        status["status"] = "degraded"
    
    return status

@app.get("/data")
def get_latest_data():
    if not influx_client:
        return {"error": "InfluxDB not connected"}
    
    try:
        query_api = influx_client.query_api()
        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
        |> range(start: -1h)
        |> filter(fn: (r) => r["_measurement"] == "electrolyzer_metrics")
        |> last()
        |> yield(name: "last")
        '''
        tables = query_api.query(query=query, org=INFLUX_ORG)
        
        if tables:
            result = []
            for table in tables:
                for record in table.records:
                    result.append({
                        "measurement": record.get_measurement(),
                        "time": record.get_time(),
                        "fields": record.values
                    })
            return {"latest_data": result}
        else:
            return {"message": "No data found in the last hour"}
    except Exception as e:
        logger.error(f"Error querying data: {e}")
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
    