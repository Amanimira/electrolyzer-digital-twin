import paho.mqtt.client as mqtt
import time
import json
import random

# MQTT Broker settings
BROKER = "localhost"
PORT = 1883
TOPIC = "electrolyzer/data"

client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print("Failed to connect, return code %d\n", rc)

client.on_connect = on_connect
client.connect(BROKER, PORT)

# Global variables to simulate state and failure scenarios
failure_scenario = "normal"
time_to_fail = time.time() + 60 * 5 # Introduce a failure after 5 minutes

# Function to simulate sensor data based on a scenario
def generate_data(scenario):
    base_temp = 75
    base_voltage = 1.9
    base_current = 550
    base_conductivity = 0.3
    
    # Simulate normal operation with slight variations
    if scenario == "normal":
        temp = base_temp + random.uniform(-1, 1)
        voltage = base_voltage + random.uniform(-0.02, 0.02)
        current = base_current + random.uniform(-5, 5)
        conductivity = base_conductivity + random.uniform(-0.01, 0.01)

    # 1. Chemical Pollution (تلوث كيميائي)
    elif scenario == "chemical_pollution":
        temp = base_temp + random.uniform(-1, 1)
        voltage = base_voltage + random.uniform(0.1, 0.2) 
        current = base_current + random.uniform(-5, 5)
        conductivity = base_conductivity + 0.05 * (time.time() - time_to_fail) / 60
        conductivity = max(conductivity, 0.5) # Reaching threshold
        
    # 2. Electrode Corrosion (تآكل الأقطاب)
    elif scenario == "electrode_corrosion":
        temp = base_temp + random.uniform(-1, 1)
        voltage = base_voltage + 0.05 * (time.time() - time_to_fail) / 60
        voltage = max(voltage, 2.0) # Voltage increases over time
        current = base_current + random.uniform(-5, 5)
        conductivity = base_conductivity + random.uniform(-0.01, 0.01)

    # 3. Cooling Failure (ضعف التبريد)
    elif scenario == "cooling_failure":
        temp = base_temp + 0.5 * (time.time() - time_to_fail)
        voltage = base_voltage + (temp - base_temp) * 0.01 # Voltage increases with temperature
        current = base_current + random.uniform(-5, 5)
        conductivity = base_conductivity + random.uniform(-0.01, 0.01)

    return {
        "temperature": round(temp, 2),
        "voltage": round(voltage, 2),
        "current": round(current, 2),
        "conductivity": round(conductivity, 2),
        "timestamp": time.time(),
        "scenario": scenario
    }

# Main loop
client.loop_start()
while True:
    if time.time() > time_to_fail and failure_scenario == "normal":
        failure_scenario = random.choice(["chemical_pollution", "electrode_corrosion", "cooling_failure"])
        print(f"--- FAILED SCENARIO ACTIVATED: {failure_scenario} ---")

    data = generate_data(failure_scenario)
    client.publish(TOPIC, json.dumps(data))
    print(f"Published: {data}")
    time.sleep(5)
    