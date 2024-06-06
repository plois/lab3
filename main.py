import logging
from typing import List
from fastapi import FastAPI
from redis import Redis
import paho.mqtt.client as mqtt
from app.adapters.store_api_adapter import StoreApiAdapter
from app.entities.processed_agent_data import ProcessedAgentData
from config import STORE_API_BASE_URL, REDIS_HOST, REDIS_PORT, BATCH_SIZE, MQTT_TOPIC, MQTT_BROKER_HOST, MQTT_BROKER_PORT

# Configure logging settings
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(module)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app.log"),
    ],
)

# Create an instance of the Redis client using the configuration
redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT)

# Create an instance of the StoreApiAdapter using the configuration
store_adapter = StoreApiAdapter(api_base_url=STORE_API_BASE_URL)

# FastAPI
app = FastAPI()

@app.post("/processed_agent_data/")
async def save_processed_agent_data(processed_agent_data: ProcessedAgentData):
    redis_client.lpush("processed_agent_data", processed_agent_data.json())
    processed_agent_data_batch: List[ProcessedAgentData] = []

    if redis_client.llen("processed_agent_data") >= BATCH_SIZE:
        for _ in range(BATCH_SIZE):
            data = redis_client.lpop("processed_agent_data")
            processed_agent_data_batch.append(ProcessedAgentData.parse_raw(data))

        store_adapter.save_data(processed_agent_data_batch=processed_agent_data_batch)
    return {"status": "ok"}

# MQTT
client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
    else:
        logging.info(f"Failed to connect to MQTT broker with code: {rc}")

def on_message(client, userdata, msg):
    try:
        payload: str = msg.payload.decode("utf-8")
        processed_agent_data = ProcessedAgentData.parse_raw(payload)
        redis_client.lpush("processed_agent_data", processed_agent_data.json())
        processed_agent_data_batch: List[ProcessedAgentData] = []

        if redis_client.llen("processed_agent_data") >= BATCH_SIZE:
            for _ in range(BATCH_SIZE):
                data = redis_client.lpop("processed_agent_data")
                processed_agent_data_batch.append(ProcessedAgentData.parse_raw(data))

            store_adapter.save_data(processed_agent_data_batch=processed_agent_data_batch)
    except Exception as e:
        logging.error(f"Error processing MQTT message: {e}")

# Connect
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT)

# Start the MQTT client loop in a background thread
client.loop_start()

# Ensure that the main application runs after the MQTT client starts
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
