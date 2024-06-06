import json
import logging
from typing import List
import requests
from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_gateway import StoreGateway

class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]):
        try:
            response = requests.post(
                f"{self.api_base_url}/processed_agent_data",
                json=[data.dict() for data in processed_agent_data_batch]
            )
            response.raise_for_status()
            return True
        except requests.RequestException as e:
            logging.error(f"Failed to save data: {e}")
            return False
