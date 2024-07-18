import json
import requests
from google.cloud import storage
from datetime import datetime
import logging


def fetch_city_coordinates(city_name, api_key):
    city_api = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name}&limit=1&appid={api_key}"
    response = requests.get(city_api)
    city_data = response.json()
    return city_data[0]['lat'], city_data[0]['lon']

def fetch_air_pollution_data(lat, lon, api_key):
    pollution_api = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}"
    response = requests.get(pollution_api)
    return response.json()

def upload_to_gcs(path_to_key, bucket_name, file_name, data):
    client = storage.Client.from_service_account_json(path_to_key)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(json.dumps(data))

def add_metadata_to_data(data, city):
    data['city'] = city
    data['timestamp'] = datetime.now().isoformat()
    return data

def generate_filename(city):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"{city}_{timestamp}.json"
    return filename

#Historia z API
#____###

def get_air_pollution_history_data(
            self,
            lat: float,
            lon: float,
            unix_start_date: int,
            unix_end_date: int,
    ) -> dict:
        """
        
        Get historical air pollution data for given coordinates.
        
        """
        url = f"{self.AIR_POLLUTION_HISTORY_URL}lat={lat}&lon={lon}&start={unix_start_date}&end={unix_end_date}&appid={self.openweather_api_key}"
        return self.get_data_from_url(url)


# AIR_POLLUTION_HISTORY_URL = 'http://api.openweathermap.org/data/2.5/air_pollution/history?'
# """