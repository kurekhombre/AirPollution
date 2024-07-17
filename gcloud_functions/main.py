from utils.data_ingestion import fetch_city_coordinates, fetch_air_pollution_data, upload_to_gcs, add_metadata_to_data, generate_filename
import os
import yaml
from google.cloud import storage
import functions_framework


OPENWEATHER_API_KEY=os.environ['OPENWEATHER_API_KEY']


with open('config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

@functions_framework.http
def open_weather_data_extract(request, context=None):

    try:
        response = {}
        id = 1
        for city_name in config['cities']:
            lat, lon = fetch_city_coordinates(city_name['name'], OPENWEATHER_API_KEY)
            pollution_data = fetch_air_pollution_data(lat, lon, OPENWEATHER_API_KEY)

            pollution_data_with_metadata = add_metadata_to_data(pollution_data, city_name) if config['output']['metadata'] else pollution_data

            file_name = generate_filename(city_name)

        #     upload_to_gcs(ACCOUNT_SERVICE_KEY, GCS_BUCKET_NAME, file_name, pollution_data_with_metadata)

            response_data = {
                "city_name": city_name,
                "coordinates": {
                    "latitude": lat,
                    "longitude": lon
                },
                "pollution_data": pollution_data,
                "pollution_data_with_metadata": pollution_data_with_metadata,
                "file_name": file_name
            }
            response[id] = response_data
            id += 1
            
        return str(response)

    except Exception as e:
        return {e}

