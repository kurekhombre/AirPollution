import logging
from src.data_ingestion import fetch_city_coordinates, fetch_air_pollution_data, upload_to_gcs, add_metadata_to_data, generate_filename
import os
from dotenv import load_dotenv
import yaml
from flask import jsonify, request
from google.cloud import storage
import functions_framework


# Remove the file-based logging configuration
# logging.basicConfig(filename='logs/app.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# ACCOUNT_SERVICE_KEY=os.environ['ACCOUNT_SERVICE_KEY']
# GCS_BUCKET_NAME=os.environ['GCS_BUCKET_NAME']

# def process_data_extraction(city_name, config):
    # OPENWEATHER_API_KEY = config['openweather']['api_key']
    # GCS_BUCKET_NAME = config['gcs']['bucket_name']
    # ACCOUNT_SERVICE_KEY = config['gcs']['account_service_key']
    
    # lat, lon = fetch_city_coordinates(city_name, OPENWEATHER_API_KEY)
    # pollution_data = fetch_air_pollution_data(lat, lon, OPENWEATHER_API_KEY)

    # pollution_data_with_metadata = add_metadata_to_data(pollution_data, city_name) if config['output']['metadata'] else pollution_data

    # file_name = generate_filename(city_name)

    # upload_to_gcs(ACCOUNT_SERVICE_KEY, GCS_BUCKET_NAME, file_name, pollution_data_with_metadata)
    
    # # logging.info("Data ingestion and upload process completed successfully.")
    
    # response_data = {
    #     "city_name": city_name,
    #     "coordinates": {
    #         "latitude": lat,
    #         "longitude": lon
    #     },
    #     "pollution_data": pollution_data,
    #     "pollution_data_with_metadata": pollution_data_with_metadata,
    #     "file_name": file_name
    # }

    # return response_data

OPENWEATHER_API_KEY="90da9cd29152fb94e06a1ec2f90d7557"


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
            
            # logging.info("Data ingestion and upload process completed successfully.")
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
        # logging.error(f"An error occurred: {e}")
        return {e}

