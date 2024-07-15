import logging
from src.data_ingestion import fetch_city_coordinates, fetch_air_pollution_data, upload_to_gcs, add_metadata_to_data, generate_filename
import os
from dotenv import load_dotenv
import yaml
from flask import jsonify, request
from google.cloud import storage
import functions_framework


logging.basicConfig(filename='logs/app.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

load_dotenv('secrets/.env')
ACCOUNT_SERVICE_KEY = os.environ['ACCOUNT_SERVICE_KEY']
GCS_BUCKET_NAME = os.environ['GCS_BUCKET_NAME']
OPENWEATHER_API_KEY = os.environ['OPENWEATHER_API_KEY']

def process_data_extraction(city_name, config):
    # OPENWEATHER_API_KEY = config['openweather']['api_key']
    # GCS_BUCKET_NAME = config['gcs']['bucket_name']
    # ACCOUNT_SERVICE_KEY = config['gcs']['account_service_key']
    
    lat, lon = fetch_city_coordinates(city_name, OPENWEATHER_API_KEY)
    pollution_data = fetch_air_pollution_data(lat, lon, OPENWEATHER_API_KEY)

    pollution_data_with_metadata = add_metadata_to_data(pollution_data, city_name) if config['output']['metadata'] else pollution_data

    file_name = generate_filename(city_name)

    upload_to_gcs(ACCOUNT_SERVICE_KEY, GCS_BUCKET_NAME, file_name, pollution_data_with_metadata)
    
    logging.info("Data ingestion and upload process completed successfully.")
    
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

    return response_data

# with open('config.yaml', 'r') as config_file:
#     config = yaml.safe_load(config_file)

@functions_framework.http
def open_weather_data_extract(request, context=None):
    return "Hello Reciutki"
    # try:
    #     request_json = request.get_json(silent=True)
    #     if request_json and 'city_name' in request_json:
    #         city_name = request_json['city_name']
    #     else:
    #         return jsonify({"error": "City name not provided"}), 400

    #     lat, lon = fetch_city_coordinates(city_name, OPENWEATHER_API_KEY)
    #     pollution_data = fetch_air_pollution_data(lat, lon, OPENWEATHER_API_KEY)

    #     pollution_data_with_metadata = add_metadata_to_data(pollution_data, city_name) if config['output']['metadata'] else pollution_data

    #     file_name = generate_filename(city_name)

    #     upload_to_gcs(ACCOUNT_SERVICE_KEY, GCS_BUCKET_NAME, file_name, pollution_data_with_metadata)
        
    #     logging.info("Data ingestion and upload process completed successfully.")
        
    #     response_data = {
    #         "city_name": city_name,
    #         "coordinates": {
    #             "latitude": lat,
    #             "longitude": lon
    #         },
    #         "pollution_data": pollution_data,
    #         "pollution_data_with_metadata": pollution_data_with_metadata,
    #         "file_name": file_name
    #     }

    #     return jsonify(response_data), 200

    # except Exception as e:
    #     logging.error(f"An error occurred: {e}")
    #     return jsonify({"error": f"An error occurred: {e}"}), 500

# # import logging
# # from src.data_ingestion import fetch_city_coordinates, fetch_air_pollution_data, upload_to_gcs, add_metadata_to_data, generate_filename
# # import os
# # from dotenv import load_dotenv

# # logging.basicConfig(filename='logs/app.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# # load_dotenv('secrets/.env')
# # ACCOUNT_SERVICE_KEY = os.environ['ACCOUNT_SERVICE_KEY']

# # def main():
# #     try:
# #         city_name = input("Input city name: ")
# #         openweather_key_api = os.environ['OPENWEATHER_API_KEY']

# #         lat, lon = fetch_city_coordinates(city_name, openweather_key_api)
# #         pollution_data = fetch_air_pollution_data(lat, lon, openweather_key_api)

# #         pollution_data_with_metadata = add_metadata_to_data(pollution_data, city_name)

# #         file_name = generate_filename(city_name)

# #         upload_to_gcs(ACCOUNT_SERVICE_KEY, 'airpollution_bucket', file_name, pollution_data_with_metadata)
        
# #         logging.info("Data ingestion and upload process completed successfully.")
# #     except Exception as e:
# #         logging.error(f"An error occurred: {e}")

# # if __name__ == "__main__":
# #     main()

# import functions_framework

# #główna baza


# import logging
# from src.data_ingestion import fetch_city_coordinates, fetch_air_pollution_data, upload_to_gcs, add_metadata_to_data, generate_filename
# import os
# from dotenv import load_dotenv
# from flask import request, jsonify

# logging.basicConfig(filename='logs/app.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# load_dotenv('secrets/.env')
# ACCOUNT_SERVICE_KEY = os.environ['ACCOUNT_SERVICE_KEY']

# @functions_framework.http
# def open_weather_data_extract(request, context=None):
# # tutaj tylko main i granulacja
#     try:
#         request_json = request.get_json(silent=True)
#         if request_json and 'city_name' in request_json:
#             city_name = request_json['city_name']
#         else:
#             return jsonify({"error": "City name not provided"}), 400

#         openweather_key_api = os.environ['OPENWEATHER_API_KEY']

#         lat, lon = fetch_city_coordinates(city_name, openweather_key_api)
#         pollution_data = fetch_air_pollution_data(lat, lon, openweather_key_api)

#         pollution_data_with_metadata = add_metadata_to_data(pollution_data, city_name)

#         file_name = generate_filename(city_name)

#         upload_to_gcs(ACCOUNT_SERVICE_KEY, 'airpollution_bucket', file_name, pollution_data_with_metadata)
        
#         logging.info("Data ingestion and upload process completed successfully.")
        
#         response_data = {
#             "city_name": city_name,
#             "coordinates": {
#                 "latitude": lat,
#                 "longitude": lon
#             },
#             "pollution_data": pollution_data,
#             "pollution_data_with_metadata": pollution_data_with_metadata,
#             "file_name": file_name
#         }

#         return jsonify(response_data), 200

#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
#         return jsonify({"error": f"An error occurred: {e}"}), 500
