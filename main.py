import logging
from src.data_ingestion import fetch_city_coordinates, fetch_air_pollution_data, upload_to_gcs, add_metadata_to_data, generate_filename
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(filename='logs/app.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

load_dotenv('secrets/.env')
ACCOUNT_SERVICE_KEY = os.environ['ACCOUNT_SERVICE_KEY']

def main():
    try:
        city_name = input("Input city name: ")
        openweather_key_api = os.environ['OPENWEATHER_API_KEY']

        lat, lon = fetch_city_coordinates(city_name, openweather_key_api)
        pollution_data = fetch_air_pollution_data(lat, lon, openweather_key_api)

        pollution_data_with_metadata = add_metadata_to_data(pollution_data, city_name)

        file_name = generate_filename(city_name)

        # Upload data directly from the variable
        upload_to_gcs(ACCOUNT_SERVICE_KEY, 'airpollution_bucket', file_name, pollution_data_with_metadata)
        
        logging.info("Data ingestion and upload process completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
