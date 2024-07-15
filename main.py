import logging
from src.data_ingestion import fetch_city_coordinates, fetch_air_pollution_data, upload_to_gcs, add_metadata_to_data, generate_filename
import os
from dotenv import load_dotenv
import yaml
from flask import jsonify, request
from google.cloud import storage
import functions_framework


logging.basicConfig(filename='logs/app.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

OPENWEATHER_API_KEY="90da9cd29152fb94e06a1ec2f90d7557"
ACCOUNT_SERVICE_KEY={
  "type": "service_account",
  "project_id": "able-store-371110",
  "private_key_id": "4ca3407b17d67e962a5bfd7491592944e2e19419",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCgSU8Ec8Dqab9f\noswYkLvbXYIr5HKXyC7eQFsoQ3Ahw8KK2Bzc/cUs5sQS7yoJtf5wA3YZUqKEtgbh\naMiS0xhipzAPd1ju21/PXErdehtaUdV79gRyEVgvqZa+CglNaPrlBBke9ahLEIdU\nhn6KtufhRbr5vtipwHd483Az/nU+z6h7DXLhepCDS2pY7SBs7xaaKoPWiezBez3v\nQnBN19XsAFjj9ec1pMwV82/IIEkVLjEU7yxno0GwroVoZB8gLCMz2DyF9q0Gt9nS\nzJaofqhZ1nNb0IG4X9MKUdxMk5jvlWmhVhpYJpEQ0mYbb2qugzL8Ns7gPQ2dFwyY\nWm1DRSEhAgMBAAECggEAOncjBGu4hYi3DopSSe+ZJkXwclQs3GJffiAZYBFhOF+w\neXak6JMENgGiqYllWw9wVKlejZQPxbqjvdq7tpqbPXjgVfPhk5afVwLCKBEfIw3Q\nghvVuUnMUe5ZFs6QYBUX1ytsgld24JKR2moIhE59PDV0Ix7S5vBWY97gIYcCezst\nwQhvDuJvUV2OX3kN9+p1MEpqzZ1wL7KM8l4DU0xtP13/R7H2p+8M3NmILnYW1tSu\nodPI9TTczpXLhr/GgCJ97VIny1meFbMtE55/wFp8zIQE5DBjDazoG6gwFOLk5APP\nBG5XXDSnBBoEcRXE2Ele8faZSIuHKcppAydGjmJXlQKBgQDTIBLzBWIM5a/yIp/1\nNdCQRL5/+AqHJB9L3elzTeNfuU6evWwmWyE8Bhs/ZsSJDSYyAACV/KTmVEboI/h3\nxe7k8oZdOUckPogtHZta6Z3FOxjidQxoDp8ZjE9n45xm5wjX8ELQ92rJxRLL5lHk\nKIiTSdzlbACMe1X34W2KGP8GQwKBgQDCWvHCh+rTvjJBgoubzMjRrLG65bPG7Sp2\nfHMGeVhA6XaNfIH/CA1OB+piInjPsFnBTnThpi8433zkW97AglAGtZOSMxuos8hd\n5C1wO3cPBDdS2dZTEB8CajSTbvEHh7YvJPcEV2fhodEYW+w2BdUFlvvXR3w3OalR\n0znucgaOywKBgB/QsYpVwVTYOA9xEFHPbkKPWXXHVy7OTrkME+YAUsdWEXbybJ3L\nFdMJyIlhXzSX+q2GOpBfpinfgV/yK7tA5KMzuPPLceEQW00RLRwiIFhcc3+My/XS\nXj9nWX/6WNNY9Yg0+Jys7DbDr/VyG90aDprDgro/8EU7QNU971fkNGopAoGAfQRa\n9rLPDp0NXq5Gd1Vm59iaCoqu9YqhAc5TfxqW9ko2hBRO2mmnhUX6Ml3SMGZTldCN\nxohLX94CTDH8OwCX0XkDD8voQsBCZoLb55GYqAQiQhz1jECWccs3Po7iY+GcAv9Y\nFWbT34NjKLQYYTenKw1Puc61zCA/Fe81GHgoV7kCgYBl6zm54PrInC4Dur2HzrY4\n6mDce4JhqMIa9ChlkHd7LF1nnO5b2K4ax69qvGlKUctbY1//1ELaNobmGuSPzACT\nJ13bqpfTZpyHZQo+Z2OOx/CaVsdY2Qr4FXNFZPp8ut2rrzhMPKxIwxh2STEdj4rJ\n62C4Dsacs+3DN1eI6bbtdQ==\n-----END PRIVATE KEY-----\n",
  "client_email": "airpollution@able-store-371110.iam.gserviceaccount.com",
  "client_id": "103783586978149637871",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/airpollution%40able-store-371110.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

GCS_BUCKET_NAME="airpollution_bucket"

def process_data_extraction(city_name, config):
    # OPENWEATHER_API_KEY = config['openweather']['api_key']
    # GCS_BUCKET_NAME = config['gcs']['bucket_name']
    # ACCOUNT_SERVICE_KEY = config['gcs']['account_service_key']
    
    lat, lon = fetch_city_coordinates(city_name, OPENWEATHER_API_KEY)
    pollution_data = fetch_air_pollution_data(lat, lon, OPENWEATHER_API_KEY)

    pollution_data_with_metadata = add_metadata_to_data(pollution_data, city_name) if config['output']['metadata'] else pollution_data

    file_name = generate_filename(city_name)

    upload_to_gcs(ACCOUNT_SERVICE_KEY, GCS_BUCKET_NAME, file_name, pollution_data_with_metadata)
    
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

    return response_data

with open('config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

@functions_framework.http
def open_weather_data_extract(request, context=None):
    try:
        request_json = request.get_json(silent=True)
        if request_json and 'city_name' in request_json:
            city_name = request_json['city_name']
        else:
            return jsonify({"error": "City name not provided"}), 400

        lat, lon = fetch_city_coordinates(city_name, OPENWEATHER_API_KEY)
        pollution_data = fetch_air_pollution_data(lat, lon, OPENWEATHER_API_KEY)

        pollution_data_with_metadata = add_metadata_to_data(pollution_data, city_name) if config['output']['metadata'] else pollution_data

        file_name = generate_filename(city_name)

        # upload_to_gcs(ACCOUNT_SERVICE_KEY, GCS_BUCKET_NAME, file_name, pollution_data_with_metadata)
        
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

        return jsonify(response_data), 200

    except Exception as e:
        # logging.error(f"An error occurred: {e}")
        return jsonify({"error": f"An error occurred: {e}"}), 500

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
