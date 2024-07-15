import os
import json
import requests
from google.cloud import storage
from dotenv import load_dotenv
from datetime import datetime
import logging

load_dotenv('secrets/.env')

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
    logging.info(f"File {file_name} uploaded to {bucket_name}.")

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

# def get_air_pollution_history_data(
#             self,
#             lat: float,
#             lon: float,
#             unix_start_date: int,
#             unix_end_date: int,
#     ) -> dict:
#         """
        
#         Get historical air pollution data for given coordinates.
        
#         """
#         url = f"{self.AIR_POLLUTION_HISTORY_URL}lat={lat}&lon={lon}&start={unix_start_date}&end={unix_end_date}&appid={self.openweather_api_key}"
#         return self.get_data_from_url(url)


# AIR_POLLUTION_HISTORY_URL = 'http://api.openweathermap.org/data/2.5/air_pollution/history?'
# """