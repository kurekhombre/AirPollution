import os
import json
import requests

from dotenv import load_dotenv

load_dotenv('secrets/.env')
openweather_key_api = os.environ['OPENWEATHER_API_KEY']

# city_name = input("City name:")
city_api = f"http://api.openweathermap.org/geo/1.0/direct?q=Marki&limit=1&appid={openweather_key_api}"

r = requests.get(city_api)
x = json.loads(r.text)
print(x)
lat=x[0]['lat']
lon=x[0]['lon']
print(lat,lon)

pollution_api = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={openweather_key_api}'
r = requests.get(pollution_api)
x = json.loads(r.text)
print(x)