import json
import requests
from kafka import KafkaProducer

CITIES = [
    {"name": "New_York", "lat": 40.7128, "lon": -74.0060},
    {"name": "London", "lat": 51.5074, "lon": -0.1278},
    {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503},
    {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
    {"name": "Sydney", "lat": -33.8688, "lon": 151.2093}
]

def fetch_and_produce():
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    for city in CITIES:
        # Open-Meteo forecast API endpoint
        url = f"https://api.open-meteo.com/v1/forecast?latitude={city['lat']}&longitude={city['lon']}&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code"
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                # Payload structure sent to Kafka topic
                payload = {
                    "city": city["name"],
                    "latitude": data.get("latitude"),
                    "longitude": data.get("longitude"),
                    "time": data.get("current", {}).get("time"),
                    "temperature_2m": data.get("current", {}).get("temperature_2m"),
                    "relative_humidity_2m": data.get("current", {}).get("relative_humidity_2m"),
                    "wind_speed_10m": data.get("current", {}).get("wind_speed_10m"),
                    "weather_code": data.get("current", {}).get("weather_code")
                }
                
                producer.send('weather_raw_topic', value=payload)
                print(f"Pushed weather record for {city['name']} to Kafka.")
        except Exception as e:
            print(f"Failed to fetch data for {city['name']}: {e}")
            
    producer.flush()

if __name__ == "__main__":
    fetch_and_produce()