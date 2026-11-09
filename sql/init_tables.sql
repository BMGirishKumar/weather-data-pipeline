CREATE TABLE IF NOT EXISTS production_weather (
    city VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT,
    time TIMESTAMP,
    temperature_c FLOAT,
    relative_humidity FLOAT,
    wind_speed_kmh FLOAT,
    weather_code INT,
    processed_at TIMESTAMP,
    PRIMARY KEY (city, time)
);

CREATE TABLE IF NOT EXISTS staging_weather (
    city VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT,
    time TIMESTAMP,
    temperature_c FLOAT,
    relative_humidity FLOAT,
    wind_speed_kmh FLOAT,
    weather_code INT,
    processed_at TIMESTAMP
);