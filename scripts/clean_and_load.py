import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

def main():
    # 1. Initialize Spark Session with Kafka and PostgreSQL JDBC packages
    spark = SparkSession.builder \
        .appName("OpenMeteoKafkaToPostgresCleaner") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.5.0") \
        .getOrCreate()

    # 2. Define the expected schema for incoming Open-Meteo weather JSON payloads
    weather_schema = StructType([
        StructField("city", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("time", StringType(), True),
        StructField("temperature_2m", FloatType(), True),
        StructField("relative_humidity_2m", FloatType(), True),
        StructField("wind_speed_10m", FloatType(), True),
        StructField("weather_code", IntegerType(), True)
    ])

    # 3. Read raw batch messages from the Kafka topic
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "weather_raw_topic") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    # 4. Parse JSON and execute data cleaning operations
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), weather_schema).alias("data")) \
        .select("data.*")

    cleaned_df = parsed_df \
        .filter(col("city").isNotNull() & col("time").isNotNull()) \
        .dropDuplicates(["city", "time"]) \
        .withColumn("time_parsed", to_timestamp(col("time"))) \
        .withColumn("processed_at", current_timestamp()) \
        .select(
            col("city"),
            col("latitude"),
            col("longitude"),
            col("time_parsed").alias("time"),
            col("temperature_2m").alias("temperature_c"),
            col("relative_humidity_2m").alias("relative_humidity"),
            col("wind_speed_10m").alias("wind_speed_kmh"),
            col("weather_code"),
            col("processed_at")
        )

    # 5. Write cleaned DataFrame to the PostgreSQL Staging Table via JDBC
    jdbc_url = "jdbc:postgresql://postgres:5432/weather_db"
    connection_properties = {
        "user": "de_user",
        "password": "de_password",
        "driver": "org.postgresql.Driver",
        "batchsize": "5000"
    }

    cleaned_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "staging_weather") \
        .options(**connection_properties) \
        .mode("overwrite") \
        .save()

    print(f"Successfully processed and loaded {cleaned_df.count()} records into PostgreSQL staging table.")
    spark.stop()

if __name__ == "__main__":
    main()