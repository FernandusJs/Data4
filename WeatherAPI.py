from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_date, expr
import requests
import time

from pyspark.sql.types import IntegerType

import ConnectionConfig as cc
from delta import configure_spark_with_delta_pip

# ─── Spark Setup ────────────────────────────────────────────────────────────────
cc.setupEnvironment()
builder = SparkSession.builder.appName("Weather API ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[4]")\
    .config("spark.driver.memory", "4g")

extra_packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
    "org.postgresql:postgresql:42.7.4"
]
builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ─── Load PostgreSQL Data ───────────────────────────────────────────────────────
cc.set_connectionProfile("velodb")
jdbc_url = cc.create_jdbc()

def read_table(name):
    return spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", name) \
        .option("user", cc.get_Property("username")) \
        .option("password", cc.get_Property("password")) \
        .load()

df_rides = read_table("rides")
df_locks = read_table("locks")
df_stations = read_table("stations")

# ─── Enrich Rides with GPS ──────────────────────────────────────────────────────
df_rides = df_rides.withColumn("ride_date", to_date("starttime"))

df_locks_enriched = df_locks.join(
    df_stations.select("stationid", "zipcode", "gpscoord"),
    on="stationid", how="left"
)

df_weather_input = df_rides.join(
    df_locks_enriched.selectExpr("lockid as start_lock_id_dim", "gpscoord", "zipcode"),
    df_rides["startlockid"] == col("start_lock_id_dim"),
    how="left"
).withColumn("hour", hour("starttime")) \
 .withColumn("lat", expr("CAST(split(split(gpscoord, ',')[0], '\\\\(')[1] AS DOUBLE)")) \
 .withColumn("lon", expr("CAST(split(split(gpscoord, ',')[1], '\\\\)')[0] AS DOUBLE)"))\
    .select("zipcode", "ride_date", "hour", "lat", "lon") \
 .distinct()


# ─── Open-Meteo API Integration ────────────────────────────────────────────────
URL = "https://archive-api.open-meteo.com/v1/archive"
weather_data = []

df_weather_input_grouped = df_weather_input.select("zipcode", "ride_date", "lat", "lon") \
    .distinct()

for row in df_weather_input_grouped.limit(5000).toLocalIterator():
    zipcode = row['zipcode']
    date = row['ride_date']
    lat = row['lat']
    lon = row['lon']

    if None in (lat, lon, date):
        continue

    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,weathercode,precipitation",
            "start_date": date.strftime('%Y-%m-%d'),
            "end_date": date.strftime('%Y-%m-%d'),
            "timezone": "Europe/Brussels"
        }

        response = requests.get(URL, params=params, timeout=10)
        if response.status_code != 200:
            print(f"[ERROR] API failed for {lat},{lon} | status: {response.status_code}")
            continue

        data = response.json()
        hourly_data = data.get("hourly", {})

        temps = hourly_data.get("temperature_2m", [])
        precs = hourly_data.get("precipitation", [])
        codes = hourly_data.get("weathercode", [])

        if not temps or not precs or not codes or len(temps) < 24:
            print(f"[WARN] Incomplete weather data for {lat},{lon}")
            continue

        for hour_val in range(24):
            temp = temps[hour_val]
            precip = precs[hour_val]
            code = codes[hour_val]

            if None in (temp, precip, code):
                category = "Unknown"
            elif precip > 0:
                category = "Unpleasant"
            elif temp >= 15 and code == [0, 1, 2, 3]:
                category = "Pleasant"
            else:
                category = "Neutral"

            weather_data.append((
                zipcode,
                date,
                hour_val,
                float(temp),
                category
            ))

        time.sleep(1)  # avoid rate limiting

    except Exception as e:
        print(f"[EXCEPTION] {e} | lat: {lat}, lon: {lon}")

# ─── Save to PostgreSQL ────────────────────────────────────────────────────────
if weather_data:
    df_weather = spark.createDataFrame(
        weather_data,
        ["zipcode", "ride_date", "hour", "temperature", "weather_description"]
    ).withColumn("zipcode",col("zipcode").cast(IntegerType()))

    df_weather.show(10, truncate=False)

    df_weather.write.format("jdbc") \
         .option("url", jdbc_url) \
         .option("driver", "org.postgresql.Driver") \
         .option("dbtable", "weather") \
         .option("user", cc.get_Property("username")) \
         .option("password", cc.get_Property("password")) \
         .mode("append") \
         .save()
else:
    print("No weather data collected. Nothing to write.")