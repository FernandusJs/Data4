from pyspark.sql.functions import col, hour, expr, from_unixtime, to_date, unix_timestamp, when, round
import os
import json
import ConnectionConfig as cc
from S1_Extract_Rides import *

# -----------------------------
# 🔁 Load main Rides data
# -----------------------------
cc.set_connectionProfile("velodb")
jdbc_url_velo = cc.create_jdbc()

df_rides_raw = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_velo) \
    .option("dbtable", "rides") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

df_rides_transformed = df_rides_raw.withColumn(
    "ride_duration", round((unix_timestamp("endtime") - unix_timestamp("starttime")) / 60, 2)
).withColumn("ride_date", to_date(col("starttime")))

# -----------------------------
# 📏 Compute ride distance
# -----------------------------
df_distance = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_velo) \
    .option("query", """
        SELECT rideid,
               CASE 
                 WHEN startpoint[0] = endpoint[0] AND startpoint[1] = endpoint[1]
                 THEN 0
                 ELSE haversine_km(
                    startpoint[1]::numeric, startpoint[0]::numeric,
                    endpoint[1]::numeric,   endpoint[0]::numeric
                 )
               END AS ride_distance
        FROM rides
    """) \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

df_rides_transformed = df_rides_transformed.join(df_distance, on="rideid", how="left")

# -----------------------------
# 🌦 Prepare coordinates for weather
# -----------------------------
df_rides_weather_coords = df_rides_transformed \
    .join(df_locks.selectExpr("lock_id as start_lock_id_dim", "gps_coord", "zipcode"),
          df_rides_transformed["startlockid"] == col("start_lock_id_dim"), how="left") \
    .withColumn("hour", hour("starttime")) \
    .withColumn("lat", expr("CAST(split(split(gps_coord, ',')[0], '\\(')[1] AS DOUBLE)")) \
    .withColumn("lon", expr("CAST(split(split(gps_coord, ',')[1], '\\)')[0] AS DOUBLE)"))

# ✅ Join with real weather data (from extracted table)
df_rides_weather = df_rides_weather_coords \
    .join(df_weather,
          (df_rides_weather_coords["zipcode"] == df_weather["zipcode"]) &
          (df_rides_weather_coords["ride_date"] == df_weather["ride_date"]) &
          (df_rides_weather_coords["hour"] == df_weather["hour"]),
          how="left") \
    .withColumn("weather_type",
                when(col("weather_description").isNull(), "Unknown")
                .otherwise(col("weather_description")))

# ✅ Join with WeatherDim to get weather_id
df_rides_weather = df_rides_weather.join(df_weather_dim, on="weather_type", how="left")

# -----------------------------
# 👤 SCD2 Join for user_sk
# -----------------------------

# Load subscriptions from velodb
df_subscriptions = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_velo) \
    .option("dbtable", "subscriptions") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

# ✅ Load UserDim from dw_rides (contains user_sk!)
cc.set_connectionProfile("dw_rides")
jdbc_url_dw = cc.create_jdbc()

df_users = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_dw) \
    .option("dbtable", "UserDim") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

# ✅ SCD2 Join
df_user_match = df_rides_weather.alias("r") \
    .join(df_subscriptions.alias("s"), col("r.subscriptionid") == col("s.subscriptionid"), how="left") \
    .join(df_users.alias("u"),
          (col("s.userid") == col("u.user_id")) &
          (col("r.starttime").cast("date").between(col("u.start_date"), col("u.end_date"))),
          how="left") \
    .selectExpr("r.*", "s.userid", "u.user_sk")

# Debug print
print("🧪 SCD2 Join — Sample with user_sk")
df_user_match.select("rideid", "subscriptionid", "userid", "user_sk").show(10, truncate=False)

# -----------------------------
# 🔗 Join with Lock & Vehicle
# -----------------------------
df_user_match = df_user_match \
    .join(df_locks.selectExpr("lock_id as start_lock_id_dim2", "lock_id as start_lock_id"),
          df_user_match["startlockid"] == col("start_lock_id_dim2")) \
    .join(df_locks.selectExpr("lock_id as end_lock_id_dim2", "lock_id as end_lock_id"),
          df_user_match["endlockid"] == col("end_lock_id_dim2")) \

vehicle_types = df_vehicles \
    .join(df_bikelots, on="bikelotid", how="left") \
    .join(df_bike_types, on="biketypeid", how="left") \
    .select(
        col("vehicleid").alias("vehicle_id_raw"),
        col("biketypedescription").alias("bike_type")
    )

vehicle_lookup = vehicle_types.join(
    df_vehicle_dim,
    vehicle_types["bike_type"] == df_vehicle_dim["type"],
    how="left"
).select(
    col("vehicle_id_raw"),
    df_vehicle_dim["vehicle_id"].alias("final_vehicle_id")
)


# 📅 Join with DateDim
df_user_match = df_user_match \
    .join(df_dates.selectExpr("date_sk as date_sk", "date as date_dim"),
          df_user_match["starttime"].cast("date") == col("date_dim"), how="left")
# 🚲 Join with VehicleDim
df_user_match = df_user_match \
    .join(vehicle_lookup, df_user_match["vehicleid"] == vehicle_lookup["vehicle_id_raw"], how="left") \
    .selectExpr("rideid", "user_sk", "start_lock_id", "end_lock_id", "date_sk", "weather_id",
                "final_vehicle_id as vehicleid", "ride_distance", "ride_duration")

# 🚫 Filter out invalid rides (negative duration)
df_user_match = df_user_match.filter(col("ride_duration") >= 0)

# 🧾 Final Fact Table
df_fact_rides = df_user_match.selectExpr(
    "rideid as ride_id",
    "user_sk",
    "start_lock_id",
    "end_lock_id",
    "date_sk",
    "weather_id",
    "vehicleid as vehicle_id",
    "ride_distance",
    "ride_duration"
)

df_fact_rides.show(10, truncate=False)

df_fact_rides.createOrReplaceTempView("transformed_fact_rides")
print("✅ RideFact transformed successfully")
