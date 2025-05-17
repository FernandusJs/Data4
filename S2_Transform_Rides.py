from pyspark.sql.functions import col, hour, expr, from_unixtime, to_date, unix_timestamp, when, round
import os
import json
import ConnectionConfig as cc
from S1_Extract_Rides import *

# 🔁 Add connection to velodb for ride details (rides table)
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


# 🧮 1. Calculate duration and extract ride_date
df_rides_transformed = df_rides_raw.withColumn(
    "ride_duration",
    round((unix_timestamp("endtime") - unix_timestamp("starttime")) / 60, 2)
).withColumn("ride_date", to_date(col("starttime")))

# 📏 2. Compute distance using haversine_km (PostgreSQL)
cc.set_connectionProfile("velodb")
jdbc_url_velo = cc.create_jdbc()

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
                    startpoint[1]::numeric, startpoint[0]::numeric,  -- lat1, lon1
                    endpoint[1]::numeric,   endpoint[0]::numeric     -- lat2, lon2
                )
                END AS ride_distance
        FROM rides
    """) \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

# 👇 Rename "rideid" to "ride_id" so it matches your df_rides_transformed
#df_distance = df_distance.withColumnRenamed("rideid", "rideid")

# 👇 Join distance back into the main dataframe
df_rides_transformed = df_rides_transformed.join(df_distance, on="rideid", how="left")

# 3. Prepare weather input (lat, lon, time) for API

df_rides_weather_coords = df_rides_transformed \
    .join(df_locks.selectExpr("lock_id as start_lock_id_dim", "gps_coord", "zipcode"),
          df_rides_transformed["startlockid"] == col("start_lock_id_dim"),
          how="left") \
    .withColumn("hour", hour("starttime")) \
    .withColumn("lat", expr("CAST(split(split(gps_coord, ',')[0], '\\(')[1] AS DOUBLE)")) \
    .withColumn("lon", expr("CAST(split(split(gps_coord, ',')[1], '\\)')[0] AS DOUBLE)"))

# 🪄 4. You now use lat, lon, ride_date to call weather API (outside of Spark)
# Loop over lat/lon/date combinations and fetch weather using Open Meteo historical API
# Then create DataFrame with schema: zipcode, ride_date, hour, weather_type
# For now, assume this DataFrame is available as: df_weather_raw

# Example dummy weather_raw structure
df_weather_raw = spark.createDataFrame([
    (2000, "2025-05-12", 14, "Unpleasant"),
    (3000, "2025-05-12", 15, "Pleasant"),
    # Add all real values later
], ["zipcode", "ride_date", "hour", "weather_type"])

# 5️⃣ Match ride to weather using zip, hour, date
df_rides_weather = df_rides_weather_coords \
    .join(df_weather_raw,
          (df_rides_weather_coords["zipcode"] == df_weather_raw["zipcode"]) &
          (df_rides_weather_coords["ride_date"] == df_weather_raw["ride_date"]) &
          (df_rides_weather_coords["hour"] == df_weather_raw["hour"]),
          how="left") \
    .withColumn("weather_type", when(col("weather_type").isNull(), "Unknown").otherwise(col("weather_type")))

# 🔄 6. Map weather_type to weather_id
df_rides_weather = df_rides_weather.join(df_weather_dim, on="weather_type", how="left")

# 👤 7. Join subscriptions to get userid, then join with UserDim to get user_sk
cc.set_connectionProfile("velodb")
jdbc_url_velo = cc.create_jdbc()

# 🔄 Load subscriptions table
df_subscriptions = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_velo) \
    .option("dbtable", "subscriptions") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

# 🔁 Join rides_weather → subscriptions → UserDim (with SCD2 logic)
print("🧪 DEBUG: Showing a few records from df_rides_weather:")
df_rides_weather.select("rideid", "subscriptionid", "starttime").show(5, truncate=False)

print("🧪 DEBUG: Showing a few records from df_subscriptions:")
df_subscriptions.select("subscriptionid", "userid").show(5, truncate=False)

print("🧪 DEBUG: Showing a few records from df_users:")
df_users.select("user_id", "user_sk", "start_date", "end_date").show(5, truncate=False)

# Join rides → subscriptions → users (SCD2 logic)
df_user_match = df_rides_weather.alias("r") \
    .join(df_subscriptions.alias("s"),
          col("r.subscriptionid") == col("s.subscriptionid"),
          how="left") \
    .join(df_users.alias("u"),
          (col("s.userid") == col("u.user_id")) &
          (col("r.starttime") == (col("u.start_date"))),
          how="left") \
    .selectExpr("r.*", "s.userid", "u.user_sk")

# Check result after join
print("🧪 DEBUG: User match join result — should contain user_sk if match succeeded:")
df_user_match.select("rideid", "subscriptionid", "userid", "user_sk").show(10, truncate=False)

# 🔗 8. Join with locks and vehicles
df_user_match = df_user_match \
    .join(df_locks.selectExpr("lock_id as start_lock_id_dim2", "lock_id as start_lock_id"),
          df_user_match["startlockid"] == col("start_lock_id_dim2")) \
    .join(df_locks.selectExpr("lock_id as end_lock_id_dim2", "lock_id as end_lock_id"),
          df_user_match["endlockid"] == col("end_lock_id_dim2")) \
    .join(df_vehicles.selectExpr("vehicle_id", "vehicle_id"), df_user_match["vehicleid"] == col("vehicle_id"))

# 📅 9. Join with DateDim
df_user_match = df_user_match \
    .join(df_dates.selectExpr("date_sk as date_sk", "date as date_dim"),
          df_user_match["starttime"].cast("date") == col("date_dim"),
          how="left")

# 🧾 10. Final output
df_fact_rides = df_user_match.selectExpr(
    "rideid as ride_id",
    "user_sk",
    "start_lock_id",
    "end_lock_id",
    "date_sk",
    "weather_id",
    "vehicle_id",
    "ride_distance",
    "ride_duration"
)

df_fact_rides.createOrReplaceTempView("transformed_fact_rides")
print("✅ RideFact transformed successful")