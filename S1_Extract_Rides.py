# S1_Extract_Rides.py

import pyspark
from pyspark.sql import SparkSession
import ConnectionConfig as cc
from delta import configure_spark_with_delta_pip

# ----------------------------------
# Step 1: Setup Spark + Delta config
# ----------------------------------
cc.setupEnvironment()

builder = SparkSession.builder.appName("Extract_RideFact") \
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
spark.sparkContext.setLogLevel("DEBUG")

# --------------------------------------------
# Step 2: Extract 'rides' from operational DB
# --------------------------------------------
cc.set_connectionProfile("velodb")
jdbc_url_velodb = cc.create_jdbc()

df_rides = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_velodb) \
    .option("dbtable", "rides") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

df_weather = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_velodb) \
    .option("dbtable", "weather") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

df_vehicles = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_velodb) \
    .option("dbtable", "vehicles") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

df_bike_types = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_velodb) \
    .option("dbtable", "bike_types") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

df_bikelots = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_velodb) \
    .option("dbtable", "bikelots") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()
# --------------------------------------------------
# Step 3: Extract DIM tables from data warehouse DB
# --------------------------------------------------
cc.set_connectionProfile("dw_rides")
jdbc_url_dw = cc.create_jdbc()

def read_dim(table):
    return spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", jdbc_url_dw) \
        .option("dbtable", table) \
        .option("user", cc.get_Property("username")) \
        .option("password", cc.get_Property("password")) \
        .load()

df_users       = read_dim("UserDim")
df_locks       = read_dim("LockDim")
df_dates       = read_dim("DateDim")
df_weather_dim = read_dim("WeatherDim")
df_vehicle_dim    = read_dim("VehicleDim")

# -----------------------------
# Step 4: Register Temp Views
# -----------------------------
df_rides.createOrReplaceTempView("rides")
df_weather.createOrReplaceTempView("weather")
df_users.createOrReplaceTempView("users")
df_locks.createOrReplaceTempView("locks")
df_dates.createOrReplaceTempView("dates")
df_weather_dim.createOrReplaceTempView("weatherdim")
df_vehicle_dim.createOrReplaceTempView("vehicles")

print("✅ Extraction complete: rides and weather (from velodb), dimensions (from dw_rides)")
