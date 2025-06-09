# S1_Extract_Vehicle.py

import pyspark
from pyspark.sql import SparkSession
import ConnectionConfig as cc
from delta import configure_spark_with_delta_pip

# Set up Spark
cc.setupEnvironment()
builder = SparkSession.builder.appName("Extract_Vehicle") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[4]")

extra_packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
    "org.postgresql:postgresql:42.7.4"
]
builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Connect to velodb
cc.set_connectionProfile("velodb")
jdbc_url = cc.create_jdbc()

# Extract bike_types table
df_bike_types = spark.read.format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url) \
    .option("dbtable", "bike_types") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

# Register as view
df_bike_types.createOrReplaceTempView("bike_types")

print("✅ S1: bike_types data extracted successfully!")
