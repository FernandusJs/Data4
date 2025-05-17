# S1_Extract_Weather.py

import ConnectionConfig as cc
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Initialize Spark (just like DateDim script)
cc.setupEnvironment()
builder = SparkSession.builder.appName("Extract_Weather") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[4]")

extra_packages = ["org.postgresql:postgresql:42.7.4"]
builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)

# >>>> This is the SparkSession that will be imported later <<<<
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("DEBUG")

# Extract weather CSV
weather_df = spark.read.csv("FileStore/weather_dim.csv", header=True, inferSchema=True)

# Save as temp view
weather_df.createOrReplaceTempView("extracted_weather")

print("✅ Weather data extracted successfully!")

