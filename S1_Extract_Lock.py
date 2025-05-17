# S1_Extract_Lock.py

import pyspark
from pyspark.sql import SparkSession
import ConnectionConfig as cc
from delta import configure_spark_with_delta_pip

# Initialize Spark
cc.setupEnvironment()
builder = SparkSession.builder.appName("Extract_Lock") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[4]")

extra_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2", "org.postgresql:postgresql:42.7.4"]
builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("DEBUG")

# Set connection profile to velodb
cc.set_connectionProfile("velodb")
jdbc_url = cc.create_jdbc()

# Extract Locks table
df_locks = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url) \
    .option("dbtable", "locks") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

# Extract Stations table
df_stations = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url) \
    .option("dbtable", "stations") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

df_locks.createOrReplaceTempView("extracted_locks")
df_stations.createOrReplaceTempView("extracted_stations")

print("✅ Lock and Station data extracted successfully!")
