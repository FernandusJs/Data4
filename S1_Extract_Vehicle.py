import pyspark
from pyspark.sql import SparkSession
import ConnectionConfig as cc
from delta import configure_spark_with_delta_pip

# Initialize Spark
cc.setupEnvironment()
builder = SparkSession.builder.appName("Extract_Vehicle") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[4]")

extra_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2", "org.postgresql:postgresql:42.7.4"]
builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("DEBUG")

# Set connection profile
cc.set_connectionProfile("velodb")
jdbc_url = cc.create_jdbc()

# Extract vehicle-related tables
df_vehicles = spark.read.format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url) \
    .option("dbtable", "vehicles") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

df_bikelots = spark.read.format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url) \
    .option("dbtable", "bikelots") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

df_bike_types = spark.read.format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url) \
    .option("dbtable", "bike_types") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

df_vehicles.createOrReplaceTempView("vehicles")
df_bikelots.createOrReplaceTempView("bikelots")
df_bike_types.createOrReplaceTempView("bike_types")

print("Vehicle data extracted successfully!")
