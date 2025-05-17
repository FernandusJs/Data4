import pyspark
from pyspark.sql import SparkSession
import ConnectionConfig as cc
from delta import configure_spark_with_delta_pip

# Initialize Spark
cc.setupEnvironment()
builder = SparkSession.builder.appName("Extract_Date") \
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

# Extract distinct dates from rides
df_rides = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url) \
    .option("dbtable", "rides") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

df_dates = df_rides.selectExpr("CAST(starttime AS DATE) AS ride_date").distinct() \
    .union(df_rides.selectExpr("CAST(endtime AS DATE) AS ride_date").distinct()) \
    .distinct()

df_dates.createOrReplaceTempView("extracted_dates")

print("Date data extracted successfully!")
