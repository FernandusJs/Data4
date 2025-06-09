from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, round, unix_timestamp
import ConnectionConfig as cc
from delta import configure_spark_with_delta_pip

# 1. Initialize Spark
cc.setupEnvironment()
builder = SparkSession.builder.appName("Neo4j") \
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

# Load data
cc.set_connectionProfile("velodb")
jdbc_url = cc.create_jdbc()

rides = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "rides") \
    .option("user", cc.get_Property("username")).option("password", cc.get_Property("password")).load()

stations = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "stations") \
    .option("user", cc.get_Property("username")).option("password", cc.get_Property("password")).load()

vehicles = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "vehicles") \
    .option("user", cc.get_Property("username")).option("password", cc.get_Property("password")).load()

# Load user dim from data warehouse
cc.set_connectionProfile("dw_rides")
jdbc_url_dw = cc.create_jdbc()

users = spark.read.format("jdbc").option("url", jdbc_url_dw).option("dbtable", "UserDim") \
    .option("user", cc.get_Property("username")).option("password", cc.get_Property("password")).load()

# Prepare transformed dataset (May 1–2, 2022)
rides = rides.withColumn("ride_date", to_date("starttime")) \
    .filter((col("ride_date") >= "2022-05-01") & (col("ride_date") <= "2022-05-02")) \
    .withColumn("ride_duration", round((unix_timestamp("endtime") - unix_timestamp("starttime")) / 60, 2)) \
    .filter(col("ride_duration") > 0)

# Join station info
rides = rides.join(stations.selectExpr("stationid as start_station_id", "district as start_district"),
                   rides["startlockid"] == col("start_station_id"), "left") \
             .join(stations.selectExpr("stationid as end_station_id", "district as end_district"),
                   rides["endlockid"] == col("end_station_id"), "left")

# Join vehicle and user
rides = rides.join(vehicles, "vehicleid", "left") \
             .join(users.selectExpr("user_sk", "user_id"), rides["subscriptionid"] == users["user_id"], "left")

# Select final JSON structure
rides_clean = rides.selectExpr(
    "rideid", "user_sk", "vehicleid",
    "startlockid as start_station_id", "endlockid as end_station_id",
    "start_district", "end_district",
    "ride_duration", "starttime as datetime"
)

# 11. Save as JSON
output_path = "C:/Users/filip/.Neo4jDesktop/relate-data/dbmss/dbms-6a02f86a-ce4e-4736-92bd-c800344c1d7b/import/rides_graph.json"
rides_clean.write.mode("overwrite").json(output_path)

print(f"✅ JSON export complete! File saved at: {output_path}")

