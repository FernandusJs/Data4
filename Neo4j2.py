from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, round, unix_timestamp
import ConnectionConfig as cc
from delta import configure_spark_with_delta_pip

# 1. Initialize Spark
cc.setupEnvironment()
builder = SparkSession.builder \
    .appName("Neo4j") \
    .master("local[4]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

extra_packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
    "org.postgresql:postgresql:42.7.4"
]

builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)
spark = builder.getOrCreate()


# 2. Load data from veloDB
cc.set_connectionProfile("velodb")
jdbc_url = cc.create_jdbc()

rides = spark.read.format("jdbc").option("driver", "org.postgresql.Driver").option("url", jdbc_url).option("dbtable", "rides") \
    .option("user", cc.get_Property("username")).option("password", cc.get_Property("password")).load()

stations = spark.read.format("jdbc").option("driver", "org.postgresql.Driver").option("url", jdbc_url).option("dbtable", "stations") \
    .option("user", cc.get_Property("username")).option("password", cc.get_Property("password")).load()

vehicles = spark.read.format("jdbc").option("driver", "org.postgresql.Driver").option("url", jdbc_url).option("dbtable", "vehicles") \
    .option("user", cc.get_Property("username")).option("password", cc.get_Property("password")).load()

locks = spark.read.format("jdbc").option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url).option("dbtable", "locks") \
    .option("user", cc.get_Property("username")).option("password", cc.get_Property("password")) \
    .load()

# 3. Load user dimension from data warehouse
cc.set_connectionProfile("dw_rides")
jdbc_url_dw = cc.create_jdbc()

users = spark.read.format("jdbc").option("driver", "org.postgresql.Driver").option("url", jdbc_url_dw).option("dbtable", "UserDim") \
    .option("user", cc.get_Property("username")).option("password", cc.get_Property("password")).load()

# 4. Filter rides for two days and compute ride duration
rides = rides.withColumn("ride_date", to_date("starttime")) \
    .filter((col("ride_date") >= "2022-05-01") & (col("ride_date") <= "2022-05-02")) \
    .withColumn("ride_duration", round((unix_timestamp("endtime") - unix_timestamp("starttime")) / 60, 2)) \
    .filter(col("ride_duration") > 0)

# 5. Load locks (needed to link start/end locks to station IDs)

# 6. Join rides to locks to get stationid for startlock
rides = rides.join(
    locks.selectExpr("lockid as startlockid_l", "stationid as start_station_id"),
    rides["startlockid"] == col("startlockid_l"),
    "left"
).drop("startlockid_l")

# 7. Join rides to stations to get start station info
rides = rides.join(
    stations.selectExpr(
        "stationid as stationid_start",
        "stationnr as start_station_nr",
        "street as start_street",
        "district as start_district"
    ),
    rides["start_station_id"] == col("stationid_start"),
    "left"
).drop("stationid_start")

# 8. Join rides to locks to get stationid for endlock
rides = rides.join(
    locks.selectExpr("lockid as endlockid_l", "stationid as end_station_id"),
    rides["endlockid"] == col("endlockid_l"),
    "left"
).drop("endlockid_l")

# 9. Join rides to stations to get end station info
rides = rides.join(
    stations.selectExpr(
        "stationid as stationid_end",
        "stationnr as end_station_nr",
        "street as end_street",
        "district as end_district"
    ),
    rides["end_station_id"] == col("stationid_end"),
    "left"
).drop("stationid_end")

# 6. Enrich with vehicle info
rides = rides.join(vehicles.select("vehicleid", "serialnumber"), "vehicleid", "left")

# 7. Enrich with user info
rides = rides.join(users.selectExpr("user_sk", "user_id"),
                   rides["subscriptionid"] == users["user_id"], "left")

# 8. Final select for Neo4j JSON export
rides_clean = rides.selectExpr(
    "rideid",
    "user_sk",
    "vehicleid",
    "startlockid as start_station_id",
    "start_district",
    "endlockid as end_station_id",
    "end_district",
    "ride_duration",
    "starttime as datetime"
)

# 9. Export to JSON for Neo4j import folder
output_path = "C:/Users/filip/.Neo4jDesktop/relate-data/dbmss/dbms-a281f508-8f20-4527-9cd1-1c1c1ccf661a/import/rides_graph2.json"

rides_clean = rides_clean.filter(
    col("vehicleid").isNotNull() &
    col("user_sk").isNotNull() &
    col("start_station_id").isNotNull() &
    col("end_station_id").isNotNull()
).limit(10000)

rides_clean.write.mode("overwrite").json(output_path)

print(f" JSON export complete! File saved at: {output_path}")
