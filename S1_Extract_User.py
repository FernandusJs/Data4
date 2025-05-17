import pyspark
from pyspark.sql import SparkSession
import ConnectionConfig as cc
from delta import configure_spark_with_delta_pip

# Initialize Spark environment
cc.setupEnvironment()

builder = SparkSession.builder.appName("Extract_User") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[4]")

extra_packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
    "org.postgresql:postgresql:42.7.4"
]
builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("DEBUG")

# Connect to velodb
cc.set_connectionProfile("velodb")
jdbc_url = cc.create_jdbc()

# 🚴 Extract velo_users
df_users = spark.read.format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url) \
    .option("dbtable", "velo_users") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

# 📦 Extract subscriptions
df_subscriptions = spark.read.format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url) \
    .option("dbtable", "subscriptions") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

# 📋 Extract subscription_types (needed for duration logic)
df_subscription_types = spark.read.format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url) \
    .option("dbtable", "subscription_types") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

# 🗂 Register views for SQL use
df_users.createOrReplaceTempView("extracted_users")
df_subscriptions.createOrReplaceTempView("extracted_subscriptions")
df_subscription_types.createOrReplaceTempView("subscription_types")

print("✅ User, Subscriptions, and Subscription Types extracted successfully!")