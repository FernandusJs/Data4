#LOAD: USERS
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import ConnectionConfig as cc

#%% SETUP SPARK SESSION
cc.setupEnvironment()
extra_packages = ["org.postgresql:postgresql:42.7.4"]

builder = SparkSession.builder \
    .appName("LoadUsers") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[4]")

builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("DEBUG")

#%% READ TRANSFORMED DATA
df_users = spark.read.format("delta").load("transformed/users")

#%% LOAD INTO POSTGRESQL DATA WAREHOUSE
cc.set_connectionProfile("data_warehouse")
jdbc_url = cc.create_jdbc()

df_users.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url) \
    .option("dbtable", "dim_users") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .mode("append") \
    .save()

print("✅ User data loaded into the data warehouse successfully!")
