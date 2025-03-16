# EXTRACT: USERS

from pyspark.sql import SparkSession
import ConnectionConfig as cc
from delta import configure_spark_with_delta_pip

#%% SETUP SPARK SESSION
cc.setupEnvironment()
extra_packages = ["org.postgresql:postgresql:42.7.4"]

builder = SparkSession.builder \
    .appName("ExtractUsers") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[4]")

builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("DEBUG")

#%% EXTRACT USERS DATA
cc.set_connectionProfile("operational_db")
jdbc_url = cc.create_jdbc()

df_users = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url) \
    .option("dbtable", "dim_users") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .load()

#%% SAVE TO STAGING AREA
df_users.write.format("delta").mode("overwrite").save("staging/users")

print("✅ User data extracted successfully!")
