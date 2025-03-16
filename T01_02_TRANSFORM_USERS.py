#TRANSFORM: USERS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from delta import configure_spark_with_delta_pip
import ConnectionConfig as cc

#%% SETUP SPARK SESSION
cc.setupEnvironment()

extra_packages = ["org.postgresql:postgresql:42.7.4"]

builder = SparkSession.builder \
    .appName("TransformUsers") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[4]")

builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("DEBUG")

#%% READ STAGED DATA
df_users = spark.read.format("delta").load("staging/users")

#%% APPLY SCD TYPE 2 TRANSFORMATION
df_users_transformed = df_users.withColumn("valid_from", current_timestamp()) \
                               .withColumn("valid_to", lit(None)) \
                               .withColumn("is_current", lit(True))

#%% SAVE TRANSFORMED DATA
df_users_transformed.write.format("delta").mode("overwrite").save("transformed/users")

print("✅ User data transformed successfully!")
