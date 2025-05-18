# S3_Export_All_To_Parquet.py

import ConnectionConfig as cc
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Setup Spark
cc.setupEnvironment()
builder = SparkSession.builder.appName("Export_All_To_Parquet") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[4]")\
    .config("spark.driver.memory", "4g")

extra_packages = ["org.postgresql:postgresql:42.7.4"]
builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)
spark = builder.getOrCreate()

# 🚀 Set DB and export path
cc.set_connectionProfile("dw_rides")  # Or velodb if you're exporting source tables
jdbc_url = cc.create_jdbc()
output_base = "output"  # or absolute path like "C:/Users/filip/ExportedParquet"

# 🗂 Tables to extract
tables = ["UserDim", "DateDim", "LockDim", "VehicleDim", "RidesFact", "WeatherDim"]

for table in tables:
    print(f"📥 Extracting {table}")
    df = spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("user", cc.get_Property("username")) \
        .option("password", cc.get_Property("password")) \
        .load()

    output_path = f"{output_base}/{table}"
    print(f"📦 Writing {table} to {output_path}")
    df.write.mode("overwrite").parquet(output_path)

print("✅ All tables exported to Parquet successfully.")
