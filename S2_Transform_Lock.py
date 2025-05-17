# S2_Transform_Lock.py

from S1_Extract_Lock import spark
from pyspark.sql.types import StructType, StructField, StringType

# Join locks and stations
df_transformed = spark.sql("""
    SELECT
        l.stationid AS station_id,
        s.stationnr AS station_nr,
        s.street AS street,
        s.number AS number,
        s.zipcode AS zipcode,
        s.district AS district,
        s.gpscoord AS gps_coord
    FROM extracted_locks l
    JOIN extracted_stations s ON l.stationid = s.stationid
""")

# Define "no lock" row
schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("station_nr", StringType(), True),
    StructField("street", StringType(), True),
    StructField("number", StringType(), True),
    StructField("zipcode", StringType(), True),
    StructField("district", StringType(), True),
    StructField("gps_coord", StringType(), True),
])

df_no_lock = spark.createDataFrame([
    (None, "no lock", None, None, None, None, None)
], schema=schema)

# Combine data
df_final = df_transformed.union(df_no_lock)

df_final.createOrReplaceTempView("transformed_locks")

print("✅ Lock dimension data transformed successfully!")
