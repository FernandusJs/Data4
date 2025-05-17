from pyspark.sql.functions import col

from S1_Extract_Vehicle import spark

df_transformed = spark.sql("""
    SELECT 
        v.vehicleid AS vehicle_id,
        bt.biketypedescription AS type
    FROM vehicles v
    JOIN bikelots bl ON v.bikelotid = bl.bikelotid
    JOIN bike_types bt ON bl.biketypeid = bt.biketypeid
""")

df_transformed.createOrReplaceTempView("transformed_vehicle")

print("Vehicle data transformed successfully!")
