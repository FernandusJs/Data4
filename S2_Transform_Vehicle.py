from S1_Extract_Vehicle import spark
from pyspark.sql.functions import col

# Load the source
df_bike_types = spark.sql("SELECT * FROM bike_types")

# Rename columns to match target schema
df_transformed = df_bike_types.select(
    col("biketypeid").alias("vehicle_id"),
    col("biketypedescription").alias("type")
)

# Register for use in S3
df_transformed.createOrReplaceTempView("transformed_vehicle")

print("✅ S2: bike_types renamed to match VehicleDim schema.")
