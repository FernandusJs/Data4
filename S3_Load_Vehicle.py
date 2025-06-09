# S3_Load_Vehicle.py

from S2_Transform_Vehicle import df_transformed
import ConnectionConfig as cc

# Connect to dw_rides
cc.set_connectionProfile("dw_rides")
jdbc_url_target = cc.create_jdbc()

# Write to VehicleDim table
df_transformed.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_target) \
    .option("dbtable", "VehicleDim") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .mode("overwrite") \
    .save()

print("✅ S3: VehicleDim table loaded successfully!")
