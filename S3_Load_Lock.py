# S3_Load_Lock.py

from S2_Transform_Lock import df_final
import ConnectionConfig as cc

# Set connection profile to dw_rides
cc.set_connectionProfile("dw_rides")
jdbc_url_target = cc.create_jdbc()

# Write to LockDim table
df_final.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_target) \
    .option("dbtable", "LockDim") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .mode("append") \
    .save()

print("✅ LockDim table loaded successfully into dw_rides!")
