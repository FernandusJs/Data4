# S3_Load_User.py

from S2_Transform_User import df_scd2
import ConnectionConfig as cc

# Set connection to dw_rides
cc.set_connectionProfile("dw_rides")
jdbc_url_target = cc.create_jdbc()

# Write to UserDim table
df_scd2.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_target) \
    .option("dbtable", "UserDim") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .mode("overwrite") \
    .save()

print("✅ UserDim table loaded successfully into dw_rides!")
