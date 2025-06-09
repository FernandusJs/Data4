from S2_Transform_Rides import df_fact_rides
import ConnectionConfig as cc

# Set connection profile to target DB
cc.set_connectionProfile("dw_rides")
jdbc_url_target = cc.create_jdbc()

# Write to RidesFact table
df_fact_rides.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_target) \
    .option("dbtable", "RidesFact") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .mode("overwrite") \
    .save()

print("✅ loaded into RidesFact successfully!")