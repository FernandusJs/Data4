import ConnectionConfig as cc
from S2_Transform_Date import df_transformed

cc.set_connectionProfile("dw_rides")
jdbc_url_target = cc.create_jdbc()
df_transformed = df_transformed.withColumnRenamed("ride_date", "date")

df_transformed.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", jdbc_url_target) \
    .option("dbtable", "DateDim") \
    .option("user", cc.get_Property("username")) \
    .option("password", cc.get_Property("password")) \
    .mode("append") \
    .save()

print("DateDim table loaded successfully!")
