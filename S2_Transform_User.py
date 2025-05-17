# S2_Transform_User.py

from S1_Extract_User import spark

# Combine users and subscriptions to get address per validfrom date
df_transformed = spark.sql("""
    SELECT
        u.userid AS user_id,
        u.street,
        u.number,
        u.zipcode,
        u.city,
        u.country_code,
        s.validfrom AS start_date
    FROM extracted_users u
    JOIN extracted_subscriptions s
        ON u.userid = s.userid
""")

from pyspark.sql.window import Window
from pyspark.sql.functions import lead

window_spec = Window.partitionBy("user_id").orderBy("start_date")

df_scd2 = df_transformed.withColumn(
    "end_date",
    lead("start_date").over(window_spec)
)

df_scd2.createOrReplaceTempView("transformed_users")

print("✅ User data transformed successfully!")
