

from S1_Extract_User import spark
from pyspark.sql.functions import expr
from pyspark.sql.functions import lead, monotonically_increasing_id

# Join subscriptions with subscription_types to get the type duration
df_transformed = spark.sql("""
    SELECT
        u.userid AS user_id,
        u.street,
        u.number,
        u.zipcode,
        u.city,
        u.country_code,
        s.validfrom AS start_date,
        st.description AS subscription_type
    FROM extracted_users u
    JOIN extracted_subscriptions s ON u.userid = s.userid
    JOIN subscription_types st ON s.subscriptiontypeid = st.subscriptiontypeid
""")

# Compute end_date based on subscription type
df_scd2 = df_transformed.withColumn(
    "end_date",
    expr("""
        CASE
            WHEN subscription_type = 'DAG' THEN date_add(start_date, 1)
            WHEN subscription_type = 'MAAND' THEN add_months(start_date, 1)
            WHEN subscription_type = 'JAAR' THEN add_months(start_date, 12)
            ELSE date_add(start_date, 1)  -- fallback
        END
    """)
)

# Drop type (optional) and show sample
df_scd2 = df_scd2.drop("subscription_type")
df_scd2 = df_scd2.withColumn("user_sk", monotonically_increasing_id())

df_scd2 = df_scd2.select(
    "user_sk",
    "user_id",
    "street",
    "number",
    "zipcode",
    "city",
    "country_code",
    "start_date",
    "end_date"
)

df_scd2.orderBy("user_id", "start_date").show(truncate=False)


df_scd2.createOrReplaceTempView("transformed_users")
print("✅ User data transformed successfully!")