# S2_Transform_Weather.py

from S1_Extract_Weather import spark

# Transform extracted data
df_transformed = spark.sql("""
    SELECT weather_id, weather_type
    FROM extracted_weather
""")

df_transformed.createOrReplaceTempView("transformed_weather")

print("✅ Weather data transformed successfully!")