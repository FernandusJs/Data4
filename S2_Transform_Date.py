from pyspark.sql.functions import col, date_format, when

from S1_Extract_Date import spark

# Transform extracted dates
df_transformed = spark.sql("SELECT ride_date FROM extracted_dates") \
    .withColumn("year", date_format(col("ride_date"), "yyyy").cast("int")) \
    .withColumn("quarter", date_format(col("ride_date"), "Q").cast("int")) \
    .withColumn("month_nr", date_format(col("ride_date"), "MM").cast("int")) \
    .withColumn("month_name", date_format(col("ride_date"), "MMMM")) \
    .withColumn("day_nr", date_format(col("ride_date"), "dd").cast("int")) \
    .withColumn("day_name", date_format(col("ride_date"), "EEEE")) \
    .withColumn("is_weekday", when(col("day_name").isin(["Saturday", "Sunday"]), False).otherwise(True))

df_transformed.createOrReplaceTempView("transformed_dates")

print("Date data transformed successfully!")
