# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("AQIPulse").getOrCreate()

bronze = spark.table("aqipulse_catalog.aqipulse.bronze_air_quality")
print(f"bronze: {bronze.count()}")
bronze.printSchema()


# COMMAND ----------

w_series = Window.partitionBy("city", "parameter").orderBy("date")

# COMMAND ----------

silver = (
    bronze
    .withColumn("date", F.to_date("date"))
    .withColumn("value", F.col("value").cast("double"))
    
    # 7-day rolling average - smooths daily sensor noise
    .withColumn("rolling_7d_avg",
                F.round(F.avg("value").over(w_series.rowsBetween(-6, 0)), 2))
    
    # 30-day rolling average - smooths weekly seasonality
    .withColumn("rolling_30d_avg",
                F.round(F.avg("value").over(w_series.rowsBetween(-29, 0)), 2))
    
    # day-over-day change
    .withColumn("prev_value", F.lag("value", 1).over(w_series))
    .withColumn("day_over_day_change",
                F.round(F.col("value") - F.col("prev_value"), 4))
    
    # US EPA-style PM2.5 breakpoints categories (applied only to pm25 rows)
    .withColumn("aqi_category",
                F.when(F.col("parameter") != "pm25", None)
                  .when(F.col("value") <= 12.0, "Good")
                  .when(F.col("value") <= 35.4, "Moderate")
                  .when(F.col("value") <= 55.4, "Unhealthy for Sensitive Groups")
                  .when(F.col("value") <= 150.4, "Unhealthy")
                  .when(F.col("value") <= 250.4, "Very Unhealthy")
                  .otherwise("Hazardous"))
    
    #Calendar columns for aggregation in dbt
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("season",
                F.when(F.col("month").isin([12, 1, 2]), "Winter")
                  .when(F.col("month").isin([3, 4, 5]), "Spring")
                  .when(F.col("month").isin([6, 7, 8]), "Summer")
                  .otherwise("Fall"))
    
    .withColumn("_loaded_at", F.current_timestamp())
    .drop("prev_value")
)

print(f"Silver Rows: {silver.count()}")
silver.filter(F.col("parameter") == "pm25")\
    .select("date", "city", "value", "rolling_30d_avg", "aqi_category")\
    .show(10)

# COMMAND ----------

silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("aqipulse_catalog.aqipulse.silver_air_quality")

print("Silver table written.")

#Quick Check - which cities have the worst average PM2.5?
silver.filter(F.col("parameter") == "pm25")\
    .groupBy("city")\
    .agg(F.avg("value").alias("avg_pm25"))\
    .orderBy(F.col("avg_pm25").desc())\
    .show(10)