import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, mean, sum as _sum, stddev, when, min as _min, max as _max
from pyspark.sql.types import DateType

try:
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("WeatherDataTransformation") \
        .config("spark.jars.packages","org.postgresql:postgresql:42.2.5") \
        .getOrCreate()
except Exception as e:
    logging.error(f"Error initializing Spark session: {e}")
    exit(1)

# PostgreSQL Connection
postgres_url = "jdbc:postgresql://postgres_app/app_db"
properties = {
    "user": "app_user",
    "password": "app_password",
    "driver": "org.postgresql.Driver"
}
try:
# Load Data
    hourly_weather = spark.read.jdbc(postgres_url, "hourlyData", properties=properties)
    daily_weather = spark.read.jdbc(postgres_url, "dailyData", properties=properties)
    # crop_data = spark.read.jdbc(postgres_url, "crop_data", properties=properties)
except Exception as e:
    logging.error(f"Error loading data from PostgreSQL: {e}")
    spark.stop()
    exit(1)

# hourly to daily transformation
hourly_weather = hourly_weather.withColumn("date", col("timestamp").cast(DateType()))

daily_agg = hourly_weather.groupBy("date").agg(
    mean("temperature_2m").alias("temperature_2m"),
    _max("temperature_2m").alias("max_temperature"),
    _min("temperature_2m").alias("min_temperature"),
    mean("relative_humidity_2m").alias("relative_humidity_2m"),
    _sum("precipitation").alias("precipitation"),
    _sum("sunshine_duration").alias("sunshine_duration"),
    _sum("direct_radiation").alias("direct_radiation"),
    _sum("diffuse_radiation").alias("diffuse_radiation"),
    mean("soil_temperature_6cm").alias("soil_temperature_6cm"),
    mean("wind_speed_10m").alias("wind_speed_10m")
)

# Normalize Units
daily_agg = daily_agg.withColumn("direct_radiation", col("direct_radiation") * 0.0864) \
                     .withColumn("diffuse_radiation", col("diffuse_radiation") * 0.0864) \
                     .withColumn("precipitation", col("precipitation") / 10)

# Derived features
# Compute Growing Degree Days (GDD)
daily_agg = daily_agg.withColumn("GDD", ((col("max_temperature") + col("min_temperature")) / 2 - 10)) \
                     .withColumn("GDD", when(col("GDD") < 0, 0).otherwise(col("GDD")))

# Heat Stress Days (above 35°C)
daily_agg = daily_agg.withColumn("heat_stress", when(col("temperature_2m") > 35, 1).otherwise(0))

# Soil Moisture Index
soil_temp_min = daily_agg.agg(_min("soil_temperature_6cm")).collect()[0][0]
soil_temp_max = daily_agg.agg(_max("soil_temperature_6cm")).collect()[0][0]
daily_agg = daily_agg.withColumn("soil_moisture_index", (col("soil_temperature_6cm") - soil_temp_min) / (soil_temp_max - soil_temp_min))


# Drought Index
precip_min = daily_agg.agg(_min("precipitation")).collect()[0][0]
precip_max = daily_agg.agg(_max("precipitation")).collect()[0][0]
daily_agg = daily_agg.withColumn("drought_index", (col("precipitation") - precip_min) / (precip_max - precip_min))

# Total Solar Radiation
daily_agg = daily_agg.withColumn("total_solar_radiation", col("direct_radiation") + col("diffuse_radiation"))

# Radiation Efficiency
daily_agg = daily_agg.withColumn("radiation_efficiency", col("total_solar_radiation") / col("sunshine_duration"))

# Extract month and year
daily_agg = daily_agg.withColumn("month", date_format(col("date"), "M")) \
                     .withColumn("year", date_format(col("date"), "yyyy"))

# Monthly Aggregation
monthly_agg = daily_agg.groupBy("year", "month").agg(
    _sum("GDD").alias("GDD"),
    _sum("precipitation").alias("precipitation"),
    _sum("sunshine_duration").alias("sunshine_duration"),
    _sum("direct_radiation").alias("direct_radiation")
)

# Seasonal Aggregation (June–November)
seasonal_agg = daily_agg.filter((col("month") >= 6) & (col("month") <= 11)).groupBy("year").agg(
    _sum("GDD").alias("GDD"),
    _sum("precipitation").alias("precipitation"),
    _sum("sunshine_duration").alias("sunshine_duration"),
    _sum("direct_radiation").alias("direct_radiation")
)

# Compute Rainfall Variability Index
precip_mean = seasonal_agg.agg(mean("precipitation")).collect()[0][0]
precip_std = seasonal_agg.agg(stddev("precipitation")).collect()[0][0]
seasonal_agg = seasonal_agg.withColumn("rainfall_variability", precip_std / precip_mean)

# Seasonal Drought Index
seasonal_precip_min = seasonal_agg.agg(_min("precipitation")).collect()[0][0]
seasonal_precip_max = seasonal_agg.agg(_max("precipitation")).collect()[0][0]
seasonal_agg = seasonal_agg.withColumn("drought_index", (col("precipitation") - seasonal_precip_min) / (seasonal_precip_max - seasonal_precip_min))

# Total Solar Radiation and Radiation Efficiency
seasonal_agg = seasonal_agg.withColumn("total_solar_radiation", col("direct_radiation") + col("diffuse_radiation")) \
                           .withColumn("radiation_efficiency", col("total_solar_radiation") / col("sunshine_duration"))

# Write DataFrames to BigQuery (if needed)
daily_agg.write.format("bigquery") \
    .option("table", "SolarCropsAnalysis.weatherData_SolarCropsAnalysis.Daily_WeatherData") \
    .option("temporaryGcsBucket", "solar-crops-analysis-gcs-bucket") \
    .mode("overwrite") \
    .save()

monthly_agg.write.format("bigquery") \
    .option("table", "SolarCropsAnalysis.weatherData_SolarCropsAnalysis.Monthly_WeatherData") \
    .option("temporaryGcsBucket", "solar-crops-analysis-gcs-bucket") \
    .mode("overwrite") \
    .save()

seasonal_agg.write.format("bigquery") \
    .option("table", "SolarCropsAnalysis.weatherData_SolarCropsAnalysis.Seasonal_WeatherData") \
    .option("temporaryGcsBucket", "solar-crops-analysis-gcs-bucket") \
    .mode("overwrite") \
    .save()

# Stop Spark session
spark.stop()