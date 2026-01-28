# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Layer Transformation
# MAGIC 
# MAGIC ## Fleet Optimization & Predictive Maintenance
# MAGIC 
# MAGIC **Purpose:** Transform Bronze data into clean, validated Silver layer tables
# MAGIC 
# MAGIC **Transformations:**
# MAGIC 1. Data quality validation and filtering
# MAGIC 2. Deduplication (by business keys)
# MAGIC 3. Type conversions and standardization
# MAGIC 4. Null handling and outlier detection
# MAGIC 5. Derived column calculations
# MAGIC 6. Anomaly flagging
# MAGIC 7. Data enrichment
# MAGIC 
# MAGIC **Business Rules:**
# MAGIC - Remove GPS coordinates outside NYC operational area
# MAGIC - Flag telemetry readings beyond normal ranges
# MAGIC - Calculate distance and speed from GPS deltas
# MAGIC - Detect vehicle anomalies (overheating, low pressure, etc.)
# MAGIC - Compute health scores
# MAGIC 
# MAGIC **Author:** Venkat M  
# MAGIC **Date:** 2026-01-25

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration & Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from math import radians, cos, sin, asin, sqrt

# Configuration (Community Edition - No Unity Catalog)
# Tables are in default Hive metastore as: bronze_schema.table_name
BRONZE_SCHEMA = "bronze_schema"
SILVER_SCHEMA = "silver_schema"

# NYC Operational Area (Bounding Box)
NYC_LAT_MIN, NYC_LAT_MAX = 40.5, 40.9
NYC_LON_MIN, NYC_LON_MAX = -74.3, -73.7

# Telemetry normal ranges
ENGINE_TEMP_MIN, ENGINE_TEMP_MAX = 0, 150
OIL_PRESSURE_MIN, OIL_PRESSURE_MAX = 0, 100
TIRE_PRESSURE_MIN, TIRE_PRESSURE_MAX = 20, 50
BATTERY_VOLTAGE_MIN, BATTERY_VOLTAGE_MAX = 10, 15

# Widget for parameterization
dbutils.widgets.text("processing_date", "2024-01-01", "Processing Date (YYYY-MM-DD)")
processing_date = dbutils.widgets.get("processing_date")

print("=" * 60)
print("Silver Layer Transformation")
print("=" * 60)
print(f"Bronze Schema: {BRONZE_SCHEMA}")
print(f"Silver Schema: {SILVER_SCHEMA}")
print(f"Processing Date: {processing_date}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helper Functions

# COMMAND ----------

# Haversine formula for distance calculation
def calculate_distance_udf():
    """Calculate distance between two GPS coordinates in km"""
    def haversine(lat1, lon1, lat2, lon2):
        if any(v is None for v in [lat1, lon1, lat2, lon2]):
            return None
        
        # Convert to radians
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        km = 6371 * c  # Earth radius in km
        return km
    
    return udf(haversine, DoubleType())

# Register UDF
calculate_distance = calculate_distance_udf()

print("✓ Helper functions registered")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transform GPS Tracking Data

# COMMAND ----------

print("Transforming GPS tracking data...")

# Read from Bronze
bronze_gps_df = spark.table(f"{BRONZE_SCHEMA}.gps_tracking_raw")

# Step 1: Filter valid GPS coordinates (within NYC operational area)
gps_filtered = bronze_gps_df.filter(
    (col("latitude").between(NYC_LAT_MIN, NYC_LAT_MAX)) &
    (col("longitude").between(NYC_LON_MIN, NYC_LON_MAX)) &
    (col("latitude").isNotNull()) &
    (col("longitude").isNotNull()) &
    (col("timestamp").isNotNull()) &
    (col("vehicle_id").isNotNull())
)

# Step 2: Deduplicate by vehicle_id + timestamp
window_dedup = Window.partitionBy("vehicle_id", "timestamp").orderBy(col("_ingestion_time").desc())
gps_deduped = gps_filtered.withColumn("row_num", row_number().over(window_dedup)) \
                          .filter(col("row_num") == 1) \
                          .drop("row_num")

# Step 3: Calculate derived metrics
# Window for previous GPS point
window_prev = Window.partitionBy("vehicle_id").orderBy("timestamp")

gps_enriched = gps_deduped \
    .withColumn("prev_latitude", lag("latitude", 1).over(window_prev)) \
    .withColumn("prev_longitude", lag("longitude", 1).over(window_prev)) \
    .withColumn("prev_timestamp", lag("timestamp", 1).over(window_prev))

# Calculate distance from previous point
gps_enriched = gps_enriched.withColumn(
    "distance_from_prev_km",
    calculate_distance(
        col("prev_latitude"),
        col("prev_longitude"),
        col("latitude"),
        col("longitude")
    )
)

# Calculate time difference in seconds
gps_enriched = gps_enriched.withColumn(
    "time_from_prev_sec",
    when(col("prev_timestamp").isNotNull(),
         (unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_timestamp")))
    ).otherwise(None)
)

# Calculate speed from GPS deltas (km/h)
gps_enriched = gps_enriched.withColumn(
    "calculated_speed_kmh",
    when(
        (col("distance_from_prev_km").isNotNull()) & (col("time_from_prev_sec") > 0),
        (col("distance_from_prev_km") / col("time_from_prev_sec")) * 3600
    ).otherwise(None)
)

# Step 4: Flag anomalies
gps_enriched = gps_enriched.withColumn(
    "speed_anomaly_flag",
    when(
        (col("speed") > 120) |  # Speed > 120 km/h
        ((col("calculated_speed_kmh").isNotNull()) & (col("calculated_speed_kmh") > 150)),
        True
    ).otherwise(False)
)

# Step 5: Add location region (simplified - just quadrants)
gps_enriched = gps_enriched.withColumn(
    "location_region",
    when((col("latitude") >= 40.7) & (col("longitude") >= -74.0), "NYC-MANHATTAN")
    .when((col("latitude") < 40.7) & (col("longitude") >= -74.0), "NYC-BROOKLYN")
    .when((col("latitude") >= 40.7) & (col("longitude") < -74.0), "NYC-QUEENS")
    .otherwise("NYC-BRONX")
)

# Step 6: Add validation flag
gps_enriched = gps_enriched.withColumn(
    "is_valid_location",
    (col("latitude").between(NYC_LAT_MIN, NYC_LAT_MAX)) &
    (col("longitude").between(NYC_LON_MIN, NYC_LON_MAX))
)

# Step 7: Calculate data quality score (0-1)
gps_enriched = gps_enriched.withColumn(
    "_data_quality_score",
    (
        when(col("latitude").isNotNull(), 0.2).otherwise(0) +
        when(col("longitude").isNotNull(), 0.2).otherwise(0) +
        when(col("speed").isNotNull(), 0.2).otherwise(0) +
        when(col("gps_accuracy") <= 10, 0.2).otherwise(0) +
        when(col("speed_anomaly_flag") == False, 0.2).otherwise(0)
    )
)

# Step 8: Rename columns for Silver schema
gps_silver = gps_enriched.select(
    col("vehicle_id"),
    col("timestamp").alias("event_timestamp"),
    col("latitude"),
    col("longitude"),
    col("speed").alias("speed_kmh"),
    col("heading").alias("heading_degrees"),
    col("altitude").alias("altitude_m"),
    col("gps_accuracy").alias("gps_accuracy_m"),
    col("is_valid_location"),
    col("location_region"),
    col("distance_from_prev_km"),
    col("time_from_prev_sec"),
    col("calculated_speed_kmh"),
    col("speed_anomaly_flag"),
    current_timestamp().alias("_processed_time"),
    col("_data_quality_score")
).withColumn("event_date", to_date(col("event_timestamp")))

# Show sample
print(f"Records after transformation: {gps_silver.count():,}")
display(gps_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transform Vehicle Telemetry Data

# COMMAND ----------

print("Transforming vehicle telemetry data...")

# Read from Bronze
bronze_telemetry_df = spark.table(f"{BRONZE_SCHEMA}.vehicle_telemetry_raw")

# Step 1: Filter valid records
telemetry_filtered = bronze_telemetry_df.filter(
    (col("vehicle_id").isNotNull()) &
    (col("timestamp").isNotNull())
)

# Step 2: Deduplicate
window_dedup = Window.partitionBy("vehicle_id", "timestamp").orderBy(col("_ingestion_time").desc())
telemetry_deduped = telemetry_filtered.withColumn("row_num", row_number().over(window_dedup)) \
                                      .filter(col("row_num") == 1) \
                                      .drop("row_num")

# Step 3: Calculate derived metrics

# Average tire pressure
telemetry_enriched = telemetry_deduped.withColumn(
    "avg_tire_pressure_psi",
    (col("tire_pressure_fl") + col("tire_pressure_fr") + 
     col("tire_pressure_rl") + col("tire_pressure_rr")) / 4
)

# Tire pressure variance
telemetry_enriched = telemetry_enriched.withColumn(
    "tire_pressure_variance",
    (
        pow(col("tire_pressure_fl") - col("avg_tire_pressure_psi"), 2) +
        pow(col("tire_pressure_fr") - col("avg_tire_pressure_psi"), 2) +
        pow(col("tire_pressure_rl") - col("avg_tire_pressure_psi"), 2) +
        pow(col("tire_pressure_rr") - col("avg_tire_pressure_psi"), 2)
    ) / 4
)

# Detect idling (RPM < 1000 and low throttle)
telemetry_enriched = telemetry_enriched.withColumn(
    "is_idling",
    (col("rpm") < 1000) & (col("throttle_position") < 10)
)

# Step 4: Flag anomalies

# Engine temperature anomaly
telemetry_enriched = telemetry_enriched.withColumn(
    "engine_temp_anomaly",
    (col("engine_temp") < ENGINE_TEMP_MIN) | 
    (col("engine_temp") > ENGINE_TEMP_MAX) |
    (col("engine_temp") > 110)  # Overheating threshold
)

# Oil pressure anomaly
telemetry_enriched = telemetry_enriched.withColumn(
    "oil_pressure_anomaly",
    (col("oil_pressure") < OIL_PRESSURE_MIN) | 
    (col("oil_pressure") > OIL_PRESSURE_MAX) |
    (col("oil_pressure") < 30)  # Low pressure threshold
)

# Tire pressure anomaly
telemetry_enriched = telemetry_enriched.withColumn(
    "tire_pressure_anomaly",
    (col("avg_tire_pressure_psi") < TIRE_PRESSURE_MIN) | 
    (col("avg_tire_pressure_psi") > TIRE_PRESSURE_MAX) |
    (col("tire_pressure_variance") > 5)  # High variance
)

# Battery health flag
telemetry_enriched = telemetry_enriched.withColumn(
    "battery_health_flag",
    (col("battery_voltage").between(BATTERY_VOLTAGE_MIN, BATTERY_VOLTAGE_MAX)) &
    (col("battery_voltage") >= 12.0)  # Healthy threshold
)

# Step 5: Calculate overall health score (0-1)
telemetry_enriched = telemetry_enriched.withColumn(
    "overall_health_score",
    (
        when(col("engine_temp_anomaly") == False, 0.25).otherwise(0) +
        when(col("oil_pressure_anomaly") == False, 0.25).otherwise(0) +
        when(col("tire_pressure_anomaly") == False, 0.25).otherwise(0) +
        when(col("battery_health_flag") == True, 0.25).otherwise(0)
    )
)

# Step 6: Calculate data quality score
telemetry_enriched = telemetry_enriched.withColumn(
    "_data_quality_score",
    (
        when(col("engine_temp").isNotNull(), 0.2).otherwise(0) +
        when(col("oil_pressure").isNotNull(), 0.2).otherwise(0) +
        when(col("avg_tire_pressure_psi").isNotNull(), 0.2).otherwise(0) +
        when(col("battery_voltage").isNotNull(), 0.2).otherwise(0) +
        when(col("odometer").isNotNull(), 0.2).otherwise(0)
    )
)

# Step 7: Select and rename columns for Silver schema
telemetry_silver = telemetry_enriched.select(
    col("vehicle_id"),
    col("timestamp").alias("event_timestamp"),
    col("engine_temp").alias("engine_temp_c"),
    col("engine_temp_anomaly"),
    col("oil_pressure").alias("oil_pressure_psi"),
    col("oil_pressure_anomaly"),
    col("avg_tire_pressure_psi"),
    col("tire_pressure_variance"),
    col("tire_pressure_anomaly"),
    col("fuel_level").alias("fuel_level_pct"),
    col("battery_voltage").alias("battery_voltage_v"),
    col("battery_health_flag"),
    col("odometer").alias("odometer_km"),
    col("rpm"),
    col("throttle_position").alias("throttle_pct"),
    col("brake_status").alias("is_braking"),
    col("is_idling"),
    col("overall_health_score"),
    current_timestamp().alias("_processed_time"),
    col("_data_quality_score")
).withColumn("event_date", to_date(col("event_timestamp")))

# Show sample
print(f"Records after transformation: {telemetry_silver.count():,}")
display(telemetry_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Silver Delta Tables

# COMMAND ----------

# Write GPS Silver table
silver_gps_table = f"{SILVER_SCHEMA}.gps_tracking_clean"

print(f"Writing to {silver_gps_table}...")

gps_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date", "vehicle_id") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_gps_table)

print(f"✓ GPS data written to: {silver_gps_table}")

# COMMAND ----------

# Write Telemetry Silver table
silver_telemetry_table = f"{SILVER_SCHEMA}.vehicle_telemetry_clean"

print(f"Writing to {silver_telemetry_table}...")

telemetry_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date", "vehicle_id") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_telemetry_table)

print(f"✓ Telemetry data written to: {silver_telemetry_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Optimize Silver Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize GPS table with Z-ordering (only on non-partition columns)
# MAGIC OPTIMIZE silver_schema.gps_tracking_clean
# MAGIC ZORDER BY (event_timestamp);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize Telemetry table with Z-ordering (only on non-partition columns)
# MAGIC OPTIMIZE silver_schema.vehicle_telemetry_clean
# MAGIC ZORDER BY (event_timestamp);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Quality Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GPS Silver data quality metrics
# MAGIC SELECT 
# MAGIC     'GPS Tracking Clean' AS table_name,
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(DISTINCT vehicle_id) AS unique_vehicles,
# MAGIC     ROUND(AVG(_data_quality_score), 3) AS avg_quality_score,
# MAGIC     SUM(CASE WHEN is_valid_location = true THEN 1 ELSE 0 END) AS valid_locations,
# MAGIC     SUM(CASE WHEN speed_anomaly_flag = true THEN 1 ELSE 0 END) AS speed_anomalies,
# MAGIC     MIN(event_timestamp) AS earliest_event,
# MAGIC     MAX(event_timestamp) AS latest_event
# MAGIC FROM silver_schema.gps_tracking_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Telemetry Silver data quality metrics
# MAGIC SELECT 
# MAGIC     'Vehicle Telemetry Clean' AS table_name,
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(DISTINCT vehicle_id) AS unique_vehicles,
# MAGIC     ROUND(AVG(_data_quality_score), 3) AS avg_quality_score,
# MAGIC     ROUND(AVG(overall_health_score), 3) AS avg_health_score,
# MAGIC     SUM(CASE WHEN engine_temp_anomaly = true THEN 1 ELSE 0 END) AS engine_temp_anomalies,
# MAGIC     SUM(CASE WHEN oil_pressure_anomaly = true THEN 1 ELSE 0 END) AS oil_pressure_anomalies,
# MAGIC     SUM(CASE WHEN tire_pressure_anomaly = true THEN 1 ELSE 0 END) AS tire_pressure_anomalies,
# MAGIC     SUM(CASE WHEN battery_health_flag = false THEN 1 ELSE 0 END) AS battery_issues
# MAGIC FROM silver_schema.vehicle_telemetry_clean;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Anomaly Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vehicles with most anomalies
# MAGIC SELECT 
# MAGIC     vehicle_id,
# MAGIC     COUNT(*) AS total_readings,
# MAGIC     SUM(CASE WHEN engine_temp_anomaly = true THEN 1 ELSE 0 END) AS engine_temp_issues,
# MAGIC     SUM(CASE WHEN oil_pressure_anomaly = true THEN 1 ELSE 0 END) AS oil_pressure_issues,
# MAGIC     SUM(CASE WHEN tire_pressure_anomaly = true THEN 1 ELSE 0 END) AS tire_pressure_issues,
# MAGIC     ROUND(AVG(overall_health_score), 3) AS avg_health_score
# MAGIC FROM silver_schema.vehicle_telemetry_clean
# MAGIC GROUP BY vehicle_id
# MAGIC HAVING (engine_temp_issues + oil_pressure_issues + tire_pressure_issues) > 0
# MAGIC ORDER BY (engine_temp_issues + oil_pressure_issues + tire_pressure_issues) DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary

# COMMAND ----------

print("=" * 60)
print("✓ Silver Layer Transformation Complete!")
print("=" * 60)

# Get final counts
gps_count = spark.table(silver_gps_table).count()
telemetry_count = spark.table(silver_telemetry_table).count()

print(f"\n✓ Tables Created:")
print(f"  - {silver_gps_table}: {gps_count:,} records")
print(f"  - {silver_telemetry_table}: {telemetry_count:,} records")

print(f"\n✓ Transformations Applied:")
print(f"  - Data quality validation")
print(f"  - Deduplication by business keys")
print(f"  - Derived metrics calculation")
print(f"  - Anomaly detection and flagging")
print(f"  - Health score computation")

print(f"\n✓ Optimizations:")
print(f"  - Z-ordering on (vehicle_id, event_timestamp)")
print(f"  - Partitioning by (event_date, vehicle_id)")

print(f"\n📌 Next Steps:")
print(f"  1. Run notebook 03_Gold_Aggregation.py")
print(f"  2. Review anomaly reports")
print(f"  3. Investigate vehicles with low health scores")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Author:** Venkat M  
# MAGIC **Project:** Fleet Optimization & Predictive Maintenance  
# MAGIC **Last Updated:** 2026-01-25
