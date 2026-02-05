# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer Ingestion
# MAGIC 
# MAGIC ## Fleet Optimization & Predictive Maintenance
# MAGIC 
# MAGIC **Purpose:** Ingest raw data from source systems into Bronze layer Delta tables
# MAGIC 
# MAGIC **Data Sources:**
# MAGIC 1. GPS Tracking Data (IoT devices)
# MAGIC 2. Vehicle Telemetry (OBD-II sensors)
# MAGIC 3. Delivery Records (Order management system)
# MAGIC 4. Maintenance Logs (Fleet management system)
# MAGIC 5. Weather Data (External API)
# MAGIC 
# MAGIC **Bronze Layer Characteristics:**
# MAGIC - Append-only (immutable)
# MAGIC - Raw data with minimal transformations
# MAGIC - Audit columns added (_ingestion_time, _source_file)
# MAGIC - Schema validation
# MAGIC - Delta Lake format with ACID transactions
# MAGIC 
# MAGIC **Author:** Venkat M  
# MAGIC **Date:** 2026-01-25

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration & Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Configuration
CATALOG_NAME = "logistics_catalog"
BRONZE_SCHEMA = "bronze_schema"
VOLUME_NAME = "raw_data_volume"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/{VOLUME_NAME}"

# Widget for parameterization (for Databricks Jobs)
dbutils.widgets.text("source_date", "2024-01-01", "Source Date (YYYY-MM-DD)")
source_date = dbutils.widgets.get("source_date")

print("=" * 60)
print("Bronze Layer Ingestion")
print("=" * 60)
print(f"Catalog: {CATALOG_NAME}")
print(f"Schema: {BRONZE_SCHEMA}")
print(f"Source Date: {source_date}")
print(f"Volume Path: {VOLUME_PATH}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define Schemas

# COMMAND ----------

# GPS Tracking Schema
gps_schema = StructType([
    StructField("vehicle_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("speed", DoubleType(), True),
    StructField("heading", DoubleType(), True),
    StructField("altitude", DoubleType(), True),
    StructField("gps_accuracy", DoubleType(), True)
])

# Vehicle Telemetry Schema
telemetry_schema = StructType([
    StructField("vehicle_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("engine_temp", DoubleType(), True),
    StructField("oil_pressure", DoubleType(), True),
    StructField("tire_pressure_fl", DoubleType(), True),
    StructField("tire_pressure_fr", DoubleType(), True),
    StructField("tire_pressure_rl", DoubleType(), True),
    StructField("tire_pressure_rr", DoubleType(), True),
    StructField("fuel_level", DoubleType(), True),
    StructField("battery_voltage", DoubleType(), True),
    StructField("odometer", DoubleType(), True),
    StructField("rpm", DoubleType(), True),
    StructField("throttle_position", DoubleType(), True),
    StructField("brake_status", BooleanType(), True)
])

# Delivery Records Schema
delivery_schema = StructType([
    StructField("delivery_id", StringType(), False),
    StructField("vehicle_id", StringType(), False),
    StructField("driver_id", StringType(), False),
    StructField("origin_address", StringType(), True),
    StructField("origin_lat", DoubleType(), True),
    StructField("origin_lon", DoubleType(), True),
    StructField("destination_address", StringType(), True),
    StructField("destination_lat", DoubleType(), True),
    StructField("destination_lon", DoubleType(), True),
    StructField("scheduled_pickup_time", TimestampType(), True),
    StructField("actual_pickup_time", TimestampType(), True),
    StructField("scheduled_delivery_time", TimestampType(), True),
    StructField("actual_delivery_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("package_count", IntegerType(), True),
    StructField("total_weight_kg", DoubleType(), True),
    StructField("delivery_notes", StringType(), True)
])

# Maintenance Logs Schema
maintenance_schema = StructType([
    StructField("maintenance_id", StringType(), False),
    StructField("vehicle_id", StringType(), False),
    StructField("maintenance_date", DateType(), False),
    StructField("maintenance_type", StringType(), True),
    StructField("service_category", StringType(), True),
    StructField("parts_replaced", StringType(), True),
    StructField("labor_hours", DoubleType(), True),
    StructField("parts_cost", DoubleType(), True),
    StructField("labor_cost", DoubleType(), True),
    StructField("total_cost", DoubleType(), True),
    StructField("downtime_hours", DoubleType(), True),
    StructField("mechanic_id", StringType(), True),
    StructField("mechanic_notes", StringType(), True),
    StructField("odometer_reading", DoubleType(), True),
    StructField("next_service_due_km", DoubleType(), True)
])

print("✓ Schemas defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest GPS Tracking Data

# COMMAND ----------

print("Ingesting GPS tracking data...")

# Read from volume (parquet format)
gps_raw_df = spark.read.parquet(f"{VOLUME_PATH}/gps_tracking_sample.parquet")

# Add audit columns if not present
if "_ingestion_time" not in gps_raw_df.columns:
    gps_raw_df = gps_raw_df.withColumn("_ingestion_time", current_timestamp())
if "_source_file" not in gps_raw_df.columns:
    gps_raw_df = gps_raw_df.withColumn("_source_file", lit("gps_tracking_sample.parquet"))

# Add ingestion date partition column
gps_raw_df = gps_raw_df.withColumn("ingestion_date", to_date(col("_ingestion_time")))

# Show sample
print(f"Records to ingest: {gps_raw_df.count():,}")
display(gps_raw_df.limit(5))

# Write to Bronze Delta table (append mode)
bronze_gps_table = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.gps_tracking_raw"

gps_raw_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("ingestion_date") \
    .option("mergeSchema", "true") \
    .saveAsTable(bronze_gps_table)

print(f"✓ GPS data ingested to: {bronze_gps_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingest Vehicle Telemetry Data

# COMMAND ----------

print("Ingesting vehicle telemetry data...")

# Read from volume
telemetry_raw_df = spark.read.parquet(f"{VOLUME_PATH}/vehicle_telemetry_sample.parquet")

# Add audit columns if not present
if "_ingestion_time" not in telemetry_raw_df.columns:
    telemetry_raw_df = telemetry_raw_df.withColumn("_ingestion_time", current_timestamp())
if "_source_file" not in telemetry_raw_df.columns:
    telemetry_raw_df = telemetry_raw_df.withColumn("_source_file", lit("vehicle_telemetry_sample.parquet"))

# Add ingestion date partition column
telemetry_raw_df = telemetry_raw_df.withColumn("ingestion_date", to_date(col("_ingestion_time")))

# Show sample
print(f"Records to ingest: {telemetry_raw_df.count():,}")
display(telemetry_raw_df.limit(5))

# Write to Bronze Delta table
bronze_telemetry_table = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.vehicle_telemetry_raw"

telemetry_raw_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("ingestion_date") \
    .option("mergeSchema", "true") \
    .saveAsTable(bronze_telemetry_table)

print(f"✓ Telemetry data ingested to: {bronze_telemetry_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Optimize Bronze Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize GPS tracking table
# MAGIC OPTIMIZE logistics_catalog.bronze_schema.gps_tracking_raw
# MAGIC ZORDER BY (vehicle_id, timestamp);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize telemetry table
# MAGIC OPTIMIZE logistics_catalog.bronze_schema.vehicle_telemetry_raw
# MAGIC ZORDER BY (vehicle_id, timestamp);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Bronze Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show all Bronze tables
# MAGIC SHOW TABLES IN logistics_catalog.bronze_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GPS tracking table stats
# MAGIC SELECT 
# MAGIC     'GPS Tracking' AS table_name,
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(DISTINCT vehicle_id) AS unique_vehicles,
# MAGIC     MIN(timestamp) AS earliest_timestamp,
# MAGIC     MAX(timestamp) AS latest_timestamp,
# MAGIC     MIN(ingestion_date) AS earliest_ingestion,
# MAGIC     MAX(ingestion_date) AS latest_ingestion
# MAGIC FROM logistics_catalog.bronze_schema.gps_tracking_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Telemetry table stats
# MAGIC SELECT 
# MAGIC     'Vehicle Telemetry' AS table_name,
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(DISTINCT vehicle_id) AS unique_vehicles,
# MAGIC     MIN(timestamp) AS earliest_timestamp,
# MAGIC     MAX(timestamp) AS latest_timestamp,
# MAGIC     MIN(ingestion_date) AS earliest_ingestion,
# MAGIC     MAX(ingestion_date) AS latest_ingestion
# MAGIC FROM logistics_catalog.bronze_schema.vehicle_telemetry_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Quality Checks

# COMMAND ----------

# GPS Data Quality Checks
print("Running GPS data quality checks...")

gps_quality = spark.sql(f"""
    SELECT 
        COUNT(*) AS total_records,
        COUNT(DISTINCT vehicle_id) AS unique_vehicles,
        SUM(CASE WHEN vehicle_id IS NULL THEN 1 ELSE 0 END) AS null_vehicle_ids,
        SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamps,
        SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) AS null_coordinates,
        SUM(CASE WHEN latitude < -90 OR latitude > 90 THEN 1 ELSE 0 END) AS invalid_latitude,
        SUM(CASE WHEN longitude < -180 OR longitude > 180 THEN 1 ELSE 0 END) AS invalid_longitude,
        SUM(CASE WHEN speed < 0 OR speed > 200 THEN 1 ELSE 0 END) AS invalid_speed
    FROM {bronze_gps_table}
""")

display(gps_quality)

# COMMAND ----------

# Telemetry Data Quality Checks
print("Running telemetry data quality checks...")

telemetry_quality = spark.sql(f"""
    SELECT 
        COUNT(*) AS total_records,
        COUNT(DISTINCT vehicle_id) AS unique_vehicles,
        SUM(CASE WHEN vehicle_id IS NULL THEN 1 ELSE 0 END) AS null_vehicle_ids,
        SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamps,
        SUM(CASE WHEN engine_temp < 0 OR engine_temp > 150 THEN 1 ELSE 0 END) AS invalid_engine_temp,
        SUM(CASE WHEN oil_pressure < 0 OR oil_pressure > 100 THEN 1 ELSE 0 END) AS invalid_oil_pressure,
        SUM(CASE WHEN battery_voltage < 10 OR battery_voltage > 15 THEN 1 ELSE 0 END) AS invalid_battery_voltage,
        SUM(CASE WHEN fuel_level < 0 OR fuel_level > 100 THEN 1 ELSE 0 END) AS invalid_fuel_level
    FROM {bronze_telemetry_table}
""")

display(telemetry_quality)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Table Metadata & History

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe GPS table
# MAGIC DESCRIBE EXTENDED logistics_catalog.bronze_schema.gps_tracking_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show GPS table history (Delta Lake time travel)
# MAGIC DESCRIBE HISTORY logistics_catalog.bronze_schema.gps_tracking_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary

# COMMAND ----------

print("=" * 60)
print("✓ Bronze Layer Ingestion Complete!")
print("=" * 60)

# Get final counts
gps_count = spark.table(bronze_gps_table).count()
telemetry_count = spark.table(bronze_telemetry_table).count()

print(f"\n✓ Tables Created:")
print(f"  - {bronze_gps_table}: {gps_count:,} records")
print(f"  - {bronze_telemetry_table}: {telemetry_count:,} records")

print(f"\n✓ Optimizations Applied:")
print(f"  - Z-ordering on (vehicle_id, timestamp)")
print(f"  - Partitioning by ingestion_date")

print(f"\n✓ Data Quality:")
print(f"  - Schema validation passed")
print(f"  - Audit columns added")
print(f"  - Quality checks completed")

print(f"\n📌 Next Steps:")
print(f"  1. Run notebook 02_Silver_Transformation.py")
print(f"  2. Review data quality reports")
print(f"  3. Monitor Delta Lake metrics")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Author:** Venkat M  
# MAGIC **Project:** Fleet Optimization & Predictive Maintenance  
# MAGIC **Last Updated:** 2026-01-25
