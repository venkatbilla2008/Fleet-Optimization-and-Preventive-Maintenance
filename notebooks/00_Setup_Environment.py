# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup Environment
# MAGIC 
# MAGIC ## Fleet Optimization & Predictive Maintenance
# MAGIC 
# MAGIC **Purpose:** Initialize Unity Catalog, create schemas, and set up the environment
# MAGIC 
# MAGIC **Tasks:**
# MAGIC 1. Create Unity Catalog and schemas (Bronze, Silver, Gold)
# MAGIC 2. Create Unity Catalog Volumes for data storage
# MAGIC 3. Set up access permissions
# MAGIC 4. Verify environment configuration
# MAGIC 
# MAGIC **Author:** Venkat M  
# MAGIC **Date:** 2026-01-25

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# Configuration parameters
CATALOG_NAME = "logistics_catalog"
BRONZE_SCHEMA = "bronze_schema"
SILVER_SCHEMA = "silver_schema"
GOLD_SCHEMA = "gold_schema"
VOLUME_NAME = "raw_data_volume"

# Display configuration
print("=" * 60)
print("Fleet Optimization - Environment Setup")
print("=" * 60)
print(f"Catalog Name: {CATALOG_NAME}")
print(f"Bronze Schema: {BRONZE_SCHEMA}")
print(f"Silver Schema: {SILVER_SCHEMA}")
print(f"Gold Schema: {GOLD_SCHEMA}")
print(f"Volume Name: {VOLUME_NAME}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog if not exists
# MAGIC CREATE CATALOG IF NOT EXISTS logistics_catalog
# MAGIC COMMENT 'Fleet Optimization & Predictive Maintenance - Main Catalog';
# MAGIC 
# MAGIC -- Use the catalog
# MAGIC USE CATALOG logistics_catalog;
# MAGIC 
# MAGIC -- Show catalog details
# MAGIC DESCRIBE CATALOG EXTENDED logistics_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Schemas (Bronze, Silver, Gold)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Bronze Schema (Raw Data)
# MAGIC CREATE SCHEMA IF NOT EXISTS logistics_catalog.bronze_schema
# MAGIC COMMENT 'Bronze Layer - Raw data from source systems (append-only)';
# MAGIC 
# MAGIC -- Create Silver Schema (Cleaned Data)
# MAGIC CREATE SCHEMA IF NOT EXISTS logistics_catalog.silver_schema
# MAGIC COMMENT 'Silver Layer - Cleaned, validated, and deduplicated data';
# MAGIC 
# MAGIC -- Create Gold Schema (Business Metrics)
# MAGIC CREATE SCHEMA IF NOT EXISTS logistics_catalog.gold_schema
# MAGIC COMMENT 'Gold Layer - Business KPIs, aggregates, and ML features';
# MAGIC 
# MAGIC -- Show all schemas
# MAGIC SHOW SCHEMAS IN logistics_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Unity Catalog Volumes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create volume for raw data storage
# MAGIC CREATE VOLUME IF NOT EXISTS logistics_catalog.bronze_schema.raw_data_volume
# MAGIC COMMENT 'Storage for raw CSV/JSON files before ingestion';
# MAGIC 
# MAGIC -- Show volumes
# MAGIC SHOW VOLUMES IN logistics_catalog.bronze_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Set Up Access Permissions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant permissions (adjust based on your organization's users/groups)
# MAGIC 
# MAGIC -- Data Engineers: Full access to all schemas
# MAGIC -- GRANT ALL PRIVILEGES ON CATALOG logistics_catalog TO `data_engineers`;
# MAGIC -- GRANT ALL PRIVILEGES ON SCHEMA logistics_catalog.bronze_schema TO `data_engineers`;
# MAGIC -- GRANT ALL PRIVILEGES ON SCHEMA logistics_catalog.silver_schema TO `data_engineers`;
# MAGIC -- GRANT ALL PRIVILEGES ON SCHEMA logistics_catalog.gold_schema TO `data_engineers`;
# MAGIC 
# MAGIC -- Data Scientists: Read access to Silver/Gold
# MAGIC -- GRANT SELECT ON SCHEMA logistics_catalog.silver_schema TO `data_scientists`;
# MAGIC -- GRANT SELECT ON SCHEMA logistics_catalog.gold_schema TO `data_scientists`;
# MAGIC -- GRANT MODIFY ON SCHEMA logistics_catalog.gold_schema TO `data_scientists`;
# MAGIC 
# MAGIC -- Analysts: Read access to Gold only
# MAGIC -- GRANT SELECT ON SCHEMA logistics_catalog.gold_schema TO `analysts`;
# MAGIC 
# MAGIC SELECT 'Permissions configured (uncomment and adjust for your environment)' AS status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Sample Data Generation Functions

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

def generate_sample_gps_data(num_vehicles=10, num_days=7):
    """Generate sample GPS tracking data"""
    
    data = []
    start_date = datetime(2026, 1, 1)
    
    # NYC Bounding Box
    NYC_LAT_MIN, NYC_LAT_MAX = 40.5, 40.9
    NYC_LON_MIN, NYC_LON_MAX = -74.3, -73.7
    
    for vehicle_num in range(1, num_vehicles + 1):
        vehicle_id = f"VEH-{vehicle_num:06d}"
        
        for day in range(num_days):
            current_date = start_date + timedelta(days=day)
            
            # Random starting location
            lat = random.uniform(NYC_LAT_MIN, NYC_LAT_MAX)
            lon = random.uniform(NYC_LON_MIN, NYC_LON_MAX)
            
            # Generate points every 5 minutes for 10 hours
            for hour in range(8, 18):
                for minute in range(0, 60, 5):
                    timestamp = current_date.replace(hour=hour, minute=minute, second=0)
                    
                    # Random walk
                    lat += random.uniform(-0.01, 0.01)
                    lon += random.uniform(-0.01, 0.01)
                    
                    # Keep in bounds (use Python's built-in min/max)
                    lat = __builtins__.max(NYC_LAT_MIN, __builtins__.min(NYC_LAT_MAX, lat))
                    lon = __builtins__.max(NYC_LON_MIN, __builtins__.min(NYC_LON_MAX, lon))
                    
                    # Create values as simple Python types (no round() in dict)
                    data.append((
                        vehicle_id,
                        timestamp,
                        float(lat),
                        float(lon),
                        float(random.uniform(0, 80)),
                        float(random.uniform(0, 360)),
                        float(random.uniform(0, 100)),
                        float(random.uniform(3, 10))
                    ))
    
    schema = StructType([
        StructField("vehicle_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("speed", DoubleType(), True),
        StructField("heading", DoubleType(), True),
        StructField("altitude", DoubleType(), True),
        StructField("gps_accuracy", DoubleType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


def generate_sample_telemetry_data(num_vehicles=10, num_days=7):
    """Generate sample vehicle telemetry data"""
    
    data = []
    start_date = datetime(2026, 1, 1)
    
    for vehicle_num in range(1, num_vehicles + 1):
        vehicle_id = f"VEH-{vehicle_num:06d}"
        
        # Vehicle-specific baselines
        base_engine_temp = random.uniform(85, 95)
        base_oil_pressure = random.uniform(40, 50)
        base_tire_pressure = random.uniform(30, 34)
        
        for day in range(num_days):
            current_date = start_date + timedelta(days=day)
            odometer = 100000 + (vehicle_num * 1000) + (day * 150)
            
            # Generate points every 2 minutes for 10 hours
            for hour in range(8, 18):
                for minute in range(0, 60, 2):
                    timestamp = current_date.replace(hour=hour, minute=minute, second=0)
                    
                    # Add variation
                    engine_temp = base_engine_temp + random.uniform(-5, 15)
                    oil_pressure = base_oil_pressure + random.uniform(-5, 5)
                    
                    # Occasional anomalies
                    if random.random() < 0.01:
                        engine_temp += random.uniform(10, 20)
                    if random.random() < 0.01:
                        oil_pressure -= random.uniform(10, 15)
                    
                    # Create as tuple for Databricks Connect compatibility
                    data.append((
                        vehicle_id,
                        timestamp,
                        float(engine_temp),
                        float(oil_pressure),
                        float(base_tire_pressure + random.uniform(-2, 2)),
                        float(base_tire_pressure + random.uniform(-2, 2)),
                        float(base_tire_pressure + random.uniform(-2, 2)),
                        float(base_tire_pressure + random.uniform(-2, 2)),
                        float(random.uniform(20, 100)),
                        float(random.uniform(12.0, 13.5)),
                        float(odometer),
                        float(random.uniform(800, 3500)),
                        float(random.uniform(0, 80)),
                        bool(random.choice([True, False]))
                    ))
                    
                    odometer += 0.1
    
    schema = StructType([
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
    
    return spark.createDataFrame(data, schema)

print("✓ Sample data generation functions created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Generate and Save Sample Data

# COMMAND ----------

# Generate sample GPS data
print("Generating sample GPS data...")
gps_df = generate_sample_gps_data(num_vehicles=10, num_days=7)

# Add audit columns
gps_df = gps_df.withColumn("_ingestion_time", current_timestamp()) \
               .withColumn("_source_file", lit("sample_data_generator"))

# Save to volume
volume_path = f"/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/{VOLUME_NAME}"
gps_df.write.mode("overwrite").parquet(f"{volume_path}/gps_tracking_sample.parquet")

print(f"✓ Generated {gps_df.count():,} GPS records")
print(f"✓ Saved to: {volume_path}/gps_tracking_sample.parquet")

# COMMAND ----------

# Generate sample telemetry data
print("Generating sample telemetry data...")
telemetry_df = generate_sample_telemetry_data(num_vehicles=10, num_days=7)

# Add audit columns
telemetry_df = telemetry_df.withColumn("_ingestion_time", current_timestamp()) \
                           .withColumn("_source_file", lit("sample_data_generator"))

# Save to volume
telemetry_df.write.mode("overwrite").parquet(f"{volume_path}/vehicle_telemetry_sample.parquet")

print(f"✓ Generated {telemetry_df.count():,} telemetry records")
print(f"✓ Saved to: {volume_path}/vehicle_telemetry_sample.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verify Environment Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify catalog and schemas
# MAGIC SELECT 'Catalog' AS object_type, catalog_name AS name, comment 
# MAGIC FROM system.information_schema.catalogs 
# MAGIC WHERE catalog_name = 'logistics_catalog'
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT 'Schema' AS object_type, schema_name AS name, comment 
# MAGIC FROM system.information_schema.schemata 
# MAGIC WHERE catalog_name = 'logistics_catalog'
# MAGIC ORDER BY object_type, name;

# COMMAND ----------

# Verify sample data files
dbutils.fs.ls(f"/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/{VOLUME_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary

# COMMAND ----------

print("=" * 60)
print("✓ Environment Setup Complete!")
print("=" * 60)
print("\n✓ Created:")
print(f"  - Catalog: {CATALOG_NAME}")
print(f"  - Schema: {BRONZE_SCHEMA} (Raw data)")
print(f"  - Schema: {SILVER_SCHEMA} (Cleaned data)")
print(f"  - Schema: {GOLD_SCHEMA} (Business metrics)")
print(f"  - Volume: {VOLUME_NAME} (Data storage)")
print("\n✓ Generated sample data:")
print("  - GPS tracking data")
print("  - Vehicle telemetry data")
print("\n📌 Next Steps:")
print("  1. Run notebook 01_Bronze_Ingestion.py")
print("  2. Run notebook 02_Silver_Transformation.py")
print("  3. Run notebook 03_Gold_Aggregation.py")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Author:** Venkat M  
# MAGIC **Project:** Fleet Optimization & Predictive Maintenance  
# MAGIC **Last Updated:** 2026-01-25
