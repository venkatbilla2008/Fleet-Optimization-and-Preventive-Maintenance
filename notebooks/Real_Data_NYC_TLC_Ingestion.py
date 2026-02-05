# Databricks notebook source
# MAGIC %md
# MAGIC # Real Data Ingestion - NYC TLC Trip Data (Community Edition Compatible)
# MAGIC 
# MAGIC ## Fleet Optimization & Predictive Maintenance
# MAGIC 
# MAGIC **Purpose:** Ingest real-world NYC Taxi data for Databricks Community Edition
# MAGIC 
# MAGIC **Data Source:** NYC TLC Trip Record Data
# MAGIC - Alternative: Use smaller sample or manual upload
# MAGIC - Format: Parquet
# MAGIC - Cost: Free
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
from datetime import datetime, timedelta  # Added timedelta
import random

# Configuration
BRONZE_SCHEMA = "bronze_schema"
SILVER_SCHEMA = "silver_schema"
GOLD_SCHEMA = "gold_schema"

# NYC Bounding Box (for filtering valid coordinates)
NYC_LAT_MIN, NYC_LAT_MAX = 40.5, 40.9
NYC_LON_MIN, NYC_LON_MAX = -74.3, -73.7

print("=" * 60)
print("Real Data Ingestion - NYC TLC Trip Data")
print("=" * 60)
print(f"Target Schema: {BRONZE_SCHEMA}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate Realistic Sample Data (Community Edition Workaround)
# MAGIC 
# MAGIC **Note:** Since Databricks Community Edition cannot download from HTTPS URLs,
# MAGIC we'll generate realistic sample data based on NYC taxi patterns.

# COMMAND ----------

def generate_realistic_nyc_taxi_data(num_vehicles=100, num_days=7):
    """
    Generate realistic NYC taxi trip data
    Based on actual NYC taxi patterns and statistics
    """
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    # NYC taxi zones (simplified - major areas)
    pickup_zones = [
        (40.7589, -73.9851, "Manhattan-Midtown"),
        (40.7614, -73.9776, "Manhattan-Times Square"),
        (40.7128, -74.0060, "Manhattan-Downtown"),
        (40.7580, -73.9855, "Manhattan-Central Park"),
        (40.6782, -73.9442, "Brooklyn-Downtown"),
        (40.7282, -73.7949, "Queens-JFK Area"),
        (40.7489, -73.9680, "Queens-LIC"),
        (40.8448, -73.8648, "Bronx-Yankee Stadium")
    ]
    
    for vehicle_num in range(1, num_vehicles + 1):
        vehicle_id = f"VEH-{vehicle_num:04d}"
        vendor_id = random.choice([1, 2])  # Two main vendors
        
        for day in range(num_days):
            current_date = start_date + timedelta(days=day)
            
            # Generate 10-30 trips per vehicle per day (realistic)
            num_trips = random.randint(10, 30)
            
            for trip in range(num_trips):
                # Random pickup time (6 AM to 11 PM)
                hour = random.randint(6, 23)
                minute = random.randint(0, 59)
                pickup_time = current_date.replace(hour=hour, minute=minute, second=0)
                
                # Select pickup location
                pickup_lat, pickup_lon, pickup_zone = random.choice(pickup_zones)
                pickup_lat += random.uniform(-0.02, 0.02)
                pickup_lon += random.uniform(-0.02, 0.02)
                
                # Generate dropoff location (different zone, usually)
                if random.random() < 0.7:  # 70% different zone
                    dropoff_lat, dropoff_lon, dropoff_zone = random.choice(pickup_zones)
                else:  # 30% same zone
                    dropoff_lat, dropoff_lon = pickup_lat, pickup_lon
                
                dropoff_lat += random.uniform(-0.02, 0.02)
                dropoff_lon += random.uniform(-0.02, 0.02)
                
                # Calculate trip distance (miles) - realistic range
                trip_distance = random.uniform(0.5, 15.0)
                
                # Calculate trip duration (minutes) - based on distance and traffic
                base_duration = trip_distance * random.uniform(3, 8)  # 3-8 min per mile
                trip_duration_min = __builtins__.max(5, int(base_duration))  # Use Python's built-in max
                
                dropoff_time = pickup_time + timedelta(minutes=trip_duration_min)
                
                # Calculate fare (realistic NYC taxi rates)
                base_fare = 3.00
                per_mile = 2.50
                per_minute = 0.50
                
                fare_amount = base_fare + (trip_distance * per_mile) + (trip_duration_min * per_minute)
                
                # Tips (15-20% for most, 0 for some)
                if random.random() < 0.85:  # 85% tip
                    tip_amount = fare_amount * random.uniform(0.15, 0.25)
                else:
                    tip_amount = 0.0
                
                # Tolls (occasional)
                tolls_amount = float(random.choice([0, 0, 0, 5.76, 6.50]))
                
                # Total amount
                total_amount = fare_amount + tip_amount + tolls_amount + 0.50  # +0.50 MTA tax
                
                # Passenger count (1-6)
                passenger_count = random.choices([1, 2, 3, 4, 5, 6], 
                                                 weights=[50, 30, 10, 5, 3, 2])[0]
                
                # Payment type (1=Credit, 2=Cash)
                payment_type = random.choices([1, 2], weights=[70, 30])[0]
                
                # Create record as tuple (NO round() calls - causes AssertionError)
                data.append((
                    vendor_id,
                    pickup_time,
                    dropoff_time,
                    passenger_count,
                    float(trip_distance),
                    float(pickup_lon),
                    float(pickup_lat),
                    float(dropoff_lon),
                    float(dropoff_lat),
                    payment_type,
                    float(fare_amount),
                    0.50,  # extra (MTA tax)
                    0.30,  # mta_tax
                    float(tip_amount),
                    float(tolls_amount),
                    0.0,   # improvement_surcharge
                    float(total_amount)
                ))
    
    # Define schema matching NYC TLC format
    schema = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

print("✓ Data generation function created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate NYC Taxi Data

# COMMAND ----------

print("Generating realistic NYC taxi data...")
print("This simulates real NYC TLC trip patterns...")

# Generate data (adjust num_vehicles and num_days as needed)
nyc_taxi_raw = generate_realistic_nyc_taxi_data(num_vehicles=100, num_days=7)

record_count = nyc_taxi_raw.count()
print(f"✓ Successfully generated {record_count:,} trip records")

# Show schema
print("\nSchema:")
nyc_taxi_raw.printSchema()

# Show sample
print("\nSample Data:")
display(nyc_taxi_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Statistics

# COMMAND ----------

# Basic statistics
print("Dataset Statistics:")
print(f"Total Records: {nyc_taxi_raw.count():,}")

# Date range
date_stats = nyc_taxi_raw.select(
    min("tpep_pickup_datetime").alias("earliest_trip"),
    max("tpep_pickup_datetime").alias("latest_trip")
).collect()[0]

print(f"Date Range: {date_stats['earliest_trip']} to {date_stats['latest_trip']}")

# Vendor distribution
print("\nVendor Distribution:")
display(nyc_taxi_raw.groupBy("VendorID").count().orderBy("VendorID"))

# Trip distance statistics
print("\nTrip Distance Statistics:")
display(nyc_taxi_raw.select(
    round(avg("trip_distance"), 2).alias("avg_distance_miles"),
    round(min("trip_distance"), 2).alias("min_distance"),
    round(max("trip_distance"), 2).alias("max_distance"),
    round(stddev("trip_distance"), 2).alias("stddev_distance")
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transform to GPS Tracking Format

# COMMAND ----------

print("Transforming NYC taxi data to GPS tracking format...")

# Create pickup GPS records
gps_pickup = nyc_taxi_raw.select(
    concat(
        lit("VEH-"),
        lpad(col("VendorID").cast("string"), 2, "0"),
        lpad((hash(col("tpep_pickup_datetime")) % 10000).cast("string"), 4, "0")
    ).alias("vehicle_id"),
    
    col("tpep_pickup_datetime").alias("timestamp"),
    col("pickup_latitude").alias("latitude"),
    col("pickup_longitude").alias("longitude"),
    
    # Calculate speed (miles/hour to km/hour)
    when(
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) > 0,
        (col("trip_distance") * 1.60934) / ((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600)
    ).otherwise(0).alias("speed"),
    
    lit(0.0).alias("heading"),
    lit(10.0).alias("altitude"),
    lit(5.0).alias("gps_accuracy")
)

# Create dropoff GPS records
gps_dropoff = nyc_taxi_raw.select(
    concat(
        lit("VEH-"),
        lpad(col("VendorID").cast("string"), 2, "0"),
        lpad((hash(col("tpep_pickup_datetime")) % 10000).cast("string"), 4, "0")
    ).alias("vehicle_id"),
    
    col("tpep_dropoff_datetime").alias("timestamp"),
    col("dropoff_latitude").alias("latitude"),
    col("dropoff_longitude").alias("longitude"),
    
    lit(0.0).alias("speed"),
    lit(0.0).alias("heading"),
    lit(10.0).alias("altitude"),
    lit(5.0).alias("gps_accuracy")
)

# Combine pickup and dropoff
gps_combined = gps_pickup.union(gps_dropoff)

# Filter valid coordinates
gps_filtered = gps_combined.filter(
    (col("latitude").between(NYC_LAT_MIN, NYC_LAT_MAX)) &
    (col("longitude").between(NYC_LON_MIN, NYC_LON_MAX)) &
    (col("latitude").isNotNull()) &
    (col("longitude").isNotNull()) &
    (col("timestamp").isNotNull())
)

# Calculate heading
window_spec = Window.partitionBy("vehicle_id").orderBy("timestamp")

gps_enriched = gps_filtered \
    .withColumn("prev_lat", lag("latitude", 1).over(window_spec)) \
    .withColumn("prev_lon", lag("longitude", 1).over(window_spec)) \
    .withColumn(
        "heading",
        when(
            (col("prev_lat").isNotNull()) & (col("prev_lon").isNotNull()),
            degrees(atan2(
                col("longitude") - col("prev_lon"),
                col("latitude") - col("prev_lat")
            )) % 360
        ).otherwise(0.0)
    ) \
    .drop("prev_lat", "prev_lon")

# Add audit columns
gps_final = gps_enriched \
    .withColumn("_ingestion_time", current_timestamp()) \
    .withColumn("_source_file", lit("nyc_taxi_realistic_sample")) \
    .withColumn("ingestion_date", to_date(col("_ingestion_time")))

print(f"✓ Transformation complete")
print(f"  Original records: {nyc_taxi_raw.count():,}")
print(f"  GPS records (pickup + dropoff): {gps_combined.count():,}")
print(f"  After filtering: {gps_filtered.count():,}")
print(f"  Final GPS records: {gps_final.count():,}")

display(gps_final.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Bronze Schema (if not exists)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Bronze schema
# MAGIC CREATE DATABASE IF NOT EXISTS bronze_schema
# MAGIC COMMENT 'Bronze Layer - Raw data from source systems';
# MAGIC 
# MAGIC -- Create Silver schema
# MAGIC CREATE DATABASE IF NOT EXISTS silver_schema
# MAGIC COMMENT 'Silver Layer - Cleaned and validated data';
# MAGIC 
# MAGIC -- Create Gold schema
# MAGIC CREATE DATABASE IF NOT EXISTS gold_schema
# MAGIC COMMENT 'Gold Layer - Business KPIs and ML features';
# MAGIC 
# MAGIC -- Show databases
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write to Bronze Delta Table

# COMMAND ----------

bronze_gps_table = f"{BRONZE_SCHEMA}.gps_tracking_raw"

print(f"Writing to Bronze table: {bronze_gps_table}")

# Write to Delta table
gps_final.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("ingestion_date") \
    .option("overwriteSchema", "true") \
    .saveAsTable(bronze_gps_table)

print(f"✓ Data written to {bronze_gps_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Optimize Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize with Z-ordering
# MAGIC OPTIMIZE bronze_schema.gps_tracking_raw
# MAGIC ZORDER BY (vehicle_id, timestamp);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Verify Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table statistics
# MAGIC SELECT 
# MAGIC     'GPS Tracking (Realistic Sample)' AS table_name,
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(DISTINCT vehicle_id) AS unique_vehicles,
# MAGIC     MIN(timestamp) AS earliest_timestamp,
# MAGIC     MAX(timestamp) AS latest_timestamp,
# MAGIC     ROUND(AVG(speed), 2) AS avg_speed_kmh,
# MAGIC     ROUND(MAX(speed), 2) AS max_speed_kmh,
# MAGIC     MIN(ingestion_date) AS earliest_ingestion,
# MAGIC     MAX(ingestion_date) AS latest_ingestion
# MAGIC FROM bronze_schema.gps_tracking_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Generate Telemetry Data (from GPS)

# COMMAND ----------

print("Generating telemetry data from GPS records...")

# Get unique vehicles and their GPS data
vehicles_with_gps = spark.sql(f"""
    SELECT DISTINCT vehicle_id
    FROM {bronze_gps_table}
""")

# Generate telemetry records for each GPS record
telemetry_from_gps = spark.sql(f"""
    SELECT 
        vehicle_id,
        timestamp,
        -- Generate realistic telemetry values
        85.0 + (RAND() * 20) AS engine_temp,
        45.0 + (RAND() * 10) - 5 AS oil_pressure,
        32.0 + (RAND() * 4) - 2 AS tire_pressure_fl,
        32.0 + (RAND() * 4) - 2 AS tire_pressure_fr,
        32.0 + (RAND() * 4) - 2 AS tire_pressure_rl,
        32.0 + (RAND() * 4) - 2 AS tire_pressure_rr,
        50.0 + (RAND() * 50) AS fuel_level,
        12.0 + (RAND() * 1.5) AS battery_voltage,
        100000.0 + (RAND() * 50000) AS odometer,
        1500.0 + (RAND() * 2000) AS rpm,
        20.0 + (RAND() * 60) AS throttle_position,
        CASE WHEN RAND() > 0.8 THEN true ELSE false END AS brake_status,
        current_timestamp() AS _ingestion_time,
        'generated_from_gps' AS _source_file
    FROM {bronze_gps_table}
""")

# Add ingestion_date
telemetry_with_date = telemetry_from_gps.withColumn("ingestion_date", to_date(col("_ingestion_time")))

# Write to Bronze telemetry table
bronze_telemetry_table = f"{BRONZE_SCHEMA}.vehicle_telemetry_raw"

print(f"Writing to {bronze_telemetry_table}...")

telemetry_with_date.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("ingestion_date") \
    .option("overwriteSchema", "true") \
    .saveAsTable(bronze_telemetry_table)

telemetry_count = spark.table(bronze_telemetry_table).count()
print(f"✓ Generated {telemetry_count:,} telemetry records")
print(f"✓ Saved to: {bronze_telemetry_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Summary

# COMMAND ----------

final_stats = spark.sql(f"""
    SELECT 
        COUNT(*) AS total_records,
        COUNT(DISTINCT vehicle_id) AS unique_vehicles,
        COUNT(DISTINCT DATE(timestamp)) AS unique_days
    FROM {bronze_gps_table}
""").collect()[0]

print("=" * 60)
print("✓ Real Data Ingestion Complete!")
print("=" * 60)
print(f"\n✓ Data Source: NYC Taxi Realistic Sample")
print(f"✓ Total GPS Records: {final_stats['total_records']:,}")
print(f"✓ Unique Vehicles: {final_stats['unique_vehicles']:,}")
print(f"✓ Days of Data: {final_stats['unique_days']}")
print(f"✓ Table: {bronze_gps_table}")

print(f"\n✓ Data Quality:")
print(f"  - All coordinates within NYC bounds")
print(f"  - Realistic trip patterns and fares")
print(f"  - Based on actual NYC taxi statistics")

print(f"\n✓ Optimizations Applied:")
print(f"  - Delta Lake format with ACID transactions")
print(f"  - Partitioned by ingestion_date")
print(f"  - Z-ordered on (vehicle_id, timestamp)")

print(f"\n📌 Next Steps:")
print(f"  1. Run notebook 02_Silver_Transformation.py")
print(f"  2. Continue with Gold layer aggregation")
print(f"  3. Train ML model")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Author:** Venkat M  
# MAGIC **Project:** Fleet Optimization & Predictive Maintenance  
# MAGIC **Data Source:** NYC Taxi Realistic Sample (Community Edition Compatible)  
# MAGIC **Last Updated:** 2026-01-25
