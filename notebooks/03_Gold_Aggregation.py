# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Layer Aggregation
# MAGIC 
# MAGIC ## Fleet Optimization & Predictive Maintenance
# MAGIC 
# MAGIC **Purpose:** Create business-level aggregations and ML-ready features in Gold layer
# MAGIC 
# MAGIC **Gold Tables:**
# MAGIC 1. **fleet_performance_kpis** - Daily vehicle performance metrics
# MAGIC 2. **maintenance_prediction_features** - ML features for predictive maintenance
# MAGIC 3. **vehicle_health_summary** - Current health status of all vehicles
# MAGIC 
# MAGIC **Business Metrics:**
# MAGIC - Distance traveled, fuel efficiency
# MAGIC - Average speed, idle time percentage
# MAGIC - Engine health indicators
# MAGIC - Anomaly counts and health scores
# MAGIC - Maintenance risk scores
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
from datetime import datetime, timedelta

# Configuration (Community Edition - No Unity Catalog)
SILVER_SCHEMA = "silver_schema"
GOLD_SCHEMA = "gold_schema"

# Widget for parameterization
dbutils.widgets.text("report_date", "2024-01-07", "Report Date (YYYY-MM-DD)")
report_date = dbutils.widgets.get("report_date")

print("=" * 60)
print("Gold Layer Aggregation")
print("=" * 60)
print(f"Silver Schema: {SILVER_SCHEMA}")
print(f"Gold Schema: {GOLD_SCHEMA}")
print(f"Report Date: {report_date}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fleet Performance KPIs (Daily)

# COMMAND ----------

print("Calculating fleet performance KPIs...")

# Ensure Gold schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS gold_schema")
print("✓ Gold schema ready")

# Read Silver tables (using hardcoded names for Community Edition compatibility)
print("Reading Silver tables...")
try:
    gps_df = spark.table("silver_schema.gps_tracking_clean")
    telemetry_df = spark.table("silver_schema.vehicle_telemetry_clean")
    print(f"✓ GPS table: {gps_df.count():,} records")
    print(f"✓ Telemetry table: {telemetry_df.count():,} records")
except Exception as e:
    print(f"❌ Error reading Silver tables: {e}")
    print("\nAvailable tables in silver_schema:")
    spark.sql("SHOW TABLES IN silver_schema").show()
    raise

# Filter for report date
gps_daily = gps_df.filter(col("event_date") == report_date)
telemetry_daily = telemetry_df.filter(col("event_date") == report_date)

# GPS-based metrics
gps_metrics = gps_daily.groupBy("vehicle_id") \
    .agg(
        sum("distance_from_prev_km").alias("total_distance_km"),
        avg("speed_kmh").alias("avg_speed_kmh"),
        max("speed_kmh").alias("max_speed_kmh"),
        count("*").alias("gps_readings_count"),
        sum(when(col("speed_anomaly_flag") == True, 1).otherwise(0)).alias("speed_anomalies")
    )

# Telemetry-based metrics
telemetry_metrics = telemetry_daily.groupBy("vehicle_id") \
    .agg(
        # Engine metrics
        avg("engine_temp_c").alias("avg_engine_temp_c"),
        max("engine_temp_c").alias("max_engine_temp_c"),
        stddev("engine_temp_c").alias("std_engine_temp_c"),
        
        # Oil pressure metrics
        avg("oil_pressure_psi").alias("avg_oil_pressure_psi"),
        min("oil_pressure_psi").alias("min_oil_pressure_psi"),
        
        # Tire pressure metrics
        avg("avg_tire_pressure_psi").alias("avg_tire_pressure_psi"),
        avg("tire_pressure_variance").alias("avg_tire_pressure_variance"),
        
        # Battery metrics
        avg("battery_voltage_v").alias("avg_battery_voltage_v"),
        min("battery_voltage_v").alias("min_battery_voltage_v"),
        
        # Fuel metrics
        avg("fuel_level_pct").alias("avg_fuel_level_pct"),
        min("fuel_level_pct").alias("min_fuel_level_pct"),
        
        # Odometer
        max("odometer_km").alias("max_odometer_km"),
        min("odometer_km").alias("min_odometer_km"),
        
        # Idle time
        sum(when(col("is_idling") == True, 1).otherwise(0)).alias("idle_readings_count"),
        count("*").alias("telemetry_readings_count"),
        
        # Anomalies
        sum(when(col("engine_temp_anomaly") == True, 1).otherwise(0)).alias("engine_temp_anomalies"),
        sum(when(col("oil_pressure_anomaly") == True, 1).otherwise(0)).alias("oil_pressure_anomalies"),
        sum(when(col("tire_pressure_anomaly") == True, 1).otherwise(0)).alias("tire_pressure_anomalies"),
        
        # Health score
        avg("overall_health_score").alias("avg_health_score")
    )

# Calculate derived metrics
telemetry_metrics = telemetry_metrics \
    .withColumn("daily_mileage_km", col("max_odometer_km") - col("min_odometer_km")) \
    .withColumn("idle_time_pct", (col("idle_readings_count") / col("telemetry_readings_count")) * 100)

# Join GPS and telemetry metrics
fleet_kpis = gps_metrics.join(telemetry_metrics, "vehicle_id", "outer") \
    .withColumn("report_date", lit(report_date).cast("date"))

# Calculate maintenance risk score (0-1, higher = more risk)
fleet_kpis = fleet_kpis.withColumn(
    "maintenance_risk_score",
    (
        # High engine temp increases risk
        when(col("max_engine_temp_c") > 110, 0.3).otherwise(0) +
        # Low oil pressure increases risk
        when(col("min_oil_pressure_psi") < 30, 0.3).otherwise(0) +
        # Tire pressure issues increase risk
        when(col("avg_tire_pressure_variance") > 5, 0.2).otherwise(0) +
        # Low battery voltage increases risk
        when(col("min_battery_voltage_v") < 12.0, 0.2).otherwise(0)
    )
)

# Estimate fuel consumed (simplified: distance / fuel_efficiency)
# Assume average fuel efficiency of 10 km/L
fleet_kpis = fleet_kpis.withColumn(
    "estimated_fuel_consumed_l",
    coalesce(col("total_distance_km") / 10.0, lit(0))
)

# Select final columns
fleet_kpis_final = fleet_kpis.select(
    col("report_date"),
    col("vehicle_id"),
    coalesce(col("total_distance_km"), lit(0)).alias("total_distance_km"),
    coalesce(col("daily_mileage_km"), lit(0)).alias("daily_mileage_km"),
    col("avg_speed_kmh"),
    col("max_speed_kmh"),
    col("estimated_fuel_consumed_l"),
    col("idle_time_pct"),
    col("avg_engine_temp_c"),
    col("max_engine_temp_c"),
    col("std_engine_temp_c"),
    col("avg_oil_pressure_psi"),
    col("min_oil_pressure_psi"),
    col("avg_tire_pressure_psi"),
    col("avg_tire_pressure_variance"),
    col("avg_battery_voltage_v"),
    col("min_battery_voltage_v"),
    coalesce(col("engine_temp_anomalies"), lit(0)).alias("engine_temp_anomalies"),
    coalesce(col("oil_pressure_anomalies"), lit(0)).alias("oil_pressure_anomalies"),
    coalesce(col("tire_pressure_anomalies"), lit(0)).alias("tire_pressure_anomalies"),
    coalesce(col("speed_anomalies"), lit(0)).alias("speed_anomalies"),
    col("avg_health_score"),
    col("maintenance_risk_score")
)

print(f"Fleet KPIs calculated for {fleet_kpis_final.count()} vehicles")
display(fleet_kpis_final.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Maintenance Prediction Features (ML-Ready)

# COMMAND ----------

print("Generating maintenance prediction features...")

# Calculate rolling window features (7-day)
# Adjusted for demo data (only 7 days available)

# Get telemetry data for last 7 days (instead of 30)
feature_date = report_date
start_date = (datetime.strptime(report_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

print(f"Feature date: {feature_date}")
print(f"Start date (7d back): {start_date}")

telemetry_7d = telemetry_df.filter(
    (col("event_date") >= start_date) & (col("event_date") <= feature_date)
)

# Check if we have data
telemetry_count = telemetry_7d.count()
print(f"Telemetry records in date range: {telemetry_count:,}")

if telemetry_count == 0:
    print("⚠️ No telemetry data found in the specified date range!")
    print("Available dates in telemetry:")
    telemetry_df.select(min("event_date"), max("event_date")).show()
    raise ValueError("No telemetry data available. Check your date range.")

# 7-day aggregations
features_7d = telemetry_7d.groupBy("vehicle_id") \
    .agg(
        avg("engine_temp_c").alias("avg_engine_temp_7d"),
        max("engine_temp_c").alias("max_engine_temp_7d"),
        stddev("engine_temp_c").alias("std_engine_temp_7d"),
        avg("oil_pressure_psi").alias("avg_oil_pressure_7d"),
        min("oil_pressure_psi").alias("min_oil_pressure_7d"),
        avg("tire_pressure_variance").alias("tire_pressure_variance_7d"),
        avg("battery_voltage_v").alias("avg_battery_voltage_7d"),
        sum(when(col("engine_temp_anomaly") == True, 1).otherwise(0)).alias("engine_anomalies_7d"),
        sum(when(col("oil_pressure_anomaly") == True, 1).otherwise(0)).alias("oil_anomalies_7d"),
        sum(when(col("is_idling") == True, 1).otherwise(0)).alias("idle_count_7d"),
        count("*").alias("readings_7d"),
        # Add mileage calculations here instead of separate 30d aggregation
        max("odometer_km").alias("total_mileage_km"),
        (max("odometer_km") - min("odometer_km")).alias("mileage_7d"),
        avg("overall_health_score").alias("avg_health_score_7d")
    )

# Calculate idle time percentage
features_7d = features_7d.withColumn(
    "idle_time_pct_7d",
    (col("idle_count_7d") / col("readings_7d")) * 100
)

# Calculate average daily mileage (from 7 days)
features_7d = features_7d.withColumn(
    "avg_daily_mileage_30d",  # Keep name for compatibility with ML model
    col("mileage_7d") / 7.0
)

# Use features_7d as ml_features (no need for separate 30d join)
ml_features = features_7d

# Add feature date
ml_features = ml_features.withColumn("feature_date", lit(feature_date).cast("date"))

# Add realistic vehicle characteristics with variation
ml_features = ml_features \
    .withColumn("vehicle_age_years", lit(2.0) + (rand() * 4.0)) \
    .withColumn("vehicle_model", lit("Ford Transit"))

# Calculate anomaly score (0-1)
ml_features = ml_features.withColumn(
    "anomaly_score",
    least(
        (coalesce(col("engine_anomalies_7d"), lit(0)) + 
         coalesce(col("oil_anomalies_7d"), lit(0))) / 100.0,
        lit(1.0)
    )
)

# Calculate failure risk score (composite) with more granularity
ml_features = ml_features.withColumn(
    "failure_risk_score",
    (
        # Engine temperature risk (0-0.3)
        when(col("max_engine_temp_7d") > 115, 0.30)
        .when(col("max_engine_temp_7d") > 110, 0.25)
        .when(col("max_engine_temp_7d") > 105, 0.15)
        .when(col("max_engine_temp_7d") > 100, 0.10)
        .otherwise(0) +
        
        # Oil pressure risk (0-0.3)
        when(col("min_oil_pressure_7d") < 25, 0.30)
        .when(col("min_oil_pressure_7d") < 30, 0.25)
        .when(col("min_oil_pressure_7d") < 35, 0.15)
        .when(col("min_oil_pressure_7d") < 40, 0.10)
        .otherwise(0) +
        
        # Tire pressure risk (0-0.2)
        when(col("tire_pressure_variance_7d") > 8, 0.20)
        .when(col("tire_pressure_variance_7d") > 5, 0.15)
        .when(col("tire_pressure_variance_7d") > 3, 0.10)
        .otherwise(0) +
        
        # Anomaly risk (0-0.2)
        when(col("anomaly_score") > 0.15, 0.20)
        .when(col("anomaly_score") > 0.10, 0.15)
        .when(col("anomaly_score") > 0.05, 0.10)
        .otherwise(0)
    )
)

# Calculate median risk score for threshold
from pyspark.sql.functions import expr

# Check if we have data
record_count = ml_features.count()
print(f"Total vehicles with features: {record_count}")

if record_count == 0:
    print("⚠️ No data found for the specified date range!")
    print(f"Report date: {report_date}")
    print(f"Start date (7d back): {start_date}")
    raise ValueError("No data available for feature generation. Check your date range.")

# Show failure_risk_score distribution
print("\nFailure risk score distribution:")
ml_features.groupBy("failure_risk_score").count().orderBy("failure_risk_score").show()

# Calculate median with safety check
quantiles = ml_features.approxQuantile("failure_risk_score", [0.5], 0.01)
if len(quantiles) > 0:
    median_risk = quantiles[0]
else:
    # Fallback if quantile calculation fails
    median_risk = 0.25
    print(f"⚠️ Could not calculate median, using default: {median_risk}")

print(f"Median failure risk score: {median_risk}")

# Get min and max for context
risk_stats = ml_features.select(
    min("failure_risk_score").alias("min_risk"),
    max("failure_risk_score").alias("max_risk"),
    avg("failure_risk_score").alias("avg_risk")
).collect()[0]

print(f"Risk score range: {risk_stats['min_risk']:.2f} to {risk_stats['max_risk']:.2f}")
print(f"Average risk score: {risk_stats['avg_risk']:.2f}")

# Add target variable using median threshold
# Use > instead of >= to ensure we don't get all True when median is at min value
ml_features = ml_features.withColumn(
    "will_require_maintenance_7d",
    col("failure_risk_score") > median_risk
)

# Verify class balance
print("\nTarget variable distribution:")
ml_features.groupBy("will_require_maintenance_7d").count().show()

# Check if we have both classes
class_counts = ml_features.groupBy("will_require_maintenance_7d").count().collect()
if len(class_counts) < 2:
    print("\n⚠️ WARNING: Only one class in target variable!")
    print("Adjusting threshold to create balanced classes...")
    
    # Use 60th percentile instead of median
    quantiles_60 = ml_features.approxQuantile("failure_risk_score", [0.6], 0.01)
    if len(quantiles_60) > 0:
        adjusted_threshold = quantiles_60[0]
        print(f"Using 60th percentile threshold: {adjusted_threshold}")
        
        ml_features = ml_features.withColumn(
            "will_require_maintenance_7d",
            col("failure_risk_score") > adjusted_threshold
        )
        
        print("\nAdjusted target variable distribution:")
        ml_features.groupBy("will_require_maintenance_7d").count().show()


# Add realistic varying features based on vehicle characteristics
ml_features = ml_features \
    .withColumn("days_since_last_maintenance", (lit(30) + (rand() * 60)).cast("int")) \
    .withColumn("km_since_last_maintenance", col("total_mileage_km") * (0.05 + rand() * 0.10)) \
    .withColumn("maintenance_frequency_30d", when(rand() > 0.7, 1).otherwise(0)) \
    .withColumn("cumulative_maintenance_cost_usd", col("vehicle_age_years") * (800 + rand() * 1200)) \
    .withColumn("cost_per_km_usd", lit(0.03) + (rand() * 0.03))

# Add IMPROVED engineered features for better ML performance
print("\nAdding engineered features...")
ml_features = ml_features \
    .withColumn("engine_temp_trend", 
        col("max_engine_temp_7d") - col("avg_engine_temp_7d")) \
    .withColumn("oil_pressure_drop", 
        lit(60) - col("min_oil_pressure_7d")) \
    .withColumn("battery_health_category", 
        when(col("avg_battery_voltage_7d") < 12.0, 0)
        .when(col("avg_battery_voltage_7d") < 12.5, 1)
        .otherwise(2)) \
    .withColumn("high_temp_events", 
        when(col("max_engine_temp_7d") > 110, 1).otherwise(0)) \
    .withColumn("low_oil_events", 
        when(col("min_oil_pressure_7d") < 30, 1).otherwise(0)) \
    .withColumn("total_anomaly_count", 
        coalesce(col("engine_anomalies_7d"), lit(0)) + coalesce(col("oil_anomalies_7d"), lit(0))) \
    .withColumn("mileage_per_day_avg", 
        col("total_mileage_km") / col("vehicle_age_years") / 365.0) \
    .withColumn("maintenance_cost_per_year", 
        col("cumulative_maintenance_cost_usd") / col("vehicle_age_years"))

print("✓ Added 8 engineered features")

# Select final feature columns (now with 30 features total)
ml_features_final = ml_features.select(
    col("feature_date"),
    col("vehicle_id"),
    col("vehicle_age_years"),
    col("vehicle_model"),
    col("total_mileage_km"),
    col("avg_daily_mileage_30d"),
    col("days_since_last_maintenance"),
    col("km_since_last_maintenance"),
    col("maintenance_frequency_30d"),
    col("avg_engine_temp_7d"),
    col("max_engine_temp_7d"),
    col("std_engine_temp_7d"),
    col("avg_oil_pressure_7d"),
    col("min_oil_pressure_7d"),
    col("tire_pressure_variance_7d"),
    col("avg_battery_voltage_7d"),
    col("idle_time_pct_7d"),
    col("cumulative_maintenance_cost_usd"),
    col("cost_per_km_usd"),
    col("anomaly_score"),
    col("failure_risk_score"),
    # New engineered features
    col("engine_temp_trend"),
    col("oil_pressure_drop"),
    col("battery_health_category"),
    col("high_temp_events"),
    col("low_oil_events"),
    col("total_anomaly_count"),
    col("mileage_per_day_avg"),
    col("maintenance_cost_per_year"),
    # Target variable
    col("will_require_maintenance_7d")
)

print(f"ML features generated for {ml_features_final.count()} vehicles")
print(f"Total features: {len(ml_features_final.columns) - 3}")  # Exclude vehicle_id, feature_date, vehicle_model
display(ml_features_final.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Vehicle Health Summary (Current State)

# COMMAND ----------

print("Creating vehicle health summary...")

# Get latest telemetry reading for each vehicle
window_latest = Window.partitionBy("vehicle_id").orderBy(col("event_timestamp").desc())

latest_telemetry = telemetry_df \
    .withColumn("row_num", row_number().over(window_latest)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# Join with 7-day features for context
health_summary = latest_telemetry.join(
    features_7d.select("vehicle_id", "avg_engine_temp_7d", "avg_oil_pressure_7d", 
                       "engine_anomalies_7d", "oil_anomalies_7d"),
    "vehicle_id",
    "left"
)

# Calculate health status
health_summary = health_summary.withColumn(
    "health_status",
    when(col("overall_health_score") >= 0.8, "HEALTHY")
    .when(col("overall_health_score") >= 0.6, "WARNING")
    .otherwise("CRITICAL")
)

# Calculate days until predicted maintenance
health_summary = health_summary.withColumn(
    "days_until_maintenance",
    when(col("overall_health_score") < 0.5, lit(3))
    .when(col("overall_health_score") < 0.7, lit(7))
    .when(col("overall_health_score") < 0.85, lit(14))
    .otherwise(lit(30))
)

# Select final columns
health_summary_final = health_summary.select(
    current_timestamp().alias("snapshot_timestamp"),
    col("vehicle_id"),
    col("event_timestamp").alias("last_reading_timestamp"),
    col("overall_health_score"),
    col("health_status"),
    col("engine_temp_c"),
    col("oil_pressure_psi"),
    col("avg_tire_pressure_psi"),
    col("battery_voltage_v"),
    col("odometer_km"),
    col("engine_temp_anomaly"),
    col("oil_pressure_anomaly"),
    col("tire_pressure_anomaly"),
    coalesce(col("engine_anomalies_7d"), lit(0)).alias("engine_anomalies_7d"),
    coalesce(col("oil_anomalies_7d"), lit(0)).alias("oil_anomalies_7d"),
    col("days_until_maintenance")
)

print(f"Health summary created for {health_summary_final.count()} vehicles")
display(health_summary_final.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Gold Delta Tables

# COMMAND ----------

# Write Fleet Performance KPIs
gold_kpis_table = "gold_schema.fleet_performance_kpis"

print(f"Writing to {gold_kpis_table}...")

fleet_kpis_final.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("report_date") \
    .option("mergeSchema", "true") \
    .saveAsTable(gold_kpis_table)

print(f"✓ Fleet KPIs written to: {gold_kpis_table}")

# COMMAND ----------

# Write ML Features
gold_ml_features_table = "gold_schema.maintenance_prediction_features"

print(f"Writing to {gold_ml_features_table}...")

ml_features_final.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("feature_date") \
    .option("mergeSchema", "true") \
    .saveAsTable(gold_ml_features_table)

print(f"✓ ML features written to: {gold_ml_features_table}")

# COMMAND ----------

# Write Vehicle Health Summary
gold_health_table = "gold_schema.vehicle_health_summary"

print(f"Writing to {gold_health_table}...")

health_summary_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_health_table)

print(f"✓ Health summary written to: {gold_health_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Optimize Gold Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize Fleet KPIs (report_date is partition column, so only Z-ORDER on vehicle_id)
# MAGIC OPTIMIZE gold_schema.fleet_performance_kpis
# MAGIC ZORDER BY (vehicle_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize ML Features (feature_date is partition column, so only Z-ORDER on vehicle_id)
# MAGIC OPTIMIZE gold_schema.maintenance_prediction_features
# MAGIC ZORDER BY (vehicle_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize Health Summary (no partitions, can Z-ORDER on both)
# MAGIC OPTIMIZE gold_schema.vehicle_health_summary
# MAGIC ZORDER BY (vehicle_id, health_status);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Business Insights & Analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 vehicles by maintenance risk
# MAGIC SELECT 
# MAGIC     vehicle_id,
# MAGIC     ROUND(maintenance_risk_score, 3) AS risk_score,
# MAGIC     ROUND(avg_health_score, 3) AS health_score,
# MAGIC     total_distance_km,
# MAGIC     engine_temp_anomalies,
# MAGIC     oil_pressure_anomalies,
# MAGIC     tire_pressure_anomalies
# MAGIC FROM gold_schema.fleet_performance_kpis
# MAGIC WHERE report_date = '2026-01-07'
# MAGIC ORDER BY maintenance_risk_score DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fleet health distribution
# MAGIC SELECT 
# MAGIC     health_status,
# MAGIC     COUNT(*) AS vehicle_count,
# MAGIC     ROUND(AVG(overall_health_score), 3) AS avg_health_score,
# MAGIC     ROUND(AVG(days_until_maintenance), 1) AS avg_days_until_maintenance
# MAGIC FROM gold_schema.vehicle_health_summary
# MAGIC GROUP BY health_status
# MAGIC ORDER BY 
# MAGIC     CASE health_status 
# MAGIC         WHEN 'CRITICAL' THEN 1 
# MAGIC         WHEN 'WARNING' THEN 2 
# MAGIC         WHEN 'HEALTHY' THEN 3 
# MAGIC     END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vehicles requiring maintenance in next 7 days
# MAGIC SELECT 
# MAGIC     vehicle_id,
# MAGIC     health_status,
# MAGIC     ROUND(overall_health_score, 3) AS health_score,
# MAGIC     days_until_maintenance,
# MAGIC     engine_temp_anomaly,
# MAGIC     oil_pressure_anomaly,
# MAGIC     tire_pressure_anomaly,
# MAGIC     ROUND(odometer_km, 0) AS odometer_km
# MAGIC FROM gold_schema.vehicle_health_summary
# MAGIC WHERE days_until_maintenance <= 7
# MAGIC ORDER BY days_until_maintenance ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Summary

# COMMAND ----------

print("=" * 60)
print("✓ Gold Layer Aggregation Complete!")
print("=" * 60)

# Get counts
kpis_count = spark.table(gold_kpis_table).filter(col("report_date") == report_date).count()
ml_count = spark.table(gold_ml_features_table).filter(col("feature_date") == report_date).count()
health_count = spark.table(gold_health_table).count()

print(f"\n✓ Tables Created:")
print(f"  - {gold_kpis_table}: {kpis_count} vehicles")
print(f"  - {gold_ml_features_table}: {ml_count} vehicles")
print(f"  - {gold_health_table}: {health_count} vehicles")

print(f"\n✓ Business Metrics:")
print(f"  - Daily fleet performance KPIs")
print(f"  - ML-ready prediction features")
print(f"  - Real-time health monitoring")
print(f"  - Maintenance risk scoring")

print(f"\n✓ Optimizations:")
print(f"  - Z-ordering on key columns")
print(f"  - Partitioning by date")

print(f"\n📌 Next Steps:")
print(f"  1. Run notebook 06_ML_Predictive_Maintenance.py")
print(f"  2. Build SQL dashboards")
print(f"  3. Set up alerting for critical vehicles")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Author:** Venkat M  
# MAGIC **Project:** Fleet Optimization & Predictive Maintenance  
# MAGIC **Last Updated:** 2026-01-25
