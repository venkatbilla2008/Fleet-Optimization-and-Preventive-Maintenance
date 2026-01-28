# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Diagnostic
# MAGIC 
# MAGIC **Purpose:** Check why ML models are performing poorly (62% accuracy)
# MAGIC 
# MAGIC **Checks:**
# MAGIC 1. Data availability
# MAGIC 2. Feature variation
# MAGIC 3. Target variable distribution
# MAGIC 4. Feature correlations

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd
import numpy as np

GOLD_SCHEMA = "gold_schema"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Check Data Availability

# COMMAND ----------

# Check if table exists and has data
try:
    ml_features = spark.table(f"{GOLD_SCHEMA}.maintenance_prediction_features")
    record_count = ml_features.count()
    print(f"✓ Table exists with {record_count} records")
    
    if record_count == 0:
        print("❌ ERROR: Table is EMPTY!")
        print("\nPossible causes:")
        print("1. Gold notebook not run")
        print("2. Wrong date parameter")
        print("3. No data in Silver tables")
    else:
        print("\n✓ Data available, continuing diagnostics...")
        
except Exception as e:
    print(f"❌ ERROR: Table not found or error accessing it")
    print(f"Error: {e}")
    print("\nAction: Run Gold notebook (03_Gold_Aggregation.py) first!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Check Target Variable Distribution

# COMMAND ----------

if record_count > 0:
    print("Checking target variable distribution...")
    
    target_dist = ml_features.groupBy("will_require_maintenance_7d").count().toPandas()
    print("\nTarget Variable Distribution:")
    print(target_dist)
    
    if len(target_dist) == 1:
        print("\n❌ CRITICAL ERROR: Only ONE class in target variable!")
        print("This is why F1-Score is so low!")
        print("\nFix: Re-run Gold notebook with median-based threshold")
    else:
        print("\n✓ Both classes present")
        
        # Check balance
        class_0 = target_dist[target_dist['will_require_maintenance_7d'] == False]['count'].values[0]
        class_1 = target_dist[target_dist['will_require_maintenance_7d'] == True]['count'].values[0]
        ratio = min(class_0, class_1) / max(class_0, class_1)
        
        print(f"\nClass balance ratio: {ratio:.2f}")
        if ratio < 0.3:
            print("⚠️ WARNING: Severe class imbalance!")
            print("Recommendation: Use SMOTE or class_weight='balanced'")
        elif ratio < 0.5:
            print("⚠️ WARNING: Moderate class imbalance")
        else:
            print("✓ Classes reasonably balanced")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Check Feature Variation

# COMMAND ----------

if record_count > 0:
    print("Checking feature variation...")
    
    # Get all numeric columns
    numeric_cols = [f.name for f in ml_features.schema.fields 
                    if f.dataType.typeName() in ['double', 'float', 'integer', 'long']]
    
    # Calculate statistics
    stats = ml_features.select(numeric_cols).describe().toPandas()
    
    print("\nFeature Statistics:")
    display(stats)
    
    # Check for constant features (no variation)
    constant_features = []
    for col in numeric_cols:
        std_dev = float(stats[stats['summary'] == 'stddev'][col].values[0])
        if std_dev == 0 or pd.isna(std_dev):
            constant_features.append(col)
    
    if constant_features:
        print(f"\n❌ CRITICAL ERROR: {len(constant_features)} features have NO VARIATION!")
        print("Constant features (useless for ML):")
        for feat in constant_features:
            print(f"  - {feat}")
        print("\nThis is why models can't learn!")
        print("Fix: Re-run Gold notebook with varying features (using rand())")
    else:
        print("\n✓ All features have variation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Check Feature Correlations

# COMMAND ----------

if record_count > 0:
    print("Checking feature correlations with target...")
    
    # Convert to Pandas
    features_pd = ml_features.select(numeric_cols + ['will_require_maintenance_7d']).toPandas()
    features_pd['target_numeric'] = features_pd['will_require_maintenance_7d'].astype(int)
    
    # Calculate correlations
    correlations = features_pd[numeric_cols].corrwith(features_pd['target_numeric']).sort_values(ascending=False)
    
    print("\nTop 10 Features Correlated with Target:")
    print(correlations.head(10))
    
    print("\nBottom 10 Features (Least Correlated):")
    print(correlations.tail(10))
    
    # Check if any features are predictive
    max_corr = abs(correlations).max()
    if max_corr < 0.1:
        print(f"\n❌ CRITICAL ERROR: Maximum correlation = {max_corr:.3f}")
        print("No features are predictive of the target!")
        print("\nPossible causes:")
        print("1. Features are all constant")
        print("2. Target variable poorly defined")
        print("3. No real relationship in data")
    elif max_corr < 0.3:
        print(f"\n⚠️ WARNING: Maximum correlation = {max_corr:.3f}")
        print("Features are weakly predictive")
    else:
        print(f"\n✓ Good correlation found: {max_corr:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sample Data Inspection

# COMMAND ----------

if record_count > 0:
    print("Sample of ML features:")
    display(ml_features.limit(10))
    
    print("\nChecking for duplicate rows...")
    distinct_count = ml_features.select("vehicle_id").distinct().count()
    print(f"Total records: {record_count}")
    print(f"Distinct vehicles: {distinct_count}")
    
    if distinct_count < record_count:
        print(f"⚠️ WARNING: {record_count - distinct_count} duplicate vehicle records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Diagnostic Summary

# COMMAND ----------

print("=" * 80)
print("DIAGNOSTIC SUMMARY")
print("=" * 80)

if record_count == 0:
    print("\n❌ CRITICAL: No data in ML features table")
    print("\nACTION REQUIRED:")
    print("1. Run Gold notebook (03_Gold_Aggregation.py)")
    print("2. Set report_date = '2024-01-07'")
    print("3. Ensure Silver tables have data")
    
elif len(target_dist) == 1:
    print("\n❌ CRITICAL: Only one class in target variable")
    print("\nACTION REQUIRED:")
    print("1. Re-upload fixed Gold notebook")
    print("2. Ensure median-based threshold is used")
    print("3. Re-run Gold notebook")
    
elif constant_features:
    print(f"\n❌ CRITICAL: {len(constant_features)} features have no variation")
    print("\nACTION REQUIRED:")
    print("1. Re-upload fixed Gold notebook with rand() for variation")
    print("2. Re-run Gold notebook")
    print("3. Verify features have different values")
    
elif max_corr < 0.1:
    print("\n❌ CRITICAL: No predictive features")
    print("\nACTION REQUIRED:")
    print("1. Check feature engineering logic")
    print("2. Verify target variable definition")
    print("3. Review data generation process")
    
else:
    print("\n✓ Data quality looks good!")
    print("\nIf ML performance is still poor, check:")
    print("1. Model hyperparameters")
    print("2. Feature scaling")
    print("3. Train/test split")

print("=" * 80)
