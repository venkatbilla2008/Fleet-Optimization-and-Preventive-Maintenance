# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - ML Predictive Maintenance Model (Community Edition)
# MAGIC 
# MAGIC ## Fleet Optimization & Predictive Maintenance
# MAGIC 
# MAGIC **Purpose:** Train and evaluate predictive maintenance models
# MAGIC 
# MAGIC **Problem:** Binary Classification - Predict if a vehicle will require maintenance in the next 7 days
# MAGIC 
# MAGIC **Models:**
# MAGIC 1. Logistic Regression (Baseline)
# MAGIC 2. Random Forest
# MAGIC 3. Gradient Boosting
# MAGIC 
# MAGIC **Note:** This version is optimized for Databricks Community Edition (no MLflow logging)
# MAGIC 
# MAGIC **Author:** Venkat M  
# MAGIC **Date:** 2026-01-25

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report,
    roc_curve
)

from pyspark.sql.functions import *

# Configuration
GOLD_SCHEMA = "gold_schema"

print("=" * 60)
print("ML Predictive Maintenance Model Training")
print("=" * 60)
print(f"Gold Schema: {GOLD_SCHEMA}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load ML Features

# COMMAND ----------

# Read ML features from Gold layer (hardcoded for Community Edition)
ml_features_table = "gold_schema.maintenance_prediction_features"

print(f"Loading features from: {ml_features_table}")

try:
    features_df = spark.table(ml_features_table)
    record_count = features_df.count()
    print(f"✓ Total records: {record_count:,}")
    print(f"✓ Features: {len(features_df.columns)}")
    
    if record_count == 0:
        print("\n❌ ERROR: Table exists but has NO DATA!")
        print("Please run 03_Gold_Aggregation.py first")
        raise ValueError("Empty features table")
        
except Exception as e:
    print(f"\n❌ Error loading features table: {e}")
    print("\nAvailable Gold tables:")
    spark.sql("SHOW TABLES IN gold_schema").show()
    print("\nPlease ensure 03_Gold_Aggregation.py has been run successfully")
    raise

# Show sample
display(features_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Feature Engineering & Preparation

# COMMAND ----------

# Convert to Pandas for sklearn
features_pd = features_df.toPandas()

print(f"Dataset shape: {features_pd.shape}")
print(f"\nTarget variable distribution:")
print(features_pd['will_require_maintenance_7d'].value_counts())
print(f"\nClass balance:")
print(features_pd['will_require_maintenance_7d'].value_counts(normalize=True))

# COMMAND ----------

# Select feature columns (IMPROVED - now 26 features)
feature_columns = [
    'vehicle_age_years',
    'total_mileage_km',
    'avg_daily_mileage_30d',
    'days_since_last_maintenance',
    'km_since_last_maintenance',
    'maintenance_frequency_30d',
    'avg_engine_temp_7d',
    'max_engine_temp_7d',
    'std_engine_temp_7d',
    'avg_oil_pressure_7d',
    'min_oil_pressure_7d',
    'tire_pressure_variance_7d',
    'avg_battery_voltage_7d',
    'idle_time_pct_7d',
    'cumulative_maintenance_cost_usd',
    'cost_per_km_usd',
    'anomaly_score',
    'failure_risk_score',
    # New engineered features
    'engine_temp_trend',
    'oil_pressure_drop',
    'battery_health_category',
    'high_temp_events',
    'low_oil_events',
    'total_anomaly_count',
    'mileage_per_day_avg',
    'maintenance_cost_per_year'
]

print(f"Total features: {len(feature_columns)}")

# Handle missing values
features_pd[feature_columns] = features_pd[feature_columns].fillna(0)

# Prepare X and y
X = features_pd[feature_columns]
y = features_pd['will_require_maintenance_7d'].astype(int)

print(f"\nFeature matrix shape: {X.shape}")
print(f"Target vector shape: {y.shape}")
print(f"\nFeatures used: {len(feature_columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Train-Test Split & Scaling

# COMMAND ----------

# Check target variable distribution BEFORE splitting
print("=" * 80)
print("DATA VALIDATION")
print("=" * 80)
print(f"\nTarget variable distribution:")
print(y.value_counts())
print(f"\nTarget variable percentages:")
print(y.value_counts(normalize=True))

# Check if we have both classes
unique_classes = y.nunique()
print(f"\nNumber of unique classes: {unique_classes}")

if unique_classes < 2:
    print("\n" + "=" * 80)
    print("❌ ERROR: Dataset has only ONE class!")
    print("=" * 80)
    print(f"All values are: {y.unique()[0]}")
    print("\nThis means the Gold notebook's target variable calculation is broken.")
    print("The 'will_require_maintenance_7d' column should have both True and False values.")
    print("\nPossible causes:")
    print("1. failure_risk_score has no variation")
    print("2. Median threshold calculation failed")
    print("3. All vehicles have the same risk score")
    print("\nPlease check the Gold notebook (03_Gold_Aggregation.py)")
    print("=" * 80)
    raise ValueError(f"Cannot train model: dataset contains only one class ({y.unique()[0]})")

# Check class balance
class_balance = y.value_counts(normalize=True)
min_class_pct = class_balance.min()

if min_class_pct < 0.1:
    print(f"\n⚠️ WARNING: Severe class imbalance detected!")
    print(f"Minority class: {min_class_pct:.1%}")
    print("This may affect model performance.")

print("\n✓ Data validation passed - proceeding with train/test split")
print("=" * 80)

# Split data (80/20) with stratification
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Training set: {X_train.shape[0]} samples")
print(f"Test set: {X_test.shape[0]} samples")
print(f"\nTraining set class distribution:")
print(y_train.value_counts(normalize=True))

# Scale features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

print("\n✓ Features scaled using StandardScaler")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Model Training

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Baseline Model - Logistic Regression

# COMMAND ----------

print("Training Logistic Regression Model...")

# Train model
lr_model = LogisticRegression(random_state=42, max_iter=1000)
lr_model.fit(X_train_scaled, y_train)

# Predictions
y_pred_lr = lr_model.predict(X_test_scaled)
y_pred_proba_lr = lr_model.predict_proba(X_test_scaled)[:, 1]

# Calculate metrics
accuracy_lr = accuracy_score(y_test, y_pred_lr)
precision_lr = precision_score(y_test, y_pred_lr)
recall_lr = recall_score(y_test, y_pred_lr)
f1_lr = f1_score(y_test, y_pred_lr)
roc_auc_lr = roc_auc_score(y_test, y_pred_proba_lr)

# Display confusion matrix
cm_lr = confusion_matrix(y_test, y_pred_lr)
fig, ax = plt.subplots(figsize=(8, 6))
sns.heatmap(cm_lr, annot=True, fmt='d', cmap='Blues', ax=ax)
ax.set_title('Confusion Matrix - Logistic Regression')
ax.set_xlabel('Predicted')
ax.set_ylabel('Actual')
plt.show()

print("\n" + "=" * 60)
print("✓ Logistic Regression Model Results")
print("=" * 60)
print(f"  Accuracy:  {accuracy_lr:.4f}")
print(f"  Precision: {precision_lr:.4f}")
print(f"  Recall:    {recall_lr:.4f}")
print(f"  F1-Score:  {f1_lr:.4f}")
print(f"  ROC-AUC:   {roc_auc_lr:.4f}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Random Forest Classifier

# COMMAND ----------

print("Training Random Forest Model...")

# Train model with optimized hyperparameters
rf_model = RandomForestClassifier(
    n_estimators=200,  # Increased from 100
    max_depth=15,      # Increased from 10
    min_samples_split=5,
    min_samples_leaf=2,
    max_features='sqrt',
    random_state=42,
    n_jobs=-1,
    class_weight='balanced'  # Handle class imbalance
)
rf_model.fit(X_train_scaled, y_train)

# Predictions
y_pred_rf = rf_model.predict(X_test_scaled)
y_pred_proba_rf = rf_model.predict_proba(X_test_scaled)[:, 1]

# Calculate metrics
accuracy_rf = accuracy_score(y_test, y_pred_rf)
precision_rf = precision_score(y_test, y_pred_rf, zero_division=0)
recall_rf = recall_score(y_test, y_pred_rf, zero_division=0)
f1_rf = f1_score(y_test, y_pred_rf, zero_division=0)
roc_auc_rf = roc_auc_score(y_test, y_pred_proba_rf)

# Feature importance
feature_importance = pd.DataFrame({
    'feature': feature_columns,
    'importance': rf_model.feature_importances_
}).sort_values('importance', ascending=False)

# Display feature importance
fig, ax = plt.subplots(figsize=(10, 8))
sns.barplot(data=feature_importance.head(10), x='importance', y='feature', ax=ax, palette='viridis')
ax.set_title('Top 10 Feature Importance - Random Forest', fontsize=14, fontweight='bold')
ax.set_xlabel('Importance Score', fontsize=12)
ax.set_ylabel('Feature', fontsize=12)
plt.tight_layout()
plt.show()

# Display confusion matrix
cm_rf = confusion_matrix(y_test, y_pred_rf)
fig, ax = plt.subplots(figsize=(8, 6))
sns.heatmap(cm_rf, annot=True, fmt='d', cmap='Blues', ax=ax, cbar_kws={'label': 'Count'})
ax.set_title('Confusion Matrix - Random Forest', fontsize=14, fontweight='bold')
ax.set_xlabel('Predicted', fontsize=12)
ax.set_ylabel('Actual', fontsize=12)
plt.tight_layout()
plt.show()

# Classification report
print("\nClassification Report:")
print(classification_report(y_test, y_pred_rf, target_names=['No Maintenance', 'Needs Maintenance']))

print("\n" + "=" * 60)
print("✓ Random Forest Model Results")
print("=" * 60)
print(f"  Accuracy:  {accuracy_rf:.4f}")
print(f"  Precision: {precision_rf:.4f}")
print(f"  Recall:    {recall_rf:.4f}")
print(f"  F1-Score:  {f1_rf:.4f}")
print(f"  ROC-AUC:   {roc_auc_rf:.4f}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Gradient Boosting Classifier

# COMMAND ----------

print("Training Gradient Boosting Model...")

# Train model with optimized hyperparameters
gb_model = GradientBoostingClassifier(
    n_estimators=150,      # Increased from 100
    learning_rate=0.05,    # Reduced for better generalization
    max_depth=6,           # Increased from 5
    min_samples_split=10,
    min_samples_leaf=4,
    subsample=0.8,
    max_features='sqrt',
    random_state=42
)
gb_model.fit(X_train_scaled, y_train)

# Predictions
y_pred_gb = gb_model.predict(X_test_scaled)
y_pred_proba_gb = gb_model.predict_proba(X_test_scaled)[:, 1]

# Calculate metrics
accuracy_gb = accuracy_score(y_test, y_pred_gb)
precision_gb = precision_score(y_test, y_pred_gb, zero_division=0)
recall_gb = recall_score(y_test, y_pred_gb, zero_division=0)
f1_gb = f1_score(y_test, y_pred_gb, zero_division=0)
roc_auc_gb = roc_auc_score(y_test, y_pred_proba_gb)

# Feature importance
feature_importance_gb = pd.DataFrame({
    'feature': feature_columns,
    'importance': gb_model.feature_importances_
}).sort_values('importance', ascending=False)

# Display feature importance
fig, ax = plt.subplots(figsize=(10, 8))
sns.barplot(data=feature_importance_gb.head(10), x='importance', y='feature', ax=ax, palette='rocket')
ax.set_title('Top 10 Feature Importance - Gradient Boosting', fontsize=14, fontweight='bold')
ax.set_xlabel('Importance Score', fontsize=12)
ax.set_ylabel('Feature', fontsize=12)
plt.tight_layout()
plt.show()

# Display confusion matrix
cm_gb = confusion_matrix(y_test, y_pred_gb)
fig, ax = plt.subplots(figsize=(8, 6))
sns.heatmap(cm_gb, annot=True, fmt='d', cmap='Greens', ax=ax, cbar_kws={'label': 'Count'})
ax.set_title('Confusion Matrix - Gradient Boosting', fontsize=14, fontweight='bold')
ax.set_xlabel('Predicted', fontsize=12)
ax.set_ylabel('Actual', fontsize=12)
plt.tight_layout()
plt.show()

# ROC Curve
fpr, tpr, _ = roc_curve(y_test, y_pred_proba_gb)
fig, ax = plt.subplots(figsize=(8, 6))
ax.plot(fpr, tpr, label=f'Gradient Boosting (AUC = {roc_auc_gb:.4f})', linewidth=2, color='green')
ax.plot([0, 1], [0, 1], 'k--', label='Random Classifier', linewidth=1)
ax.set_xlabel('False Positive Rate', fontsize=12)
ax.set_ylabel('True Positive Rate', fontsize=12)
ax.set_title('ROC Curve - Gradient Boosting', fontsize=14, fontweight='bold')
ax.legend(loc='lower right', fontsize=10)
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

# Classification report
print("\nClassification Report:")
print(classification_report(y_test, y_pred_gb, target_names=['No Maintenance', 'Needs Maintenance']))

print("\n" + "=" * 60)
print("✓ Gradient Boosting Model Results")
print("=" * 60)
print(f"  Accuracy:  {accuracy_gb:.4f}")
print(f"  Precision: {precision_gb:.4f}")
print(f"  Recall:    {recall_gb:.4f}")
print(f"  F1-Score:  {f1_gb:.4f}")
print(f"  ROC-AUC:   {roc_auc_gb:.4f}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Model Comparison

# COMMAND ----------

# Compare all models
model_comparison = pd.DataFrame({
    'Model': ['Logistic Regression', 'Random Forest', 'Gradient Boosting'],
    'Accuracy': [accuracy_lr, accuracy_rf, accuracy_gb],
    'Precision': [precision_lr, precision_rf, precision_gb],
    'Recall': [recall_lr, recall_rf, recall_gb],
    'F1-Score': [f1_lr, f1_rf, f1_gb],
    'ROC-AUC': [roc_auc_lr, roc_auc_rf, roc_auc_gb]
})

print("\n" + "=" * 60)
print("Model Comparison")
print("=" * 60)
display(model_comparison)

# Visualize comparison
fig, axes = plt.subplots(2, 3, figsize=(15, 10))
metrics = ['Accuracy', 'Precision', 'Recall', 'F1-Score', 'ROC-AUC']

for idx, metric in enumerate(metrics):
    ax = axes[idx // 3, idx % 3]
    sns.barplot(data=model_comparison, x='Model', y=metric, ax=ax)
    ax.set_title(f'{metric} Comparison')
    ax.set_ylim(0, 1)
    ax.tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Best Model Selection

# COMMAND ----------

# Select best model based on F1-Score
best_model_idx = model_comparison['F1-Score'].idxmax()
best_model_name = model_comparison.loc[best_model_idx, 'Model']

print("\n" + "=" * 60)
print(f"🏆 Best Model: {best_model_name}")
print("=" * 60)
print(f"   F1-Score:  {model_comparison.loc[best_model_idx, 'F1-Score']:.4f}")
print(f"   Precision: {model_comparison.loc[best_model_idx, 'Precision']:.4f}")
print(f"   Recall:    {model_comparison.loc[best_model_idx, 'Recall']:.4f}")
print(f"   ROC-AUC:   {model_comparison.loc[best_model_idx, 'ROC-AUC']:.4f}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Business Impact Analysis

# COMMAND ----------

print("\n" + "=" * 60)
print("Business Impact Analysis")
print("=" * 60)

# Assumptions:
# - Average unplanned breakdown cost: $2,000
# - Average preventive maintenance cost: $500
# - Current breakdown rate: 15% of fleet per month

total_vehicles = len(features_pd)
current_monthly_breakdowns = int(total_vehicles * 0.15)
current_monthly_cost = current_monthly_breakdowns * 2000

# With predictive maintenance (using best model's recall)
predicted_preventions = int(current_monthly_breakdowns * recall_gb)
remaining_breakdowns = current_monthly_breakdowns - predicted_preventions
new_monthly_cost = (predicted_preventions * 500) + (remaining_breakdowns * 2000)

monthly_savings = current_monthly_cost - new_monthly_cost
annual_savings = monthly_savings * 12

print(f"\nCurrent State:")
print(f"  - Monthly breakdowns: {current_monthly_breakdowns}")
print(f"  - Monthly cost: ${current_monthly_cost:,}")

print(f"\nWith Predictive Maintenance:")
print(f"  - Prevented breakdowns: {predicted_preventions}")
print(f"  - Remaining breakdowns: {remaining_breakdowns}")
print(f"  - Monthly cost: ${new_monthly_cost:,}")

print(f"\n💰 Cost Savings:")
print(f"  - Monthly: ${monthly_savings:,}")
print(f"  - Annual: ${annual_savings:,}")
print(f"  - ROI: {((current_monthly_cost - new_monthly_cost) / current_monthly_cost * 100):.1f}%")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary

# COMMAND ----------

print("=" * 60)
print("✓ ML Predictive Maintenance Model Training Complete!")
print("=" * 60)

print(f"\n✓ Models Trained:")
print(f"  - Logistic Regression (Baseline)")
print(f"  - Random Forest")
print(f"  - Gradient Boosting")

print(f"\n✓ Best Model: {best_model_name}")
print(f"  - F1-Score:  {model_comparison.loc[best_model_idx, 'F1-Score']:.4f}")
print(f"  - Precision: {model_comparison.loc[best_model_idx, 'Precision']:.4f}")
print(f"  - Recall:    {model_comparison.loc[best_model_idx, 'Recall']:.4f}")

print(f"\n✓ Business Impact:")
print(f"  - Annual cost savings: ${annual_savings:,}")
print(f"  - Breakdown reduction: {(predicted_preventions / current_monthly_breakdowns * 100):.1f}%")

print(f"\n📌 Next Steps:")
print(f"  1. Deploy model for batch/real-time inference")
print(f"  2. Set up monitoring and retraining pipeline")
print(f"  3. Integrate predictions with maintenance scheduling")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Author:** Venkat M  
# MAGIC **Project:** Fleet Optimization & Predictive Maintenance  
# MAGIC **Version:** Community Edition Compatible (No MLflow)  
# MAGIC **Last Updated:** 2026-01-25
