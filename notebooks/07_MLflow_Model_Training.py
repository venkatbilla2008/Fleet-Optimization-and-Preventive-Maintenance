# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - MLflow Model Training & Registry
# MAGIC 
# MAGIC ## Fleet Predictive Maintenance with MLflow
# MAGIC 
# MAGIC **Purpose:** Train ML models with MLflow experiment tracking and model registry
# MAGIC 
# MAGIC **Features:**
# MAGIC - Experiment tracking (parameters, metrics, artifacts)
# MAGIC - Model versioning and registry
# MAGIC - Model comparison and selection
# MAGIC - Production deployment
# MAGIC 
# MAGIC **Author:** Venkat M  
# MAGIC **Date:** 2026-01-31

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Imports

# COMMAND ----------

import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
from mlflow.tracking import MlflowClient

import pandas as pd
import numpy as np
from datetime import datetime

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score, 
    roc_auc_score, confusion_matrix, classification_report
)

from pyspark.sql.functions import *

print("=" * 60)
print("MLflow Model Training & Registry")
print("=" * 60)
print(f"MLflow Version: {mlflow.__version__}")
print(f"Tracking URI: {mlflow.get_tracking_uri()}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Features from Gold Layer

# COMMAND ----------

# Load ML features
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
    raise

# Convert to Pandas
features_pd = features_df.toPandas()
print(f"\n✓ Dataset shape: {features_pd.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Feature Preparation

# COMMAND ----------

# Select feature columns (26 features)
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
    # Engineered features
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Validation

# COMMAND ----------

# Check target variable distribution
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
    raise ValueError(f"Cannot train model: dataset contains only one class")

# Check class balance
class_balance = y.value_counts(normalize=True)
min_class_pct = class_balance.min()

if min_class_pct < 0.1:
    print(f"\n⚠️ WARNING: Severe class imbalance detected!")
    print(f"Minority class: {min_class_pct:.1%}")

print("\n✓ Data validation passed - proceeding with train/test split")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Train/Test Split & Scaling

# COMMAND ----------

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
# MAGIC ## 6. MLflow Experiment Setup

# COMMAND ----------

# Set experiment name (use simple name to avoid directory issues)
experiment_name = "/Shared/fleet-predictive-maintenance"

# Create or get experiment
try:
    experiment_id = mlflow.create_experiment(experiment_name)
    print(f"✓ Created new experiment: {experiment_name}")
except:
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment:
        experiment_id = experiment.experiment_id
        print(f"✓ Using existing experiment: {experiment_name}")
    else:
        # Fallback: use default experiment
        experiment_name = "Default"
        mlflow.set_experiment(experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
        experiment_id = experiment.experiment_id
        print(f"✓ Using default experiment")

mlflow.set_experiment(experiment_name)

print(f"Experiment ID: {experiment_id}")
print(f"Tracking URI: {mlflow.get_tracking_uri()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Train Models with MLflow Tracking

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Logistic Regression

# COMMAND ----------

print("Training Logistic Regression with MLflow...")

with mlflow.start_run(run_name="Logistic_Regression") as run:
    
    # Log parameters
    mlflow.log_param("model_type", "Logistic Regression")
    mlflow.log_param("max_iter", 1000)
    mlflow.log_param("random_state", 42)
    mlflow.log_param("n_features", len(feature_columns))
    mlflow.log_param("train_size", X_train.shape[0])
    mlflow.log_param("test_size", X_test.shape[0])
    
    # Train model
    lr_model = LogisticRegression(max_iter=1000, random_state=42)
    lr_model.fit(X_train_scaled, y_train)
    
    # Predictions
    y_pred_lr = lr_model.predict(X_test_scaled)
    y_pred_proba_lr = lr_model.predict_proba(X_test_scaled)[:, 1]
    
    # Calculate metrics
    lr_accuracy = accuracy_score(y_test, y_pred_lr)
    lr_precision = precision_score(y_test, y_pred_lr)
    lr_recall = recall_score(y_test, y_pred_lr)
    lr_f1 = f1_score(y_test, y_pred_lr)
    lr_roc_auc = roc_auc_score(y_test, y_pred_proba_lr)
    
    # Log metrics
    mlflow.log_metric("accuracy", lr_accuracy)
    mlflow.log_metric("precision", lr_precision)
    mlflow.log_metric("recall", lr_recall)
    mlflow.log_metric("f1_score", lr_f1)
    mlflow.log_metric("roc_auc", lr_roc_auc)
    
    # Log confusion matrix as artifact
    cm_lr = confusion_matrix(y_test, y_pred_lr)
    cm_df = pd.DataFrame(cm_lr, 
                         columns=['Predicted_No', 'Predicted_Yes'],
                         index=['Actual_No', 'Actual_Yes'])
    
    # Save confusion matrix
    cm_path = "/tmp/confusion_matrix_lr.csv"
    cm_df.to_csv(cm_path)
    mlflow.log_artifact(cm_path, "confusion_matrix")
    
    # Log model with signature (without registry for Community Edition)
    signature = infer_signature(X_train_scaled, y_pred_lr)
    mlflow.sklearn.log_model(
        lr_model, 
        "model",
        signature=signature
        # Note: Model registry requires Databricks Premium
        # Models are still logged and can be loaded from experiments
    )
    
    # Log feature importance (use numpy.abs to avoid PySpark conflict)
    feature_importance = pd.DataFrame({
        'feature': feature_columns,
        'importance': np.abs(lr_model.coef_[0])
    }).sort_values('importance', ascending=False)
    
    importance_path = "/tmp/feature_importance_lr.csv"
    feature_importance.to_csv(importance_path, index=False)
    mlflow.log_artifact(importance_path, "feature_importance")
    
    print(f"✓ Logistic Regression trained and logged")
    print(f"  Run ID: {run.info.run_id}")
    print(f"  F1-Score: {lr_f1:.4f}")
    print(f"  Recall: {lr_recall:.4f}")
    print(f"  ROC-AUC: {lr_roc_auc:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Random Forest

# COMMAND ----------

print("Training Random Forest with MLflow...")

with mlflow.start_run(run_name="Random_Forest") as run:
    
    # Log parameters
    mlflow.log_param("model_type", "Random Forest")
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 10)
    mlflow.log_param("random_state", 42)
    mlflow.log_param("n_features", len(feature_columns))
    mlflow.log_param("train_size", X_train.shape[0])
    mlflow.log_param("test_size", X_test.shape[0])
    
    # Train model
    rf_model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
    rf_model.fit(X_train_scaled, y_train)
    
    # Predictions
    y_pred_rf = rf_model.predict(X_test_scaled)
    y_pred_proba_rf = rf_model.predict_proba(X_test_scaled)[:, 1]
    
    # Calculate metrics
    rf_accuracy = accuracy_score(y_test, y_pred_rf)
    rf_precision = precision_score(y_test, y_pred_rf)
    rf_recall = recall_score(y_test, y_pred_rf)
    rf_f1 = f1_score(y_test, y_pred_rf)
    rf_roc_auc = roc_auc_score(y_test, y_pred_proba_rf)
    
    # Log metrics
    mlflow.log_metric("accuracy", rf_accuracy)
    mlflow.log_metric("precision", rf_precision)
    mlflow.log_metric("recall", rf_recall)
    mlflow.log_metric("f1_score", rf_f1)
    mlflow.log_metric("roc_auc", rf_roc_auc)
    
    # Log confusion matrix
    cm_rf = confusion_matrix(y_test, y_pred_rf)
    cm_df = pd.DataFrame(cm_rf,
                         columns=['Predicted_No', 'Predicted_Yes'],
                         index=['Actual_No', 'Actual_Yes'])
    
    cm_path = "/tmp/confusion_matrix_rf.csv"
    cm_df.to_csv(cm_path)
    mlflow.log_artifact(cm_path, "confusion_matrix")
    
    # Log model (without registry for Community Edition)
    signature = infer_signature(X_train_scaled, y_pred_rf)
    mlflow.sklearn.log_model(
        rf_model,
        "model",
        signature=signature
    )
    
    # Log feature importance
    feature_importance = pd.DataFrame({
        'feature': feature_columns,
        'importance': rf_model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    importance_path = "/tmp/feature_importance_rf.csv"
    feature_importance.to_csv(importance_path, index=False)
    mlflow.log_artifact(importance_path, "feature_importance")
    
    print(f"✓ Random Forest trained and logged")
    print(f"  Run ID: {run.info.run_id}")
    print(f"  F1-Score: {rf_f1:.4f}")
    print(f"  Recall: {rf_recall:.4f}")
    print(f"  ROC-AUC: {rf_roc_auc:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Gradient Boosting

# COMMAND ----------

print("Training Gradient Boosting with MLflow...")

with mlflow.start_run(run_name="Gradient_Boosting") as run:
    
    # Log parameters
    mlflow.log_param("model_type", "Gradient Boosting")
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("learning_rate", 0.1)
    mlflow.log_param("max_depth", 3)
    mlflow.log_param("random_state", 42)
    mlflow.log_param("n_features", len(feature_columns))
    mlflow.log_param("train_size", X_train.shape[0])
    mlflow.log_param("test_size", X_test.shape[0])
    
    # Train model
    gb_model = GradientBoostingClassifier(
        n_estimators=100,
        learning_rate=0.1,
        max_depth=3,
        random_state=42
    )
    gb_model.fit(X_train_scaled, y_train)
    
    # Predictions
    y_pred_gb = gb_model.predict(X_test_scaled)
    y_pred_proba_gb = gb_model.predict_proba(X_test_scaled)[:, 1]
    
    # Calculate metrics
    gb_accuracy = accuracy_score(y_test, y_pred_gb)
    gb_precision = precision_score(y_test, y_pred_gb)
    gb_recall = recall_score(y_test, y_pred_gb)
    gb_f1 = f1_score(y_test, y_pred_gb)
    gb_roc_auc = roc_auc_score(y_test, y_pred_proba_gb)
    
    # Log metrics
    mlflow.log_metric("accuracy", gb_accuracy)
    mlflow.log_metric("precision", gb_precision)
    mlflow.log_metric("recall", gb_recall)
    mlflow.log_metric("f1_score", gb_f1)
    mlflow.log_metric("roc_auc", gb_roc_auc)
    
    # Log confusion matrix
    cm_gb = confusion_matrix(y_test, y_pred_gb)
    cm_df = pd.DataFrame(cm_gb,
                         columns=['Predicted_No', 'Predicted_Yes'],
                         index=['Actual_No', 'Actual_Yes'])
    
    cm_path = "/tmp/confusion_matrix_gb.csv"
    cm_df.to_csv(cm_path)
    mlflow.log_artifact(cm_path, "confusion_matrix")
    
    # Log model (without registry for Community Edition)
    signature = infer_signature(X_train_scaled, y_pred_gb)
    mlflow.sklearn.log_model(
        gb_model,
        "model",
        signature=signature
    )
    
    # Log feature importance
    feature_importance = pd.DataFrame({
        'feature': feature_columns,
        'importance': gb_model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    importance_path = "/tmp/feature_importance_gb.csv"
    feature_importance.to_csv(importance_path, index=False)
    mlflow.log_artifact(importance_path, "feature_importance")
    
    print(f"✓ Gradient Boosting trained and logged")
    print(f"  Run ID: {run.info.run_id}")
    print(f"  F1-Score: {gb_f1:.4f}")
    print(f"  Recall: {gb_recall:.4f}")
    print(f"  ROC-AUC: {gb_roc_auc:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Model Comparison

# COMMAND ----------

# Compare all models
results = pd.DataFrame({
    'Model': ['Logistic Regression', 'Random Forest', 'Gradient Boosting'],
    'Accuracy': [lr_accuracy, rf_accuracy, gb_accuracy],
    'Precision': [lr_precision, rf_precision, gb_precision],
    'Recall': [lr_recall, rf_recall, gb_recall],
    'F1-Score': [lr_f1, rf_f1, gb_f1],
    'ROC-AUC': [lr_roc_auc, rf_roc_auc, gb_roc_auc]
})

print("\n" + "=" * 80)
print("MODEL COMPARISON")
print("=" * 80)
display(results)

# Find best model (use correct column name with hyphen)
best_model_idx = results['F1-Score'].idxmax()
best_model_name = results.loc[best_model_idx, 'Model']
best_f1 = results.loc[best_model_idx, 'F1-Score']

print(f"\n🏆 Best Model: {best_model_name}")
print(f"   F1-Score: {best_f1:.4f}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Best Model Selection (Community Edition)

# COMMAND ----------

# Note: Model Registry requires Databricks Premium
# In Community Edition, we can still identify and load the best model from experiments

print("=" * 60)
print("BEST MODEL IDENTIFICATION")
print("=" * 60)

# Determine best model
if best_model_name == "Logistic Regression":
    model_artifact = "Logistic_Regression"
    best_model = lr_model
elif best_model_name == "Random Forest":
    model_artifact = "Random_Forest"
    best_model = rf_model
else:
    model_artifact = "Gradient_Boosting"
    best_model = gb_model

print(f"\n🏆 Best Model: {best_model_name}")
print(f"   F1-Score: {best_f1:.4f}")
print(f"   Model Artifact: {model_artifact}")

print(f"\n📌 How to Load This Model:")
print(f"   1. Go to MLflow Experiments")
print(f"   2. Find experiment: {experiment_name}")
print(f"   3. Select run: {model_artifact}")
print(f"   4. Download or load model artifact")

print(f"\n💡 Loading Model from Experiment:")
print(f"   # Get run ID from MLflow UI")
print(f"   run_id = 'your-run-id-here'")
print(f"   model_uri = f'runs:/{{run_id}}/model'")
print(f"   loaded_model = mlflow.sklearn.load_model(model_uri)")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary

# COMMAND ----------

print("=" * 60)
print("✓ MLflow Model Training Complete!")
print("=" * 60)

print(f"\n✓ Experiment: {experiment_name}")
print(f"✓ Models Trained: 3 (LR, RF, GB)")
print(f"✓ Best Model: {best_model_name}")
print(f"✓ Best F1-Score: {best_f1:.4f}")

print(f"\n✓ Metrics Logged:")
print(f"  - Accuracy, Precision, Recall, F1-Score, ROC-AUC")
print(f"  - Confusion matrices")
print(f"  - Feature importance")
print(f"  - Model artifacts")

print(f"\n📌 Community Edition Note:")
print(f"  - Model Registry requires Databricks Premium")
print(f"  - Models are logged in experiments and can be loaded")
print(f"  - All tracking and comparison features work")

print(f"\n📌 Next Steps:")
print(f"  1. View experiments in MLflow UI")
print(f"  2. Compare runs and metrics")
print(f"  3. Load best model from experiment")
print(f"  4. Use for predictions")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Author:** Venkat M  
# MAGIC **Project:** Fleet Optimization & Predictive Maintenance  
# MAGIC **Last Updated:** 2026-01-31
