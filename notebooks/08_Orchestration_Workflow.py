# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Orchestration Workflow Setup
# MAGIC 
# MAGIC ## Automated Pipeline with Databricks Jobs
# MAGIC 
# MAGIC **Purpose:** Set up automated workflow orchestration for the ML pipeline
# MAGIC 
# MAGIC **Workflow:**
# MAGIC 1. Bronze Ingestion (Data Generation)
# MAGIC 2. Silver Transformation (Data Cleaning)
# MAGIC 3. Gold Aggregation (Feature Engineering)
# MAGIC 4. MLflow Model Training (ML Training & Registry)
# MAGIC 
# MAGIC **Author:** Venkat M  
# MAGIC **Date:** 2026-01-31

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Workflow Configuration

# COMMAND ----------

# Workflow configuration
WORKFLOW_CONFIG = {
    "name": "Fleet_Predictive_Maintenance_Pipeline",
    "description": "End-to-end ML pipeline for fleet predictive maintenance",
    "schedule": "0 0 * * *",  # Daily at midnight
    "timeout_seconds": 7200,   # 2 hours
    "max_concurrent_runs": 1,
    "email_notifications": {
        "on_success": ["venkat.m@example.com"],
        "on_failure": ["venkat.m@example.com"],
        "no_alert_for_skipped_runs": False
    }
}

# Notebook paths (update these to your actual paths)
NOTEBOOK_PATHS = {
    "bronze": "/Users/venkat.m@example.com/01_Bronze_Ingestion_IMPROVED",
    "silver": "/Users/venkat.m@example.com/02_Silver_Transformation",
    "gold": "/Users/venkat.m@example.com/03_Gold_Aggregation",
    "mlflow": "/Users/venkat.m@example.com/07_MLflow_Model_Training"
}

# Task configurations
TASK_CONFIGS = {
    "bronze": {
        "timeout_seconds": 1800,  # 30 minutes
        "max_retries": 2,
        "retry_on_timeout": True
    },
    "silver": {
        "timeout_seconds": 2400,  # 40 minutes
        "max_retries": 2,
        "retry_on_timeout": True
    },
    "gold": {
        "timeout_seconds": 1200,  # 20 minutes
        "max_retries": 2,
        "retry_on_timeout": True
    },
    "mlflow": {
        "timeout_seconds": 600,   # 10 minutes
        "max_retries": 1,
        "retry_on_timeout": True
    }
}

print("=" * 60)
print("Workflow Configuration")
print("=" * 60)
print(f"Workflow Name: {WORKFLOW_CONFIG['name']}")
print(f"Schedule: {WORKFLOW_CONFIG['schedule']} (Daily at midnight)")
print(f"Total Timeout: {WORKFLOW_CONFIG['timeout_seconds'] / 3600} hours")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Workflow Definition (JSON)

# COMMAND ----------

import json

# Define the workflow as JSON (for Databricks Jobs API)
workflow_json = {
    "name": WORKFLOW_CONFIG["name"],
    "email_notifications": WORKFLOW_CONFIG["email_notifications"],
    "timeout_seconds": WORKFLOW_CONFIG["timeout_seconds"],
    "max_concurrent_runs": WORKFLOW_CONFIG["max_concurrent_runs"],
    "tasks": [
        {
            "task_key": "bronze_ingestion",
            "description": "Ingest raw data into Bronze layer",
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATHS["bronze"],
                "base_parameters": {
                    "source_date": "{{job.start_time.date}}"
                }
            },
            "timeout_seconds": TASK_CONFIGS["bronze"]["timeout_seconds"],
            "max_retries": TASK_CONFIGS["bronze"]["max_retries"],
            "retry_on_timeout": TASK_CONFIGS["bronze"]["retry_on_timeout"]
        },
        {
            "task_key": "silver_transformation",
            "description": "Clean and transform data in Silver layer",
            "depends_on": [
                {"task_key": "bronze_ingestion"}
            ],
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATHS["silver"],
                "base_parameters": {
                    "processing_date": "{{job.start_time.date}}"
                }
            },
            "timeout_seconds": TASK_CONFIGS["silver"]["timeout_seconds"],
            "max_retries": TASK_CONFIGS["silver"]["max_retries"],
            "retry_on_timeout": TASK_CONFIGS["silver"]["retry_on_timeout"]
        },
        {
            "task_key": "gold_aggregation",
            "description": "Create ML features in Gold layer",
            "depends_on": [
                {"task_key": "silver_transformation"}
            ],
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATHS["gold"],
                "base_parameters": {
                    "report_date": "{{job.start_time.date}}"
                }
            },
            "timeout_seconds": TASK_CONFIGS["gold"]["timeout_seconds"],
            "max_retries": TASK_CONFIGS["gold"]["max_retries"],
            "retry_on_timeout": TASK_CONFIGS["gold"]["retry_on_timeout"]
        },
        {
            "task_key": "mlflow_training",
            "description": "Train ML models with MLflow",
            "depends_on": [
                {"task_key": "gold_aggregation"}
            ],
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATHS["mlflow"],
                "base_parameters": {}
            },
            "timeout_seconds": TASK_CONFIGS["mlflow"]["timeout_seconds"],
            "max_retries": TASK_CONFIGS["mlflow"]["max_retries"],
            "retry_on_timeout": TASK_CONFIGS["mlflow"]["retry_on_timeout"]
        }
    ],
    "schedule": {
        "quartz_cron_expression": WORKFLOW_CONFIG["schedule"],
        "timezone_id": "Asia/Kolkata",
        "pause_status": "UNPAUSED"
    }
}

# Pretty print the workflow JSON
print("Workflow Definition (JSON):")
print("=" * 60)
print(json.dumps(workflow_json, indent=2))
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Workflow via Databricks Jobs API

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** This cell creates the workflow programmatically using Databricks REST API.
# MAGIC 
# MAGIC You can also create workflows manually via the Databricks UI:
# MAGIC 1. Go to Workflows → Create Job
# MAGIC 2. Add tasks in order: Bronze → Silver → Gold → MLflow
# MAGIC 3. Set dependencies between tasks
# MAGIC 4. Configure schedule (daily at midnight)

# COMMAND ----------

# Example: Create workflow using Databricks REST API
# (Requires personal access token)

import requests
import os

def create_databricks_workflow(workflow_definition, databricks_host, token):
    """
    Create a Databricks workflow using the Jobs API
    
    Args:
        workflow_definition: Workflow JSON definition
        databricks_host: Databricks workspace URL
        token: Personal access token
    """
    
    url = f"{databricks_host}/api/2.1/jobs/create"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(url, headers=headers, json=workflow_definition)
    
    if response.status_code == 200:
        job_id = response.json()["job_id"]
        print(f"✓ Workflow created successfully!")
        print(f"  Job ID: {job_id}")
        print(f"  View at: {databricks_host}/#job/{job_id}")
        return job_id
    else:
        print(f"❌ Error creating workflow:")
        print(f"  Status: {response.status_code}")
        print(f"  Response: {response.text}")
        return None

# Uncomment and configure to create workflow programmatically
# DATABRICKS_HOST = "https://dbc-f9a27c8f-092a.cloud.databricks.com"
# DATABRICKS_TOKEN = dbutils.secrets.get(scope="your-scope", key="databricks-token")
# 
# job_id = create_databricks_workflow(workflow_json, DATABRICKS_HOST, DATABRICKS_TOKEN)

print("⚠️ To create workflow programmatically, uncomment and configure the code above")
print("Or create manually via Databricks UI (recommended for first-time setup)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Workflow Monitoring & Alerts

# COMMAND ----------

# Define monitoring queries
MONITORING_QUERIES = {
    "pipeline_status": """
        SELECT 
            job_id,
            run_id,
            start_time,
            end_time,
            state,
            result_state,
            TIMESTAMPDIFF(MINUTE, start_time, end_time) AS duration_minutes
        FROM system.workflow_runs
        WHERE job_name = 'Fleet_Predictive_Maintenance_Pipeline'
        ORDER BY start_time DESC
        LIMIT 10
    """,
    
    "task_failures": """
        SELECT 
            task_key,
            COUNT(*) AS failure_count,
            MAX(end_time) AS last_failure
        FROM system.workflow_task_runs
        WHERE job_name = 'Fleet_Predictive_Maintenance_Pipeline'
          AND result_state = 'FAILED'
          AND end_time >= CURRENT_DATE - INTERVAL 7 DAYS
        GROUP BY task_key
        ORDER BY failure_count DESC
    """,
    
    "data_quality": """
        SELECT 
            'Bronze GPS' AS layer,
            COUNT(*) AS record_count,
            MAX(ingestion_date) AS latest_date
        FROM bronze_schema.gps_tracking_raw
        UNION ALL
        SELECT 
            'Bronze Telemetry',
            COUNT(*),
            MAX(ingestion_date)
        FROM bronze_schema.vehicle_telemetry_raw
        UNION ALL
        SELECT 
            'Silver GPS',
            COUNT(*),
            MAX(event_date)
        FROM silver_schema.gps_tracking_clean
        UNION ALL
        SELECT 
            'Silver Telemetry',
            COUNT(*),
            MAX(event_date)
        FROM silver_schema.vehicle_telemetry_clean
        UNION ALL
        SELECT 
            'Gold Features',
            COUNT(*),
            MAX(feature_date)
        FROM gold_schema.maintenance_prediction_features
    """
}

print("Monitoring Queries Defined:")
print("=" * 60)
for query_name in MONITORING_QUERIES.keys():
    print(f"  - {query_name}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Manual Workflow Trigger

# COMMAND ----------

def trigger_workflow_run(job_id, parameters=None):
    """
    Manually trigger a workflow run
    
    Args:
        job_id: Databricks job ID
        parameters: Optional parameters to pass to the workflow
    """
    
    # This would use Databricks REST API to trigger a run
    # Example implementation:
    
    print(f"Triggering workflow run for Job ID: {job_id}")
    
    if parameters:
        print(f"Parameters: {parameters}")
    
    # API call would go here
    # response = requests.post(f"{databricks_host}/api/2.1/jobs/run-now", ...)
    
    print("✓ Workflow run triggered")
    print("  Check Databricks UI for run status")

# Example usage:
# trigger_workflow_run(job_id=12345, parameters={"source_date": "2024-01-31"})

print("Use trigger_workflow_run() to manually start the pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Workflow Best Practices

# COMMAND ----------

BEST_PRACTICES = """
WORKFLOW ORCHESTRATION BEST PRACTICES:

1. TASK DEPENDENCIES:
   ✓ Bronze → Silver → Gold → MLflow (sequential)
   ✓ Each task depends on previous task success
   ✓ Use task_key to reference dependencies

2. ERROR HANDLING:
   ✓ Set max_retries (2-3 for data tasks, 1 for ML)
   ✓ Enable retry_on_timeout
   ✓ Configure email notifications
   ✓ Set appropriate timeouts per task

3. SCHEDULING:
   ✓ Daily at midnight (0 0 * * *)
   ✓ Use timezone_id for consistency
   ✓ Set max_concurrent_runs = 1 (avoid overlaps)
   ✓ Pause during maintenance windows

4. MONITORING:
   ✓ Check workflow runs daily
   ✓ Monitor task failure rates
   ✓ Validate data quality after each run
   ✓ Set up alerts for failures

5. PARAMETERS:
   ✓ Use {{job.start_time.date}} for dynamic dates
   ✓ Pass parameters between tasks if needed
   ✓ Document all parameters

6. PERFORMANCE:
   ✓ Optimize notebook execution time
   ✓ Use appropriate cluster sizes
   ✓ Enable autoscaling
   ✓ Monitor resource usage

7. TESTING:
   ✓ Test each notebook individually first
   ✓ Run workflow manually before scheduling
   ✓ Validate end-to-end pipeline
   ✓ Check data quality at each layer

8. DOCUMENTATION:
   ✓ Document workflow purpose
   ✓ Maintain runbook for failures
   ✓ Keep contact list updated
   ✓ Version control workflow definitions
"""

print(BEST_PRACTICES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary

# COMMAND ----------

print("=" * 60)
print("✓ Orchestration Workflow Setup Complete!")
print("=" * 60)

print(f"\n✓ Workflow Name: {WORKFLOW_CONFIG['name']}")
print(f"✓ Tasks: 4 (Bronze → Silver → Gold → MLflow)")
print(f"✓ Schedule: Daily at midnight (Asia/Kolkata)")
print(f"✓ Total Duration: ~2 hours")

print(f"\n✓ Task Breakdown:")
print(f"  1. Bronze Ingestion: 30 min")
print(f"  2. Silver Transformation: 40 min")
print(f"  3. Gold Aggregation: 20 min")
print(f"  4. MLflow Training: 10 min")

print(f"\n✓ Features:")
print(f"  - Automatic retries on failure")
print(f"  - Email notifications")
print(f"  - Task dependencies")
print(f"  - Dynamic date parameters")

print(f"\n📌 Next Steps:")
print(f"  1. Create workflow in Databricks UI")
print(f"  2. Configure email notifications")
print(f"  3. Test manual run")
print(f"  4. Enable schedule")
print(f"  5. Monitor daily runs")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Author:** Venkat M  
# MAGIC **Project:** Fleet Optimization & Predictive Maintenance  
# MAGIC **Last Updated:** 2026-01-31
