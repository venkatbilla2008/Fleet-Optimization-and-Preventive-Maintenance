# Databricks notebook source
# MAGIC %md
# MAGIC # Quick Fix: Restart Python Kernel
# MAGIC 
# MAGIC **If you're getting NumPy/Pandas compatibility errors, run this cell:**

# COMMAND ----------

# Detach and reattach notebook to cluster
# This clears the Python environment

dbutils.notebook.exit("Restarting Python environment...")

# COMMAND ----------

# MAGIC %md
# MAGIC **After running above, re-run your ML notebook from the beginning**
