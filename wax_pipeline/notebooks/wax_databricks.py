# Databricks notebook source
# MAGIC %md
# MAGIC # WAX Pipeline - Databricks

# COMMAND ----------

%pip install openpyxl

# COMMAND ----------

import sys
sys.path.append("/dbfs/wax_pipeline/src")
from main import main

# COMMAND ----------

main()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vérifier les logs
# MAGIC - `/mnt/logs/execution`
# MAGIC - `/mnt/logs/quality`
