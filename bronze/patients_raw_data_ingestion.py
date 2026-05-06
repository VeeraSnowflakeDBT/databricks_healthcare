# Databricks notebook source
# MAGIC %md
# MAGIC This is the change

# COMMAND ----------

# DBTITLE 1,Cell 4

source_path = "abfss://data@healthcarestorageacc.dfs.core.windows.net/staging/patients/"
checkpoint_path = "abfss://data@healthcarestorageacc.dfs.core.windows.net/bronze/patient_raw/checkpoint/"
schema_location = "abfss://data@healthcarestorageacc.dfs.core.windows.net/bronze/patient_raw/schema/"

# Autoloader read
df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("cloudFiles.maxFilesPerTrigger", 1) # READ ONE FILE AT A TIME
          .option("cloudFiles.schemaLocation", schema_location)  
          .load(source_path)
     )

# Write Bronze table (append)
(
    df.drop("_rescued_data")
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpoint_path)
      .outputMode("append")
      .trigger(availableNow=True)   # AVAILABLE NOW → ingests all files & stops
      .toTable("healthcarecatalog.bronze.patient_raw")
)

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists healthcarecatalog.bronze;

# COMMAND ----------

# DBTITLE 1,Cell 4

source_path = "abfss://data@healthcarestorageacc.dfs.core.windows.net/staging/patients/"
checkpoint_path = "abfss://data@healthcarestorageacc.dfs.core.windows.net/bronze/patient_raw/checkpoint/"
schema_location = "abfss://data@healthcarestorageacc.dfs.core.windows.net/bronze/patient_raw/schema/"

# Autoloader read
df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("cloudFiles.maxFilesPerTrigger", 1) # READ ONE FILE AT A TIME
          .option("cloudFiles.schemaLocation", schema_location)  
          .load(source_path)
     )

# Write Bronze table (append)
(
    df.drop("_rescued_data")
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpoint_path)
      .outputMode("append")
      .trigger(availableNow=True)   # AVAILABLE NOW → ingests all files & stops
      .toTable("healthcarecatalog.bronze.patient_raw")
)

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %sql
# MAGIC
# MAGIC select * from healthcarecatalog.bronze.patient_raw