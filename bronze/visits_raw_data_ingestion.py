# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists healthcarecatalog.bronze;

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists healthcarecatalog.bronze;

# COMMAND ----------

# DBTITLE 1,Cell 2
source_path = "abfss://data@healthcarestorageacc.dfs.core.windows.net/staging/visits/"
checkpoint_path = "abfss://data@healthcarestorageacc.dfs.core.windows.net/bronze/visit_raw/checkpoint/"
schema_location = "abfss://data@healthcarestorageacc.dfs.core.windows.net/bronze/visit_raw/schema/"

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
      .toTable("healthcarecatalog.bronze.visit_raw")
)

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC %sql
# MAGIC
# MAGIC select * from healthcarecatalog.bronze.visit_raw