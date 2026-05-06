# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists healthcarecatalog.silver;

# COMMAND ----------

# DBTITLE 1,Cell 2
bronze_table = 'healthcarecatalog.bronze.visit_raw'
silver_table = 'healthcarecatalog.silver.fact_visit'
checkpoint_path = "abfss://data@healthcarestorageacc.dfs.core.windows.net/silver/fact_visit/checkpoint/"

# COMMAND ----------


from pyspark.sql.functions import col, lag, to_date, datediff, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC %sql
# MAGIC
# MAGIC select * from healthcarecatalog.bronze.visit_raw;

# COMMAND ----------

# DBTITLE 1,Cell 5
silver_patient_table = "healthcarecatalog.silver.dim_patient"
silver_hospital_table = "healthcarecatalog.silver.dim_hospital"
silver_diagnosis_table = "healthcarecatalog.silver.dim_diagnosis"
bronze_table = "healthcarecatalog.bronze.visit_raw"

# COMMAND ----------


df_patient = spark.read.table(silver_patient_table)
df_hospital = spark.read.table(silver_hospital_table)
df_diagnosis = spark.read.table(silver_diagnosis_table)

# COMMAND ----------

df_visit_bronze = (
    spark.readStream.table(bronze_table)
)

# COMMAND ----------

# Rename columns to avoid duplicates
df_patient = df_patient.withColumnRenamed("city", "patient_city")

df_hospital = df_hospital.withColumnRenamed("city", "hospital_city")


# Join clean fact visit with dimension tables
df_fact_combined = (
    df_visit_bronze
        .join(df_patient, "patient_id", "left")
        .join(df_hospital, "hospital_id", "left")
        .join(df_diagnosis, "diagnosis_code", "left")
        .withColumn("admission_date", to_date("admission_date"))
        .withColumn("discharge_date", to_date("discharge_date"))
        .withColumn("load_timestamp", current_timestamp())
)

# COMMAND ----------

# -------------------------
# Merge into Silver fact_visit
# -------------------------
def merge_fact_visit(batch_df, batch_id):

    if not spark.catalog.tableExists(silver_table):
        batch_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        return

    fact = DeltaTable.forName(spark, silver_table)
    fact.alias("t").merge(
        batch_df.alias("s"),
        "t.visit_id = s.visit_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

# -------------------------
# Run as availableNow incremental
# -------------------------
(
    df_fact_combined.drop("load_timestamp").writeStream
        .foreachBatch(merge_fact_visit)
        .outputMode("update")
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
)

# COMMAND ----------

# DBTITLE 1,Cell 11
# MAGIC %sql
# MAGIC
# MAGIC select * from healthcarecatalog.silver.fact_visit;