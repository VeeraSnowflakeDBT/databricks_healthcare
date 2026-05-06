# Databricks notebook source
from pyspark.sql.functions import sha2, col, current_timestamp, monotonically_increasing_id

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists healthcarecatalog.silver;

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %sql
# MAGIC
# MAGIC select * from healthcarecatalog.bronze.diagnosis_raw;

# COMMAND ----------

# DBTITLE 1,Cell 4
bronze_table = 'healthcarecatalog.bronze.diagnosis_raw'
silver_table = 'healthcarecatalog.silver.dim_diagnosis'
checkpoint_path = "abfss://data@healthcarestorageacc.dfs.core.windows.net/silver/dim_diagnosis/checkpoint/"


df_diagnosis_bronze = (
    spark.readStream.table(bronze_table)
)

df_patient_clean = (
    df_diagnosis_bronze
        .dropDuplicates(["diagnosis_code"])
        .withColumn("load_timestamp", current_timestamp())
)


from delta.tables import DeltaTable

def merge_dim_diagnosis(batch_df, batch_id):
    if not spark.catalog.tableExists(silver_table):
        batch_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        return

    # Load Delta table by name and upsert
    dim_diagnosis = DeltaTable.forName(spark, silver_table)

    (dim_diagnosis.alias("t")
        .merge(
            batch_df.alias("s"),
            "t.diagnosis_code = s.diagnosis_code"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())



(
    df_patient_clean.writeStream
        .foreachBatch(merge_dim_diagnosis)
        .outputMode("update")
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
)


# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %sql
# MAGIC
# MAGIC select * from healthcarecatalog.silver.dim_diagnosis;