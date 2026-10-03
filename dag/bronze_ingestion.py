import os
from pyspark.sql import functions as F

def run_bronze_ingestion(spark, env, volume_path, bronze_table, checkpoint_path, schema_location):
    """
    Incremental ingestion from Volume to Bronze Delta Table using Auto Loader.
    """
    print(f"🚀 Starting Bronze ingestion for env: {env}")

    bronze_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("recursiveFileLookup", "true")
        .option("cloudFiles.schemaLocation", schema_location)
        .load(volume_path))

    query_bronze = (bronze_stream
        .withColumn("file_path", F.col("_metadata.file_path"))
        .withColumn("ingested_at", F.current_timestamp())
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(bronze_table))

    print(f"✅ Bronze ingestion to {bronze_table} completed.")
    return query_bronze

if __name__ == "__main__":
    # Databricks Job Parameters
    dbutils.widgets.text("env", "dev", "Environment")
    dbutils.widgets.text("volume_path", "/Volumes/panerai_project/dev/panerai_data", "Volume Path")
    dbutils.widgets.text("bronze_table", "panerai_project.dev.panerai_data_bronze_dev", "Bronze Table")
    dbutils.widgets.text("checkpoint_path", "/Volumes/panerai_project/dev/_checkpoints/bronze_ingestion", "Checkpoint Path")
    dbutils.widgets.text("schema_location", "/Volumes/panerai_project/dev/_checkpoints/bronze_schema", "Schema Location")

    # Get values
    env = dbutils.widgets.get("env")
    vol_path = dbutils.widgets.get("volume_path")
    b_table = dbutils.widgets.get("bronze_table")
    c_path = dbutils.widgets.get("checkpoint_path")
    s_loc = dbutils.widgets.get("schema_location")

    run_bronze_ingestion(spark, env, vol_path, b_table, c_path, s_loc)
