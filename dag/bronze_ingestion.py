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
    dbutils.widgets.text("catalog", "panerai_project", "Catalog")
    dbutils.widgets.text("base_volume", "/Volumes/panerai_project", "Base Volume")

    # Get values
    env = dbutils.widgets.get("env")
    catalog = dbutils.widgets.get("catalog")
    base_vol = dbutils.widgets.get("base_volume")

    # Dynamically derive paths and tables
    vol_path = f"{base_vol}/{env}/panerai_data"
    b_table = f"{catalog}.{env}.panerai_data_bronze"
    c_path = f"{base_vol}/{env}/_checkpoints/bronze_ingestion"
    s_loc = f"{base_vol}/{env}/_checkpoints/bronze_schema"

    run_bronze_ingestion(spark, env, vol_path, b_table, c_path, s_loc)
