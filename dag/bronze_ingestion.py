import os
import sys
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
    # Parse key=value CLI args passed by the job (e.g. env=prod)
    args = dict(arg.split("=", 1) for arg in sys.argv[1:])
    env = args["env"]
    catalog = args["catalog"]
    base_vol = args["base_volume"]

    # Dynamically derive paths and tables
    vol_path = f"{base_vol}/{env}/panerai_data"
    b_table = f"{catalog}.{env}.panerai_data_bronze"
    c_path = f"{base_vol}/{env}/panerai_checkpoints/bronze_schema"
    s_loc = f"{base_vol}/{env}/panerai_checkpoints/bronze_ingestion"

    run_bronze_ingestion(spark, env, vol_path, b_table, c_path, s_loc)
    