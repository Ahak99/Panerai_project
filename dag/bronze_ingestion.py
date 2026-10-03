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
