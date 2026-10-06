import yaml
import sys
from pyspark.sql import functions as F
from pyspark.sql import Window

def clean_silver_data(df, exchange_rates):
    """
    Cleans, normalizes, and deduplicates data for the Silver layer.
    """
    try:
        with open("./ref_corrected_url.yaml", "r") as f:
            ref_corrected_url = yaml.safe_load(f)
    except:
        ref_corrected_url = {}

    # 1. Basic Cleaning and transformation
    df = df.dropna(subset=["reference"])
    df = df.withColumn('date', F.to_date(F.col('date'), 'd-M-yyyy'))

    # 2. URL Correction
    if ref_corrected_url:
        corrections = [(ref, country, data["url"]) for ref, countries in ref_corrected_url.items() for country, data in countries.items()]
        corr_df = spark.createDataFrame(corrections, ["reference", "country", "correct_url"])
        df = df.join(F.broadcast(corr_df), on=["reference", "country"], how="left")
        df = df.withColumn("url", F.coalesce(F.col("correct_url"), F.col("url"))).drop("correct_url")

    # 3. Normalization
    df = df.withColumn("price_clean", F.regexp_replace(F.col("price"), r"[^0-9]", "").cast("int"))
    df = df.withColumn("diameter_mm", F.regexp_replace(F.col("diameter_tag"), r"[^0-9]", "").cast("int"))

    # 4. Currency Conversion
    rate_expr = F.lit(1.0)
    for curr, rate in exchange_rates.items():
        rate_expr = F.when(F.col("currency") == curr, F.lit(rate)).otherwise(rate_expr)
    df = df.withColumn("price_eur", F.col("price_clean") * rate_expr)

    # 5. Missing Value Handling
    df = df.withColumn("edition", F.coalesce(F.col("edition"), F.lit("unknown")))
    df = df.withColumn("comment", F.coalesce(F.col("comment"), F.lit("unknown")))

    # 6. Price Display Logic
    df = df.withColumn("price_display",
                       F.when(F.col("price_clean").isNull(), F.lit("Contact store directly"))
                        .otherwise(F.col("price_clean").cast("string")))

    # 7. DEDUPLICATION & EXCEPTION HANDLING
    url_count_window = Window.partitionBy("reference", "country", "date")
    df = df.withColumn("unique_url_count", F.size(F.collect_set("url").over(url_count_window)))
    df = df.withColumn("is_exception", F.when(F.col("unique_url_count") > 1, True).otherwise(False))

    dedup_window = Window.partitionBy("reference", "country", "date").orderBy(F.col("ingested_at").desc())
    df = df.withColumn("row_num", F.row_number().over(dedup_window)) \
           .filter("row_num = 1").drop("row_num")

    return df

def run_silver_upsert(spark, bronze_table, silver_table, exchange_rates):
    """
    Implements incremental watermark processing and merges updates into Silver.
    """
    print(f"🚀 Starting Silver incremental upsert to {silver_table}")

    # 1. Determine the watermark
    if spark.catalog.tableExists(silver_table):
        max_ingested_at = spark.sql(f"SELECT MAX(ingested_at) FROM {silver_table}").collect()[0][0]
        print(f"📅 Last silver ingestion: {max_ingested_at}")
        bronze_updates = spark.read.table(bronze_table).filter(F.col("ingested_at") > max_ingested_at)
    else:
        print("🆕 Silver table does not exist. Processing all Bronze data.")
        bronze_updates = spark.read.table(bronze_table)

    # 2. Process new batch
    silver_updates = clean_silver_data(bronze_updates, exchange_rates)

    if silver_updates.count() == 0:
        print("💤 No new data to process in Silver layer.")
        return

    # 3. Setup/Merge
    if not spark.catalog.tableExists(silver_table):
        print(f"Initializing {silver_table}...")
        silver_updates.write.format("delta").saveAsTable(silver_table)
    else:
        silver_updates.createOrReplaceTempView("silver_updates_view")
        spark.sql(f"ALTER TABLE {silver_table} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

        spark.sql(f"""
            MERGE WITH SCHEMA EVOLUTION INTO {silver_table} AS target
            USING silver_updates_view AS source
            ON target.reference = source.reference
            AND target.country = source.country
            AND target.date = source.date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
        print(f"✅ Incremental merge of {silver_updates.count()} rows to {silver_table} complete.")

if __name__ == "__main__":
    # Parse key=value CLI args passed by the job (e.g. env=prod)
    args = dict(arg.split("=", 1) for arg in sys.argv[1:])
    env = args["env"]
    catalog = args["catalog"]
    base_vol = args["base_volume"]

    # Dynamically derive tables
    b_table = f"{catalog}.{env}.panerai_data_bronze"
    s_table = f"{catalog}.{env}.panerai_data_silver"

    exchange_rates = {
        "USD": 0.88, "JPY": 0.0056, "AED": 0.24,
        "CHF": 1.06, "GBP": 1.16, "EUR": 1.0
    }

    run_silver_upsert(spark, b_table, s_table, exchange_rates)
