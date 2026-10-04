from pyspark.sql import functions as F
from pyspark.sql import Window

def merge_dimension(spark, source_df, target_table, natural_key, id_column, attribute_cols):
    """
    Implements SCD Type 1 merge for dimension tables.
    Ensures surrogate keys are stable and attributes are updated.
    """
    print(f"Updating dimension: {target_table}")

    if not spark.catalog.tableExists(target_table):
        print(f"Initializing {target_table}...")
        # Generate initial IDs for the first load
        final_df = source_df.withColumn(id_column, F.monotonically_increasing_id())
        final_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return

    # Get current max ID to ensure new IDs are sequential and stable
    max_id = spark.sql(f"SELECT MAX({id_column}) FROM {target_table}").collect()[0][0] or 0

    # Identify new records that need IDs
    target_keys = spark.table(target_table).select(natural_key).distinct()
    new_records = source_df.join(target_keys, on=natural_key, how="left_anti")

    if new_records.count() > 0:
        # Assign new IDs starting from max_id + 1
        window_spec = Window.orderBy(F.lit(1))
        new_records = new_records.withColumn(
            id_column,
            F.row_number().over(window_spec) + max_id
        )

        # Use a temporary view for the merge
        new_records.createOrReplaceTempView("dim_updates")

        spark.sql(f"""
            MERGE INTO {target_table} AS target
            USING dim_updates AS source
            ON target.{natural_key} = source.{natural_key}
            WHEN MATCHED THEN
                UPDATE SET {', '.join([f"target.{col} = source.{col}" for col in attribute_cols])}
            WHEN NOT MATCHED THEN
                INSERT *
        """)
    else:
        # Even if no new records, we update attributes for existing records
        # Only attempt merge if there are actually attributes to update
        if attribute_cols:
            source_df.createOrReplaceTempView("dim_updates")
            spark.sql(f"""
                MERGE INTO {target_table} AS target
                USING dim_updates AS source
                ON target.{natural_key} = source.{natural_key}
                WHEN MATCHED THEN
                    UPDATE SET {', '.join([f"target.{col} = source.{col}" for col in attribute_cols])}
            """)
        else:
            print(f"No attributes to update for {target_table}, skipping.")

def run_gold_transformation(spark, env, silver_table, catalog):
    """
    Transforms the Silver table into a Star Schema (Dimensions and Fact table) incrementally.
    """
    print(f"🚀 Starting Incremental Gold layer transformation for env: {env}")

    # 1. Load Silver data
    silver_final = spark.read.table(silver_table)

    # Define table names
    dim_col_table = f"{catalog}.{env}.dim_collection"
    dim_cnt_table = f"{catalog}.{env}.dim_country"
    dim_w_table = f"{catalog}.{env}.dim_watch"
    fact_p_table = f"{catalog}.{env}.fact_pricing"

    # 2. Process Dimensions (SCD Type 1)
    # Dim Collection
    col_df = silver_final.select("collection").distinct()
    merge_dimension(spark, col_df, dim_col_table, "collection", "collection_id", [])

    # Dim Country
    cnt_df = silver_final.select("country", "currency").dropDuplicates(["country"])
    merge_dimension(spark, cnt_df, dim_cnt_table, "country", "country_id", ["currency"])

    # Dim Watch
    w_df = silver_final.select("reference", "title", "edition", "comment").dropDuplicates(["reference"])
    merge_dimension(spark, w_df, dim_w_table, "reference", "watch_id", ["title", "edition", "comment"])

    # 3. Process Fact Pricing (Incremental Upsert)
    print(f"Updating fact table: {fact_p_table}")

    # Join silver data with stabilized dimensions to get IDs
    fact_updates = silver_final.join(spark.table(dim_col_table), "collection") \
                              .join(spark.table(dim_cnt_table), "country") \
                              .join(spark.table(dim_w_table), "reference") \
                              .select(
                                  "reference", "date", "price_eur", "diameter_mm",
                                  "collection_id", "country_id", "watch_id"
                              ) \
                              .dropDuplicates(["reference", "date", "collection_id", "country_id"])

    fact_updates.createOrReplaceTempView("fact_updates_view")

    if not spark.catalog.tableExists(fact_p_table):
        print(f"Initializing {fact_p_table}...")
        fact_updates.write.format("delta").mode("overwrite").saveAsTable(fact_p_table)
    else:
        # Merge on the 4-part composite key: reference, date, collection, country
        # Note: collection and country are implicitly handled by the surrogate IDs if we join correctly,
        # but the user explicitly requested reference, date, collection, and country.
        # Since surrogate IDs are 1:1 with those, we can merge on the IDs and reference/date.

        spark.sql(f"""
            MERGE INTO {fact_p_table} AS target
            USING fact_updates_view AS source
            ON target.reference = source.reference
               AND target.date = source.date
               AND target.collection_id = source.collection_id
               AND target.country_id = source.country_id
            WHEN MATCHED THEN
                UPDATE SET target.price_eur = source.price_eur,
                           target.diameter_mm = source.diameter_mm,
                           target.watch_id = source.watch_id
            WHEN NOT MATCHED THEN
                INSERT *
        """)

    print("✅ Gold layer tables updated incrementally.")

if __name__ == "__main__":
    # Databricks Job Parameters
    dbutils.widgets.text("env", "dev", "Environment")
    dbutils.widgets.text("catalog", "panerai_project", "Catalog")
    dbutils.widgets.text("base_volume", "/Volumes/panerai_project", "Base Volume")

    # Get values
    env = dbutils.widgets.get("env")
    catalog = dbutils.widgets.get("catalog")
    base_vol = dbutils.widgets.get("base_volume")

    # Dynamically derive tables
    s_table = f"{catalog}.{env}.panerai_data_silver"

