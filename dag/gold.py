from pyspark.sql import functions as F

def run_gold_transformation(spark, env, silver_table, catalog):
    """
    Transforms the Silver table into a Star Schema (Dimensions and Fact table).
    """
    print(f"🚀 Starting Gold layer transformation for env: {env}")
    silver_final = spark.read.table(silver_table)

    # Dim Collection
    dim_col = silver_final.select("collection").distinct().withColumn("collection_id", F.monotonically_increasing_id())
    dim_col.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{env}.dim_collection")

    # Dim Country
    dim_cnt = silver_final.select("country", "currency").distinct().withColumn("country_id", F.monotonically_increasing_id())
    dim_cnt.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{env}.dim_country")

    # Dim Watch
    dim_w = silver_final.select("reference", "title", "edition", "comment").distinct()
    dim_w.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{env}.dim_watch")

    # Fact Pricing
    fact_p = silver_final.join(dim_col, "collection").join(dim_cnt, "country") \
               .select("reference", "date", "price_eur", "diameter_mm", "collection_id", "country_id")
    fact_p.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{env}.fact_pricing")

    print("✅ Gold layer tables updated.")

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

    run_gold_transformation(spark, env, s_table, catalog)
