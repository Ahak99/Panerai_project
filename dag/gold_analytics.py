import sys
from pyspark.sql import functions as F

def run_analytics_transformation(spark, env, catalog):
    """
    Creates analysis views on top of the Gold layer for the PoC Dashboard.
    Using views ensures the dashboard always reflects the latest state of the Gold tables
    without needing to manage additional physical storage.
    """
    print(f"🚀 Creating Analytics Views for env: {env}")

    # 1. Unified Analysis View
    # This is the base view used by all other analytics views.
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{env}.v_deep_analysis AS
        SELECT
            p.date, p.price_eur, p.diameter_mm, p.reference, p.url,
            c.collection,
            cnt.country, cnt.currency,
            w.title, w.edition
        FROM {catalog}.{env}.fact_pricing p
        JOIN {catalog}.{env}.dim_collection c ON p.collection_id = c.collection_id
        JOIN {catalog}.{env}.dim_country cnt ON p.country_id = cnt.country_id
        JOIN {catalog}.{env}.dim_watch w ON p.watch_id = w.watch_id
    """)

    # 2. Price Distribution View (Median based)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{env}.v_analytics_price_distribution AS
        SELECT
            collection,
            country,
            percentile_approx(price_eur, 0.5) as median_price,
            COUNT(*) as offer_count
        FROM {catalog}.{env}.v_deep_analysis
        GROUP BY collection, country
    """)

    # 3. Price Extremes View (Most/Least expensive per collection)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{env}.v_analytics_extremes AS
        WITH CollectionRankings AS (
            SELECT
               collection, title, reference, price_eur, country, url,
                ROW_NUMBER() OVER (PARTITION BY collection ORDER BY price_eur DESC) as rank_desc,
                ROW_NUMBER() OVER (PARTITION BY collection ORDER BY price_eur ASC) as rank_asc
            FROM {catalog}.{env}.v_deep_analysis
        )
        SELECT reference, title, collection, country, price_eur, 'Most Expensive' as label, url
        FROM CollectionRankings WHERE rank_desc = 1
        UNION ALL
        SELECT reference, title, collection, country, price_eur, 'Least Expensive' as label, url
        FROM CollectionRankings WHERE rank_asc = 1
        ORDER BY collection
    """)

    # 4. Market Variance View (Country extremes per reference)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{env}.v_analytics_market_variance AS
        WITH ReferenceExtremes AS (
            SELECT
                reference,
                country,
                price_eur,
                url,
                FIRST_VALUE(country) OVER (PARTITION BY reference ORDER BY price_eur ASC) as cheapest_country,
                FIRST_VALUE(price_eur) OVER (PARTITION BY reference ORDER BY price_eur ASC) as min_price,
                FIRST_VALUE(url) OVER (PARTITION BY reference ORDER BY price_eur ASC) as min_price_url,
                FIRST_VALUE(country) OVER (PARTITION BY reference ORDER BY price_eur DESC) as most_expensive_country,
                FIRST_VALUE(price_eur) OVER (PARTITION BY reference ORDER BY price_eur DESC) as max_price,
                FIRST_VALUE(url) OVER (PARTITION BY reference ORDER BY price_eur DESC) as max_price_url
            FROM {catalog}.{env}.v_deep_analysis
        )
        SELECT 
            reference,
            ROUND((max_price - min_price) / min_price * 100, 2)      AS arbitrage_pct,
            ROUND(max_price - min_price, 2)                          AS arbitrage_delta_eur,
            cheapest_country,
            min_price,
            min_price_url,
            most_expensive_country,
            max_price,
            max_price_url
        FROM ReferenceExtremes
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
        ORDER BY arbitrage_pct DESC, reference
    """)

    print("✅ Analytics Views created successfully.")

if __name__ == "__main__":
    # Parse key=value CLI args passed by the job
    args = dict(arg.split("=", 1) for arg in sys.argv[1:])
    env = args["env"]
    catalog = args["catalog"]

    run_analytics_transformation(spark, env, catalog)
