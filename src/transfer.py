import os
import sys
from dotenv import load_dotenv
from utils import upload_daily_data_to_databricks

if __name__ == "__main__":
    load_dotenv()

    # 1. Configuration
    LOCAL_DATA_ROOT = "Data"
    VOLUME_BASE = "/Volumes/panerai_project/dev/panerai_data"
    DB_TOKEN = os.getenv("DATABRICKS_TOKEN")
    DB_URL = os.getenv("DATABRICKS_URL")

    # 2. Parameter Handling (Target Countries)
    # Support: python src/transfer.py -c USA,UK,Japan
    if len(sys.argv) > 2 and sys.argv[1] == "-c":
        target_countries_str = sys.argv[2]
        targets = [c.strip() for c in target_countries_str.split(",")]
        print(f"🎯 Targeted transfer for: {targets}")
    elif len(sys.argv) == 1 or (len(sys.argv) > 1 and sys.argv[1] != "-c"):
        targets = None # Passing None tells the uploader to upload everything
        print("🌍 No specific targets specified. Transferring all today's data...")
    else:
        targets = None
        print("⚠️ Invalid arguments. Defaulting to all countries...")

    # 3. Execute Transfer
    print("\n" + "="*60)
    print("🚀 Initiating Databricks Transfer Process...")
    print("="*60)

    upload_daily_data_to_databricks(
        LOCAL_DATA_ROOT,
        VOLUME_BASE,
        DB_TOKEN,
        DB_URL,
        target_countries=targets
    )
