import os
import time
import yaml
import random
import sys
import pandas as pd
from datetime import date
from dotenv import load_dotenv

from utils import handle_cookie_consent, scroll_and_load_all, extract_watch_data, create_folder, init_webdriver

if __name__ == "__main__":
    load_dotenv()

    # 1. Configuration
    country_domain = {
        "France": "com/fr/fr", "Japan": "com/jp/ja",
        "UK": "com/gb/en", "USA": "com/us/en",
        "UAE": "com/ae/en", "Switzerland": "com/ch/en"
    }

    country_currency = {
        "France": "EUR", "Japan": "JPY", "UK": "GBP",
        "USA": "USD", "UAE": "AED", "Switzerland": "CHF"
    }

    collections = {
        "Luminor": "luminor",
        "Submersible": "submersible",
        "Radiomir": "radiomir",
        "Luminor Due": "luminor-due"
    }

    # Load XPaths from config
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)
    xpaths = config["xpaths"]

    # 2. Parameter Handling (Target Countries)
    if len(sys.argv) > 2 and sys.argv[1] == "-c":
        target_countries_str = sys.argv[2]
        targets = [c.strip() for c in target_countries_str.split(",")]
        print(f"🎯 Targeted scrape for: {targets}")
    elif len(sys.argv) == 1 or (len(sys.argv) > 1 and sys.argv[1] != "-c"):
        targets = list(country_domain.keys())
        print("🌍 No valid targets specified. Scraping all countries...")
    else:
        # Handles case where -c was provided but no countries followed it
        targets = list(country_domain.keys())
        print("⚠️ -c flag provided without countries. Scraping all countries...")

    # 3. Scraping Loop
    for country in targets:
        if country not in country_domain:
            print(f" [!] Warning: {country} is not in the configuration. Skipping.")
            continue

        # Create directory named with current date (day-month-year)
        folder_path = os.path.join("Data", date.today().strftime("%d-%m-%Y"), country)
        create_folder(folder_path)

        # Initialize Stealth WebDriver per country to refresh User-Agent
        driver = init_webdriver()
        print(f"\n--- Starting {country} ---")

        for collection, col_slug in collections.items():
            try:
                url = f"https://www.panerai.{country_domain[country]}/collections/watch-collection/{col_slug}.html"
                print(f"Navigating to {url}...")
                driver.get(url)

                # Step 1: Handle Cookies
                handle_cookie_consent(driver, xpaths)

                # Step 2: Scroll and Load All Content
                scroll_and_load_all(driver, xpaths)

                # Step 3: Extract Data
                data = extract_watch_data(driver, xpaths)

                # Step 4: Convert to DataFrame and Save
                df = pd.DataFrame(data)
                df["currency"] = country_currency[country]
                df["country"] = country
                df["date"] = date.today().strftime("%d-%m-%Y")

                file_name = f"panerai_watches_{country}_{collection}_{date.today().strftime('%d-%m-%Y')}.csv"
                full_file_path = os.path.join(folder_path, file_name)

                df.to_csv(full_file_path, index=False, encoding="utf-8-sig")
                print(f"✅ Saved: {file_name}")

            except Exception as e:
                print(f"❌ Critical error occurred during {collection} in {country}: {e}")

            # Randomized sleep to mimic human behavior
            time.sleep(random.uniform(3, 5))

        driver.quit()

    # 4. Final Step: Scraping Complete
    print("\n" + "="*60)
    print("🚀 All targeted scraping completed successfully.")
    print("="*60)
    print("Tip: Run 'python src/transfer.py -c <countries>' to upload the results to Databricks.")
