import os
import time
import yaml
import random
import pandas as pd
from datetime import date

from utils import handle_cookie_consent, scroll_and_load_all, extract_watch_data, create_folder, init_webdriver


if __name__ == "__main__":

    country_domain = {"France": "com/fr/fr", "Japan": "com/jp/ja",
                        "UK": "com/gb/en", "USA": "com/us/en",
                        "UAE": "com/ae/en", "Switzerland": "com/ch/en"}    

    country_currency = {"France": "EUR", "Japan": "JPY", "UK": "GBP",
                        "USA": "USD", "UAE": "AED", "Switzerland": "CHF"}

    collections = {
        "Luminor": "luminor",
        "Submersible": "submersible",
        "Radiomir": "radiomir",
        "Luminor Due": "luminor-due"
    }

    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)

    xpaths = config["xpaths"]

    for country in country_domain.keys():
        # Create directory named with current date (day-month-year)
        folder_path = os.path.join("Data", date.today().strftime("%d-%m-%Y"), country)
        create_folder(folder_path)

        # Initialize Stealth WebDriver per country to refresh User-Agent
        driver = init_webdriver()
        print(f"WebDriver initialized for {country} successfully!")

        for collection in collections.keys():
            try:
                url = f"https://www.panerai.{country_domain[country]}/collections/watch-collection/{collections[collection]}.html"

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

                print("\n--- SUCCESS ---")
                print(f"Data saved to {file_name}")

            except Exception as e:
                print(f"A critical error occurred during {collection} in {country}: {e}")

            # Randomized sleep to mimic human behavior
            time.sleep(random.uniform(3, 5))

        driver.quit()
