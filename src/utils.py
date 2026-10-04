import os
import time
import requests
import datetime
from datetime import date

from dotenv import load_dotenv
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
from fake_useragent import UserAgent

load_dotenv()

def init_webdriver():
    # Setup Chrome options
    chrome_options = uc.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--start-maximized")

    # Generate a random user agent to avoid detection
    ua = UserAgent()
    random_user_agent = ua.random
    chrome_options.add_argument(f"user-agent={random_user_agent}")
    print(f"Using User-Agent: {random_user_agent}")

    return uc.Chrome(options=chrome_options)

def handle_cookie_consent(driver, xpaths):
    """Waits for and clicks the 'Tout Accepter' cookie button."""
    try:
        print("Waiting 5 seconds for page assets to load...")
        time.sleep(5)

        print("Looking for 'Tout Accepter' button...")
        cookie_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, xpaths["accepter_button"]))
        )
        cookie_button.click()
        print("Successfully clicked 'Tout Accepter'.")
        time.sleep(3)
    except Exception as e:
        print(f"Cookie button not found or already clicked: {e}")

def scroll_and_load_all(driver, xpaths):
    """Scrolls slowly and clicks 'Load More' button until the end of the page."""
    current_position = 0
    scroll_step = 4500

    while True:
        current_position += scroll_step
        driver.execute_script(f"window.scrollTo(0, {current_position});")

        time.sleep(2)

        try:
            load_more_button = driver.find_element(By.XPATH, xpaths["load_more"])
            if load_more_button.is_displayed():
                print("Found 'Load More' button! Clicking now...")
                driver.execute_script("arguments[0].click();", load_more_button)
                time.sleep(5)
        except:
            pass

        total_height = driver.execute_script("return document.body.scrollHeight")
        if current_position >= total_height:
            print("Reached the bottom of the page. Final verification...")
            time.sleep(4)
            if current_position >= driver.execute_script("return document.body.scrollHeight"):
                print("All products loaded successfully!")
                break
    return True

def extract_watch_data(driver, xpaths):
    """Extracts watch details from the loaded page based on provided XPaths."""
    print("Starting data extraction...")

    items_in = driver.find_elements(By.XPATH, xpaths["items_in"])
    print(f"Found {len(items_in)} product containers.")

    watch_list = []

    for index, item in enumerate(items_in, 1):
        try:
            try:
                url = item.find_element(By.XPATH, xpaths["url"]).get_attribute("href")
            except: url = None

            try:
                title = item.find_element(By.XPATH, xpaths["title"]).text
            except: title = None

            try:
                reference = item.find_element(By.XPATH, xpaths["reference"]).text
            except: reference = None

            try:
                price = item.find_element(By.XPATH, xpaths["price"]).text
            except: price = None

            try:
                tag = item.find_element(By.XPATH, xpaths["tag"]).text
            except: tag = None

            try:
                edition = item.find_element(By.XPATH, xpaths["edition"]).text
            except: edition = None

            try:
                comment = item.find_element(By.XPATH, xpaths["comment"]).text
            except: comment = None

            try:
                total_articles = item.find_element(By.XPATH, xpaths["total_articles"]).text
            except: total_articles = None

            watch_data = {
                "title": title,
                "reference": reference,
                "price": price,
                "url": url,
                "diameter_tag": tag,
                "edition": edition,
                "comment": comment,
                "total_articles": total_articles
            }
            watch_list.append(watch_data)

            if index % 10 == 0:
                print(f"Extracted {index} watches...")

        except Exception as e:
            print(f"Error extracting item {index}: {e}")

    return watch_list

def create_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Created folder: {folder_path}")

def upload_daily_data_to_databricks(base_local_dir, volume_base_path, token, workspace_url, target_countries=None):
    """
    Optimized transfer of today's data folder to Databricks Volume.
    Only uploads files from target_countries if provided, otherwise uploads all.
    Includes professional terminal formatting, connection pooling, and streaming.
    """
    # 1. Folder Setup
    today_folder_name = "30-09-2026"#datetime.datetime.now().strftime("%d-%m-%Y")
    local_folder_path = os.path.join(base_local_dir, today_folder_name)

    print("\n" + "="*60)
    print(f" [UPLOADER] 🔵 Starting transfer to Databricks Volume")
    if target_countries:
        print(f" [UPLOADER] 🎯 Target Countries: {', '.join(target_countries)}")
    else:
        print(f" [UPLOADER] 🌍 Target: All countries")
    print(f" [UPLOADER] 🔵 Source: {local_folder_path}")
    print(f" [UPLOADER] 🔵 Destination: {volume_base_path}/{today_folder_name}")
    print("="*60)

    if not os.path.exists(local_folder_path):
        print(f" [UPLOADER] 🔴 Error: Today's folder {today_folder_name} not found.")
        print("="*60)
        return

    # 2. Connection Optimization: Use a Session for TCP Keep-Alive
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream"
    })

    files_uploaded = 0
    total_files = 0

    # 3. Recursive Upload
    for root, _, files in os.walk(local_folder_path):
        # Check if we are in a country folder
        # Path structure: Data/dd-mm-yyyy/CountryName
        parts = os.path.relpath(root, local_folder_path).split(os.sep)

        if parts[0] != '.' and target_countries:
            current_country = parts[0]
            if current_country not in target_countries:
                continue # Skip this folder if it's not in our target list

        for file in files:
            # Skip hidden/system files
            if file.startswith('.') or file.startswith('Thumbs.db'):
                continue

            # Only upload files > 1KB
            local_file_path = os.path.join(root, file)
            if os.path.getsize(local_file_path) <= 1024:
                continue

            total_files += 1
            relative_path = os.path.relpath(local_file_path, local_folder_path)
            db_relative_path = relative_path.replace('\\', '/')
            full_api_path = f"{volume_base_path}/{today_folder_name}/{db_relative_path}"

            endpoint = f"{workspace_url}/api/2.0/fs/files{full_api_path}"

            try:
                # Stream the file from disk directly to avoid RAM spikes
                with open(local_file_path, 'rb') as f:
                    response = session.put(endpoint, data=f)

                if response.status_code in [204]:
                    print(f" [UPLOADER] ✅ Uploaded: {db_relative_path}")
                    files_uploaded += 1
                else:
                    print(f" [UPLOADER] ❌ Failed: {db_relative_path} (Status: {response.status_code})")
            except Exception as e:
                print(f" [UPLOADER] 🔴 Error uploading {file}: {e}")

    # 4. Final Summary Dashboard
    print("\n" + "-"*60)
    print(f" [UPLOADER] ✨ TRANSFER COMPLETE")
    print(f" [UPLOADER] Total Files: {total_files} | Uploaded: {files_uploaded} | Failed: {total_files - files_uploaded}")
    print("="*60 + "\n")


def check_data_completeness():
    """
    Checks if today's data is already present and complete.
    Returns a list of countries that need scraping.
    """
    # 1. Define requirements
    required_countries = ["France", "UAE", "USA", "UK", "Switzerland", "Japan"]
    min_files_per_country = 4
    min_file_size_kb = 1

    today_folder = date.today().strftime("%d-%m-%Y")
    data_root = "Data"
    today_path = os.path.join(data_root, today_folder)

    print(f"Checking data completeness for: {today_folder}")

    missing_countries = []

    # Check if today's folder even exists
    if not os.path.exists(today_path):
        print(f" [!] Today's folder {today_folder} not found.")
        return required_countries

    # Check each country
    for country in required_countries:
        country_path = os.path.join(today_path, country)

        if not os.path.exists(country_path):
            print(f" [!] Missing folder for country: {country}")
            missing_countries.append(country)
            continue

        # Count valid files (size > 1KB)
        files = [f for f in os.listdir(country_path) if os.path.isfile(os.path.join(country_path, f))]
        valid_files = 0

        for f in files:
            file_path = os.path.join(country_path, f)
            if os.path.getsize(file_path) > (min_file_size_kb * 1024):
                valid_files += 1

        if valid_files < min_files_per_country:
            print(f" [!] Country {country} has only {valid_files} valid files (need {min_files_per_country}).")
            missing_countries.append(country)

    if not missing_countries:
        print(" [✓] All required data for today is already present.")
    else:
        print(f" [!] The following countries need scraping: {', '.join(missing_countries)}")

    return missing_countries
