import os
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent


def init_webdriver():
    # Setup Chrome options
    chrome_options = uc.ChromeOptions()
    # chrome_options.add_argument("--headless")
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
        # print(f"Scrolling... Position: {current_position}px")
        
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

    # Find all product containers
    items_in = driver.find_elements(By.XPATH, xpaths["items_in"])
    print(f"Found {len(items_in)} product containers.")
    
    watch_list = []
    
    for index, item in enumerate(items_in, 1):
        try:
            # Relative extraction
            # URL: Find the <a> tag and get href
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
