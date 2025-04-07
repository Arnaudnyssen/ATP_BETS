# unibet_odds_scraper.py (Using Chrome)

import pandas as pd
import numpy as np
import time
import traceback
from typing import List, Dict, Any, Optional
from datetime import datetime
import os

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, WebDriverException,
    StaleElementReferenceException
)

# Webdriver Manager import
try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    print("Warning: webdriver-manager not found.")
    ChromeDriverManager = None

# --- Configuration ---
BASE_URL = "https://www.unibet.fr/sport/tennis?filter=Top+Paris&subFilter=Vainqueur+du+match" # New URL
WAIT_TIMEOUT = 30 # Increased timeout
DATA_DIR = "data_archive"
BASE_FILENAME = "unibet_odds" # New base filename
DATE_FORMAT = "%Y%m%d"

# --- SELECTORS (Based on analysis - NEED VERIFICATION on live site) ---
# It's better to use more general class-based selectors if possible
# These are educated guesses based on provided info and common patterns
MAIN_LIST_CONTAINER = "#cps-eventsdays-list" # Main container for days/matches
# DAY_CONTAINER = ".eventsdays_content" # If needed to loop through days first
MATCH_CONTAINER = "section._2b8e9" # Found this class on live site around match info, might be more stable
PLAYER_1_NAME = "div._11a5c > div:nth-child(1) ._3658e" # Classes for home team name (needs verification)
PLAYER_2_NAME = "div._11a5c > div:nth-child(2) ._3658e" # Classes for away team name (needs verification)
# Odds are tricky - often in buttons. This selects the odds value within potential buttons/containers
# We expect two main odds values per match for Player1/Player2 win.
ODDS_VALUES = "button span.acae7" # Found this class on odds spans (needs verification)

# --- Helper Functions (setup_driver, parse_odds, save_data_to_dated_csv - remain largely the same) ---

def setup_driver() -> Optional[webdriver.Chrome]:
    """Sets up and returns a headless Chrome WebDriver instance."""
    print("Setting up Chrome WebDriver...")
    options = ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument('--log-level=1')
    # Add user agent to potentially avoid basic bot detection
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')


    chromedriver_path_apt = "/usr/bin/chromedriver"
    chromedriver_path_wdm = None
    if ChromeDriverManager:
         try:
              print("Attempting to install/use ChromeDriver via webdriver-manager...")
              chromedriver_path_wdm = ChromeDriverManager().install()
              print(f"webdriver-manager path: {chromedriver_path_wdm}")
         except Exception as e:
              print(f"Could not get path from webdriver-manager: {e}")

    driver = None
    service = None
    try:
        if os.path.exists(chromedriver_path_apt):
            print(f"Using chromedriver from apt path: {chromedriver_path_apt}")
            service = ChromeService(executable_path=chromedriver_path_apt)
            driver = webdriver.Chrome(service=service, options=options)
        elif chromedriver_path_wdm and os.path.exists(chromedriver_path_wdm):
             print(f"Using chromedriver from webdriver-manager path: {chromedriver_path_wdm}")
             service = ChromeService(executable_path=chromedriver_path_wdm)
             driver = webdriver.Chrome(service=service, options=options)
        else:
             print("Chromedriver not found at specific paths, attempting PATH...")
             driver = webdriver.Chrome(options=options)

        print("Chrome WebDriver setup successful.")
        return driver
    except WebDriverException as e:
        print(f"WebDriver setup failed: {e}"); traceback.print_exc();
        if driver: driver.quit()
        return None
    except Exception as e:
         print(f"An unexpected error occurred during Chrome WebDriver setup: {e}"); traceback.print_exc();
         if driver: driver.quit()
         return None

def parse_odds(odds_text: str) -> Optional[float]:
    """Converts odds text (e.g., '1,85') to float, handling commas."""
    if not odds_text: return None
    try:
        # Replace comma with dot for float conversion
        return float(odds_text.replace(',', '.'))
    except ValueError:
        print(f"Warning: Could not convert odds text '{odds_text}' to float.")
        return None

def save_data_to_dated_csv(data: pd.DataFrame, base_filename: str, output_dir: str) -> Optional[str]:
    """Saves the DataFrame to a dated CSV file."""
    if data is None or data.empty:
        print("No data provided or DataFrame is empty. Nothing to save.")
        return None
    script_dir = os.path.dirname(os.path.abspath(__file__))
    absolute_output_dir = os.path.join(script_dir, output_dir)
    try:
        os.makedirs(absolute_output_dir, exist_ok=True)
        print(f"Ensured output directory exists: '{absolute_output_dir}'")
    except OSError as e:
        print(f"Error creating output directory '{absolute_output_dir}': {e}")
        return None

    today_date_str = datetime.now().strftime(DATE_FORMAT)
    filename = f"{base_filename}_{today_date_str}.csv"
    output_path = os.path.join(absolute_output_dir, filename)
    print(f"Attempting to save data to: {output_path}")
    try:
        data.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Successfully saved data to: {output_path}")
        return output_path
    except Exception as e:
        print(f"Error saving data to CSV file '{output_path}': {e}")
        traceback.print_exc()
        return None

# --- New Scraper Function for Unibet ---
def scrape_unibet_matches() -> pd.DataFrame:
    """
    Navigates Unibet.fr tennis page, scrapes match odds,
    and returns a consolidated DataFrame.
    """
    driver = setup_driver()
    if driver is None:
        return pd.DataFrame()

    all_matches_data = []
    try:
        print(f"Navigating to {BASE_URL}...")
        driver.get(BASE_URL)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)

        # Wait for the main container holding matches to be visible
        print(f"Waiting for main match list container ('{MAIN_LIST_CONTAINER}') to be visible...")
        try:
            main_container = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, MAIN_LIST_CONTAINER)))
            print("Main container found and visible.")
            time.sleep(2) # Allow dynamic content within to load
        except TimeoutException:
             print(f"Error: Timed out waiting for main container: {MAIN_LIST_CONTAINER}. Page structure might have changed.")
             # Try capturing page source for debugging before quitting
             try:
                  print("Page source at time of failure:")
                  print(driver.page_source[:2000]) # Print first 2000 chars
             except:
                  pass
             return pd.DataFrame()


        # Find all match containers within the main list
        print(f"Finding all match containers using selector: '{MATCH_CONTAINER}'...")
        match_elements = main_container.find_elements(By.CSS_SELECTOR, MATCH_CONTAINER)
        print(f"Found {len(match_elements)} potential match containers.")

        if not match_elements:
            print("No match containers found. Check the MATCH_CONTAINER selector.")
            return pd.DataFrame()

        # Iterate through each match container
        for index, match_element in enumerate(match_elements):
            p1_name, p2_name, p1_odds, p2_odds = "N/A", "N/A", None, None
            tournament_name = "N/A" # Placeholder, try to extract if possible

            try:
                # Scroll match element into view (optional, can help)
                # driver.execute_script("arguments[0].scrollIntoViewIfNeeded(true);", match_element)
                # time.sleep(0.2)

                # Extract Player Names relative to the match_element
                try:
                    p1_name_element = match_element.find_element(By.CSS_SELECTOR, PLAYER_1_NAME)
                    p1_name = " ".join(p1_name_element.text.split())
                except NoSuchElementException:
                    print(f"Warning: Player 1 name not found in match {index+1}")

                try:
                    p2_name_element = match_element.find_element(By.CSS_SELECTOR, PLAYER_2_NAME)
                    p2_name = " ".join(p2_name_element.text.split())
                except NoSuchElementException:
                    print(f"Warning: Player 2 name not found in match {index+1}")

                # Extract Odds relative to the match_element
                try:
                    odds_elements = match_element.find_elements(By.CSS_SELECTOR, ODDS_VALUES)
                    if len(odds_elements) >= 2:
                        # Assume first is P1 odds, second is P2 odds
                        p1_odds = parse_odds(odds_elements[0].text)
                        p2_odds = parse_odds(odds_elements[1].text)
                    else:
                        print(f"Warning: Found {len(odds_elements)} odds values in match {index+1}, expected 2.")

                except NoSuchElementException:
                     print(f"Warning: Odds values not found using selector '{ODDS_VALUES}' in match {index+1}")


                # Basic validation - only add if we have names and odds
                if p1_name != "N/A" and p2_name != "N/A" and p1_odds is not None and p2_odds is not None:
                    match_dict = {
                        'p1_name': p1_name,
                        'p2_name': p2_name,
                        'p1_odds': p1_odds,
                        'p2_odds': p2_odds,
                        'tournament': tournament_name # Add tournament if extractable
                    }
                    all_matches_data.append(match_dict)
                    print(f"  Extracted Match {index+1}: {p1_name} ({p1_odds}) vs {p2_name} ({p2_odds})")
                else:
                    print(f"  Skipping match {index+1} due to missing data (Name1: {p1_name}, Name2: {p2_name}, Odds1: {p1_odds}, Odds2: {p2_odds})...")

            except StaleElementReferenceException:
                print(f"Warning: Stale element reference processing match {index+1}. Skipping.")
                continue
            except Exception as e_inner:
                print(f"Unexpected error processing match {index+1}: {e_inner}")
                traceback.print_exc(limit=1)

    except Exception as e_outer:
        print(f"An unexpected error occurred during scraping: {e_outer}")
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()
            print("Browser closed.")

    # --- Final DataFrame Creation ---
    if not all_matches_data:
        print("\nNo match data collected from Unibet.")
        return pd.DataFrame()

    print(f"\nCollected data for {len(all_matches_data)} matches in total.")
    try:
        final_df = pd.DataFrame(all_matches_data)
        final_df['scrape_timestamp_utc'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')
        # Clean player names (strip whitespace, lowercase for potential matching later)
        final_df['p1_name'] = final_df['p1_name'].str.strip().str.lower()
        final_df['p2_name'] = final_df['p2_name'].str.strip().str.lower()
        # Drop duplicates based on cleaned names (and tournament if available)
        final_df = final_df.drop_duplicates(subset=['tournament', 'p1_name', 'p2_name'])
        print(f"DataFrame shape after dropping duplicates: {final_df.shape}")
        print("Created final DataFrame:")
        print(final_df.head())
        return final_df
    except Exception as df_err:
        print(f"Error creating or processing final DataFrame: {df_err}")
        traceback.print_exc()
        return pd.DataFrame()

# --- Main Execution ---
if __name__ == "__main__":
    print("Starting Unibet.fr odds scraping process...")
    odds_df = scrape_unibet_matches()

    if not odds_df.empty:
        print("\n--- Saving Unibet Data ---")
        saved_filepath = save_data_to_dated_csv(
            data=odds_df,
            base_filename=BASE_FILENAME, # Uses "unibet_odds"
            output_dir=DATA_DIR
        )
        if saved_filepath:
             print(f"Unibet data saving process completed successfully. File: {saved_filepath}")
        else:
             print("Unibet data saving process failed.")
    else:
        print("\n--- No Unibet odds data scraped. ---")
