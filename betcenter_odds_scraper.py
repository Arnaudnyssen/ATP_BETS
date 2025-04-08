# betcenter_odds_scraper.py (Dropdown Strategy)

import pandas as pd
import numpy as np
import time
import traceback
from typing import List, Dict, Any, Optional
from datetime import datetime
import os
import re

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select # Required for dropdowns
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, WebDriverException,
    StaleElementReferenceException, ElementNotInteractableException
)

# Webdriver Manager import
try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    print("Warning: webdriver-manager not found.")
    ChromeDriverManager = None

# --- Configuration ---
BASE_URL = "https://www.betcenter.be/fr/tennis"
WAIT_TIMEOUT = 30 # General timeout
WAIT_TIMEOUT_SHORT = 10 # Shorter timeout for page updates after selection
DATA_DIR = "data_archive"
BASE_FILENAME = "betcenter_odds"
DATE_FORMAT = "%Y%m%d"

# --- SELECTORS ---
# Dropdown select element
TOURNAMENT_DROPDOWN_SELECTOR = (By.CSS_SELECTOR, "#filter-league select") # More specific selector for the select
# Container holding the list of matches *after* a tournament is selected
GAMELIST_ITEMS_CONTAINER = (By.CSS_SELECTOR, "#content-container > div > home-page > section > div > games-list > div > gamelist > div")
# Marker for individual match elements within the container
MATCH_ELEMENT_MARKER = (By.CSS_SELECTOR, "div.gamelist_event")
# --- Selectors RELATIVE to a MATCH_ELEMENT_MARKER (div.gamelist_event) ---
PLAYER_1_NAME_SELECTOR = (By.CSS_SELECTOR, "div.game-header--team-name-0")
PLAYER_2_NAME_SELECTOR = (By.CSS_SELECTOR, "div.game-header--team-name-1")
ODDS_BUTTON_CONTAINER_SELECTOR = (By.CSS_SELECTOR, "odd-button")
ODDS_VALUE_RELATIVE_SELECTOR = (By.CSS_SELECTOR, "div.odd-button__value > div")

# --- Helper Functions (setup_driver, parse_odds_value, save_data_to_dated_csv) ---
# [No changes to helper functions - keeping them collapsed for brevity]
def setup_driver() -> Optional[webdriver.Chrome]:
    print("Setting up Chrome WebDriver...")
    options = ChromeOptions()
    options.add_argument("--headless=new"); options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage"); options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1200"); options.add_argument('--log-level=1')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    chromedriver_path_apt = "/usr/bin/chromedriver"; chromedriver_path_wdm = None
    if ChromeDriverManager:
         try: chromedriver_path_wdm = ChromeDriverManager().install(); print(f"webdriver-manager path: {chromedriver_path_wdm}")
         except Exception as e: print(f"Could not get path from webdriver-manager: {e}")
    driver = None; service = None
    try:
        if os.path.exists(chromedriver_path_apt): service = ChromeService(executable_path=chromedriver_path_apt); driver = webdriver.Chrome(service=service, options=options); print(f"Using chromedriver from apt path: {chromedriver_path_apt}")
        elif chromedriver_path_wdm and os.path.exists(chromedriver_path_wdm): service = ChromeService(executable_path=chromedriver_path_wdm); driver = webdriver.Chrome(service=service, options=options); print(f"Using chromedriver from webdriver-manager path: {chromedriver_path_wdm}")
        else: driver = webdriver.Chrome(options=options); print("Chromedriver not found at specific paths, attempting PATH...")
        print("Chrome WebDriver setup successful."); return driver
    except Exception as e: print(f"WebDriver setup failed: {e}"); traceback.print_exc(); return None

def parse_odds_value(odds_text: str) -> Optional[float]:
    if not odds_text: return None
    try: return float(odds_text.replace(',', '.'))
    except ValueError: print(f"Warning: Could not convert odds text '{odds_text}' to float."); return None

def save_data_to_dated_csv(data: pd.DataFrame, base_filename: str, output_dir: str) -> Optional[str]:
    if data is None or data.empty: print("No data provided or DataFrame is empty. Nothing to save."); return None
    script_dir = os.path.dirname(os.path.abspath(__file__)); absolute_output_dir = os.path.join(script_dir, output_dir)
    try: os.makedirs(absolute_output_dir, exist_ok=True); print(f"Ensured output directory exists: '{absolute_output_dir}'")
    except OSError as e: print(f"Error creating output directory '{absolute_output_dir}': {e}"); return None
    today_date_str = datetime.now().strftime(DATE_FORMAT); filename = f"{base_filename}_{today_date_str}.csv"
    output_path = os.path.join(absolute_output_dir, filename); print(f"Attempting to save data to: {output_path}")
    try: data.to_csv(output_path, index=False, encoding='utf-8'); print(f"Successfully saved data to: {output_path}"); return output_path
    except Exception as e: print(f"Error saving data to CSV file '{output_path}': {e}"); traceback.print_exc(); return None

# --- Main Scraping Function ---
def scrape_betcenter_tennis() -> pd.DataFrame:
    """
    Scrapes tennis match odds from Betcenter.be/fr/tennis using the tournament dropdown filter.
    Excludes ITF tournaments.
    """
    driver = setup_driver()
    if driver is None: return pd.DataFrame()

    all_matches_data = []

    try:
        print(f"Navigating to {BASE_URL}...")
        driver.get(BASE_URL)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        wait_short = WebDriverWait(driver, WAIT_TIMEOUT_SHORT)

        # --- Find and Filter Dropdown Options ---
        print(f"Waiting for tournament dropdown ({TOURNAMENT_DROPDOWN_SELECTOR[1]})...")
        select_element = wait.until(EC.visibility_of_element_located(TOURNAMENT_DROPDOWN_SELECTOR))
        select_object = Select(select_element)
        print("Dropdown found.")

        all_options = select_object.options
        valid_tournament_texts = []
        print(f"Found {len(all_options)} options in dropdown. Filtering for ATP/Challenger (excluding ITF)...")
        for option in all_options:
            option_text = option.text
            option_text_lower = option_text.lower()
            # Filter criteria: Must contain ATP or Challenger, must NOT contain ITF
            if ("atp" in option_text_lower or "challenger" in option_text_lower) and "itf" not in option_text_lower:
                valid_tournament_texts.append(option_text)
                print(f"  Adding valid tournament: {option_text}")

        if not valid_tournament_texts:
            print("No valid ATP or Challenger tournaments found in the dropdown.")
            return pd.DataFrame()

        print(f"\nFound {len(valid_tournament_texts)} relevant tournaments to scrape.")

        # --- Iterate Through Filtered Tournaments ---
        for i, tournament_text in enumerate(valid_tournament_texts):
            print(f"\n--- Processing Tournament {i+1}/{len(valid_tournament_texts)}: {tournament_text} ---")
            try:
                # --- Select Tournament from Dropdown ---
                print(f"  Selecting '{tournament_text}' from dropdown...")
                # Re-find element before interacting to avoid staleness
                select_element = wait.until(EC.visibility_of_element_located(TOURNAMENT_DROPDOWN_SELECTOR))
                select_object = Select(select_element)
                select_object.select_by_visible_text(tournament_text)
                print("  Option selected.")

                # --- Wait for Page Update ---
                # Strategy: Wait for the container to potentially become stale OR
                # wait for at least one match element to appear within the container.
                # A short sleep might also be needed for JS updates.
                print("  Waiting for match list to update...")
                time.sleep(1.5) # Small static pause for JS to potentially start updates
                try:
                    # Wait briefly for at least one match element to be present
                    wait_short.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, f"{GAMELIST_ITEMS_CONTAINER[1]} {MATCH_ELEMENT_MARKER[1]}")
                    ))
                    print("  Match list updated (found at least one match element).")
                except TimeoutException:
                    # If no matches appear quickly, maybe the tournament has no matches listed currently
                    print(f"  Warning: No match elements found quickly after selecting '{tournament_text}'. Tournament might be empty or page update slow/failed.")
                    continue # Skip to the next tournament

                # --- Scrape Matches for Selected Tournament ---
                print("  Scraping matches...")
                # Find the container again (might have been replaced)
                gamelist_items_container_element = wait.until(EC.presence_of_element_located(GAMELIST_ITEMS_CONTAINER))
                match_elements = gamelist_items_container_element.find_elements(*MATCH_ELEMENT_MARKER)
                print(f"  Found {len(match_elements)} match elements for '{tournament_text}'.")

                for match_index, match_element in enumerate(match_elements):
                    try:
                        p1_name, p2_name, p1_odds, p2_odds = "N/A", "N/A", None, None
                        # Extract relative data
                        p1_name_el = match_element.find_element(*PLAYER_1_NAME_SELECTOR)
                        p1_name = " ".join(p1_name_el.text.split())
                        p2_name_el = match_element.find_element(*PLAYER_2_NAME_SELECTOR)
                        p2_name = " ".join(p2_name_el.text.split())
                        odds_containers = match_element.find_elements(*ODDS_BUTTON_CONTAINER_SELECTOR)
                        if len(odds_containers) >= 2:
                            p1_odds_el = odds_containers[0].find_element(*ODDS_VALUE_RELATIVE_SELECTOR)
                            p1_odds = parse_odds_value(p1_odds_el.text)
                            p2_odds_el = odds_containers[1].find_element(*ODDS_VALUE_RELATIVE_SELECTOR)
                            p2_odds = parse_odds_value(p2_odds_el.text)
                        else:
                            print(f"    Warning: Found {len(odds_containers)} odds button containers for match {match_index+1}, expected 2.")

                        if p1_name and p1_name != "N/A" and p2_name and p2_name != "N/A" and p1_odds is not None and p2_odds is not None:
                            match_dict = {
                                'tournament': tournament_text.replace("Tennis - ", "").strip(), # Cleaned tournament name
                                'p1_name': p1_name, 'p2_name': p2_name,
                                'p1_odds': p1_odds, 'p2_odds': p2_odds
                            }
                            all_matches_data.append(match_dict)
                            # Limit logging
                            if match_index < 5: # Log first 5 matches per tournament
                                print(f"    Extracted Match {match_index+1}: {p1_name} ({p1_odds}) vs {p2_name} ({p2_odds})")
                            elif match_index == 5:
                                print("    (Further match extraction logs for this tournament suppressed...)")
                        else:
                            print(f"    Skipping match {match_index+1} due to missing data.")
                    except NoSuchElementException as e_inner:
                        print(f"    Error finding element within match {match_index+1}: {e_inner}. Check relative selectors.")
                    except StaleElementReferenceException:
                        print(f"    Warning: Stale element reference processing match {match_index+1}. Skipping.")
                        continue
                    except Exception as e_match:
                        print(f"    Unexpected error processing match {match_index+1}: {e_match}")
                        traceback.print_exc(limit=1)

            except ElementNotInteractableException:
                 print(f"Error: Dropdown option '{tournament_text}' not interactable. Skipping.")
                 continue
            except TimeoutException:
                print(f"Error: Timed out waiting for elements after selecting '{tournament_text}'. Skipping.")
                continue
            except StaleElementReferenceException:
                print(f"Error: Select element became stale while processing '{tournament_text}'. Attempting to continue loop.")
                # Might need to re-find select element at the start of the next loop iteration
                continue
            except Exception as e_loop:
                 print(f"Error processing tournament '{tournament_text}': {e_loop}")
                 traceback.print_exc(limit=1)
                 continue # Try next tournament

        print("\nFinished processing all selected tournaments.")
    # --- Outer Error Handling & Cleanup ---
    except TimeoutException:
        print(f"Error: Timed out waiting for initial page elements (dropdown?). Check selectors and page load state.")
        try: print(f"Page Title at Timeout: {driver.title}")
        except Exception: pass
    except NoSuchElementException as e_main:
         print(f"Error: Could not find primary container element: {e_main}. Check initial selectors.")
    except Exception as e_outer:
        print(f"An unexpected error occurred during scraping: {e_outer}")
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()
            print("Browser closed.")

    # --- Final DataFrame Creation ---
    if not all_matches_data:
        print("\nNo match data collected from Betcenter.")
        return pd.DataFrame()
    print(f"\nCollected data for {len(all_matches_data)} matches in total.")
    try:
        final_df = pd.DataFrame(all_matches_data)
        final_df['scrape_timestamp_utc'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')
        final_df['p1_name'] = final_df['p1_name'].astype(str).str.strip().str.lower()
        final_df['p2_name'] = final_df['p2_name'].astype(str).str.strip().str.lower()
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
    print("Starting Betcenter.be tennis odds scraping process (Dropdown Strategy)...")
    odds_df = scrape_betcenter_tennis()
    if not odds_df.empty:
        print("\n--- Saving Betcenter Data ---")
        saved_filepath = save_data_to_dated_csv(data=odds_df, base_filename=BASE_FILENAME, output_dir=DATA_DIR)
        if saved_filepath: print(f"Betcenter data saving process completed successfully. File: {saved_filepath}")
        else: print("Betcenter data saving process failed.")
    else: print("\n--- No Betcenter odds data scraped. ---")
