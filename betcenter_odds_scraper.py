# betcenter_odds_scraper.py (Debug Match Element Finder)

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
BASE_URL = "https://www.betcenter.be/fr/tennis"
WAIT_TIMEOUT = 30
DATA_DIR = "data_archive"
BASE_FILENAME = "betcenter_odds"
DATE_FORMAT = "%Y%m%d"

# --- SELECTORS (Refined based on user input & relative strategy) ---
GAMELIST_ITEMS_CONTAINER = (By.CSS_SELECTOR, "#content-container > div > home-page > section > div > games-list > div > gamelist > div")
LIST_CHILD_DIVS = (By.XPATH, "./div")
# --- Selectors RELATIVE to a LIST_CHILD_DIV ---
TOURNAMENT_HEADER_MARKER = (By.CSS_SELECTOR, "sport-league-header")
TOURNAMENT_NAME_SELECTOR = (By.CSS_SELECTOR, "div.sport-league-header__label.sport-league-header__label--tennis > span")
# *** UPDATED SELECTOR based on screenshot clues ***
# Attempting to find divs with class 'gamelist_event' as match containers
MATCH_ELEMENT_MARKER = (By.CSS_SELECTOR, "div.gamelist_event")
# --- Selectors RELATIVE to a MATCH_ELEMENT_MARKER (now likely a div.gamelist_event) ---
# These might need adjustment if the structure inside gamelist_event is different from inside <game>
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
    options.add_argument("--window-size=1920,1080"); options.add_argument('--log-level=1')
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
    Scrapes tennis match odds from Betcenter.be/fr/tennis, excluding ITF tournaments.
    Includes debugging for match group divs and uses updated match element selector.
    """
    driver = setup_driver()
    if driver is None: return pd.DataFrame()

    all_matches_data = []
    current_tournament_name = "Unknown"
    skip_current_tournament = False

    try:
        print(f"Navigating to {BASE_URL}...")
        driver.get(BASE_URL)
        wait_general = WebDriverWait(driver, WAIT_TIMEOUT)

        print(f"Waiting for gamelist items container ({GAMELIST_ITEMS_CONTAINER[0]}: {GAMELIST_ITEMS_CONTAINER[1]})...")
        gamelist_items_container_element = wait_general.until(EC.presence_of_element_located(GAMELIST_ITEMS_CONTAINER))
        print("Gamelist items container found.")
        time.sleep(3)

        print(f"Finding direct child elements ({LIST_CHILD_DIVS[0]}: {LIST_CHILD_DIVS[1]}) within the items container...")
        child_elements = gamelist_items_container_element.find_elements(*LIST_CHILD_DIVS)
        print(f"Found {len(child_elements)} direct child elements (potential headers or match groups).")

        if not child_elements:
            print("No child elements found. Check GAMELIST_ITEMS_CONTAINER selector."); return pd.DataFrame()

        for i, child_element in enumerate(child_elements):
            try:
                is_header = False
                # Check if this child contains a tournament header marker
                header_markers = child_element.find_elements(*TOURNAMENT_HEADER_MARKER)

                if header_markers:
                    is_header = True
                    header_element = header_markers[0]
                    # (Keep tournament name extraction logic as before)
                    try:
                        tournament_name_element = header_element.find_element(*TOURNAMENT_NAME_SELECTOR)
                        current_tournament_name = " ".join(tournament_name_element.text.split())
                        if not current_tournament_name:
                             print(f"Warning: Found tournament name element in header {i}, but its text is empty.")
                        elif "itf" in current_tournament_name.lower():
                            skip_current_tournament = True
                            print(f"\n--- Skipping ITF Tournament: {current_tournament_name} (Child Index: {i}) ---")
                        else:
                            skip_current_tournament = False
                            print(f"\n--- Processing Tournament: {current_tournament_name} (Child Index: {i}) ---")
                    except NoSuchElementException:
                        print(f"Warning: Found header marker in child {i} but couldn't find tournament name using selector '{TOURNAMENT_NAME_SELECTOR[1]}'. Using previous: '{current_tournament_name}'")
                    except Exception as e_header:
                        print(f"Warning: Error processing header in child {i}: {e_header}. Using previous: '{current_tournament_name}'")

                # If this child div is NOT a header AND we are NOT skipping the current tournament
                if not is_header and not skip_current_tournament:
                    print(f"  Processing Potential Match Group Div (Child Index: {i}) for tournament '{current_tournament_name}'...")

                    # --- Added Debugging for Match Group Div ---
                    try:
                        group_html = child_element.get_attribute('outerHTML')
                        print(f"\n  --- Debug: HTML for Match Group Div {i} ---")
                        print(group_html[:1500]) # Print more chars for group div
                        print("  --- End Debug ---")
                    except Exception as e_debug_group:
                        print(f"  Warning: Could not get HTML for match group debug: {e_debug_group}")
                    # --- End Debugging ---

                    # Look for individual match elements within this child div using the UPDATED marker
                    match_elements = child_element.find_elements(*MATCH_ELEMENT_MARKER)

                    if match_elements:
                        # *** Success! Found match elements using the new selector ***
                        print(f"  Found {len(match_elements)} match elements (using '{MATCH_ELEMENT_MARKER[1]}') in group div (Child Index: {i}).")
                        for match_index, match_element in enumerate(match_elements):
                            try:
                                p1_name, p2_name, p1_odds, p2_odds = "N/A", "N/A", None, None
                                # Extract data relative to match_element
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
                                        'tournament': current_tournament_name, 'p1_name': p1_name, 'p2_name': p2_name,
                                        'p1_odds': p1_odds, 'p2_odds': p2_odds
                                    }
                                    all_matches_data.append(match_dict)
                                    print(f"    Extracted Match {match_index+1}: {p1_name} ({p1_odds}) vs {p2_name} ({p2_odds})")
                                else:
                                    print(f"    Skipping match {match_index+1} in '{current_tournament_name}' due to missing data (P1: '{p1_name}', P2: '{p2_name}', O1: {p1_odds}, O2: {p2_odds})")
                            except NoSuchElementException as e_inner:
                                print(f"    Error finding element within match {match_index+1} in '{current_tournament_name}': {e_inner}. Check relative selectors (PLAYER_*, ODDS_*) inside '{MATCH_ELEMENT_MARKER[1]}'")
                            except StaleElementReferenceException:
                                print(f"    Warning: Stale element reference processing match {match_index+1}. Skipping.")
                                continue
                            except Exception as e_match:
                                print(f"    Unexpected error processing match {match_index+1}: {e_match}")
                                traceback.print_exc(limit=1)
                    else:
                        # Script failed to find matches even with the new selector
                         print(f"  Warning: No match elements found using selector '{MATCH_ELEMENT_MARKER[1]}' in group div {i}. Check Debug HTML for this div.")


            except StaleElementReferenceException:
                print(f"Warning: Stale element reference processing child div {i}. Skipping this child.")
                continue
            except Exception as e_child_loop:
                 print(f"Error processing child div {i}: {e_child_loop}")
                 traceback.print_exc(limit=1)
                 continue

    # --- Outer Error Handling & Cleanup ---
    except TimeoutException:
        print(f"Error: Timed out waiting for initial elements ({GAMELIST_ITEMS_CONTAINER[1]}). Check selectors and page load state.")
        try: print(f"Page Title at Timeout: {driver.title}")
        except Exception: pass
    except NoSuchElementException as e_main:
         print(f"Error: Could not find primary container element: {e_main}. Check GAMELIST_ITEMS_CONTAINER selector.")
    except Exception as e_outer:
        print(f"An unexpected error occurred during scraping: {e_outer}")
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()
            print("Browser closed.")

    # --- Final DataFrame Creation ---
    # [No changes to DataFrame creation logic]
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
# [No changes to main execution logic]
if __name__ == "__main__":
    print("Starting Betcenter.be tennis odds scraping process...")
    odds_df = scrape_betcenter_tennis()
    if not odds_df.empty:
        print("\n--- Saving Betcenter Data ---")
        saved_filepath = save_data_to_dated_csv(data=odds_df, base_filename=BASE_FILENAME, output_dir=DATA_DIR)
        if saved_filepath: print(f"Betcenter data saving process completed successfully. File: {saved_filepath}")
        else: print("Betcenter data saving process failed.")
    else: print("\n--- No Betcenter odds data scraped. ---")
