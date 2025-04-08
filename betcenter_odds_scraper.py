# betcenter_odds_scraper.py (Guessing Option Selector)

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
# from selenium.webdriver.support.ui import Select # No longer using Select class
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, WebDriverException,
    StaleElementReferenceException, ElementNotInteractableException,
    ElementClickInterceptedException
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
WAIT_TIMEOUT_SHORT = 15 # Timeout for options/updates
DATA_DIR = "data_archive"
BASE_FILENAME = "betcenter_odds"
DATE_FORMAT = "%Y%m%d"

# --- SELECTORS ---
# Selector for the VISIBLE element you CLICK to OPEN the dropdown
DROPDOWN_TRIGGER_SELECTOR = (By.CSS_SELECTOR, "#filter-league .filter-select") # Confirmed Trigger

# !!! EDUCATED GUESS for OPTION SELECTOR - Based on hidden <option> class !!!
# Assuming the visible options (likely divs or lis) reuse this class. Needs verification if script fails.
DROPDOWN_OPTION_SELECTOR = (By.CSS_SELECTOR, ".filter-select__option") # GUESS: Target elements with this class directly

# --- Selectors for scraping matches (likely still valid) ---
GAMELIST_ITEMS_CONTAINER = (By.CSS_SELECTOR, "#content-container > div > home-page > section > div > games-list > div > gamelist > div")
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
    Scrapes tennis match odds from Betcenter.be/fr/tennis using custom dropdown interaction.
    Excludes ITF tournaments. Uses GUESS for visible option selector.
    """
    driver = setup_driver()
    if driver is None: return pd.DataFrame()

    all_matches_data = []

    try:
        print(f"Navigating to {BASE_URL}...")
        driver.get(BASE_URL)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        wait_short = WebDriverWait(driver, WAIT_TIMEOUT_SHORT)

        print("Pausing briefly for initial page elements...")
        time.sleep(5)

        # --- Find Dropdown Trigger ---
        print(f"Waiting for dropdown TRIGGER element ({DROPDOWN_TRIGGER_SELECTOR[1]})...")
        trigger_element = wait.until(EC.element_to_be_clickable(DROPDOWN_TRIGGER_SELECTOR))
        print("Dropdown trigger found and clickable.")

        # --- Get and Filter Tournament Options ---
        valid_tournament_texts = []
        try:
            print("Clicking dropdown trigger to get options...")
            try: trigger_element.click()
            except (ElementClickInterceptedException, ElementNotInteractableException): driver.execute_script("arguments[0].click();", trigger_element)
            print("Clicked dropdown trigger.")

            # --- Wait for the FIRST VISIBLE OPTION using the GUESSED SELECTOR ---
            print(f"Waiting for the first dropdown OPTION ({DROPDOWN_OPTION_SELECTOR[1]}) to appear...")
            # *** USER MUST VERIFY DROPDOWN_OPTION_SELECTOR if this fails ***
            # Wait for the first element matching the guess to be visible
            wait_short.until(EC.visibility_of_element_located(DROPDOWN_OPTION_SELECTOR))
            print("At least one option element found and visible.")

            # --- Find ALL options matching the selector ---
            print(f"Finding all options ({DROPDOWN_OPTION_SELECTOR[1]})...")
            # It's possible options load slightly after the first, so short pause
            time.sleep(0.5)
            option_elements = driver.find_elements(*DROPDOWN_OPTION_SELECTOR)
            print(f"Found {len(option_elements)} potential option elements. Filtering...")

            for option_element in option_elements:
                try:
                    # Check visibility again for each element just in case
                    if not option_element.is_displayed():
                        continue # Skip hidden elements that might match selector
                    option_text = option_element.text
                    option_text_lower = option_text.lower()
                    if ("atp" in option_text_lower or "challenger" in option_text_lower) and "itf" not in option_text_lower:
                        # Avoid duplicates if text appears multiple times
                        if option_text not in valid_tournament_texts:
                             valid_tournament_texts.append(option_text)
                             print(f"  Adding valid tournament: {option_text}")
                except StaleElementReferenceException: print("  Warning: Option became stale while reading text."); continue
                except Exception as e_opt_filter: print(f"  Warning: Error reading option text: {e_opt_filter}"); continue

            # Close dropdown after getting texts
            print("  Closing dropdown after getting texts...")
            try: driver.find_element(By.TAG_NAME, 'body').click(); time.sleep(0.5)
            except: print("  Warning: Could not click body to close dropdown.")


        except TimeoutException:
             # This means NO element matching DROPDOWN_OPTION_SELECTOR became visible
             print(f"Error: Timed out waiting for ANY options matching '{DROPDOWN_OPTION_SELECTOR[1]}'. Our selector guess is likely wrong.")
             print("Please inspect the elements that appear after clicking the trigger and update DROPDOWN_OPTION_SELECTOR.")
             return pd.DataFrame() # Cannot proceed
        except Exception as e_get_options:
             print(f"Error getting dropdown options: {e_get_options}")
             traceback.print_exc(limit=1)
             return pd.DataFrame() # Cannot proceed

        if not valid_tournament_texts:
            print("No valid ATP or Challenger tournament options found after filtering.")
            return pd.DataFrame()

        print(f"\nFound {len(valid_tournament_texts)} relevant tournaments to scrape.")

        # --- Iterate Through Filtered Tournaments ---
        for i, tournament_text in enumerate(valid_tournament_texts):
            print(f"\n--- Processing Tournament {i+1}/{len(valid_tournament_texts)}: {tournament_text} ---")
            try:
                # --- Open Dropdown Again ---
                print(f"  Re-opening dropdown to select '{tournament_text}'...")
                trigger_element = wait.until(EC.element_to_be_clickable(DROPDOWN_TRIGGER_SELECTOR))
                try: trigger_element.click()
                except (ElementClickInterceptedException, ElementNotInteractableException): driver.execute_script("arguments[0].click();", trigger_element)
                print("  Dropdown trigger clicked.")

                # --- Find and Click Specific Option ---
                print(f"  Waiting for option '{tournament_text}' to be clickable...")
                # Using XPath to find the specific option by text, assuming the class guess is correct
                # If DROPDOWN_OPTION_SELECTOR is just '.some-class', this XPath works.
                option_class = DROPDOWN_OPTION_SELECTOR[1].split('.')[-1] # Extract class name from selector
                option_xpath = f"//*[contains(@class, '{option_class}') and normalize-space()='{tournament_text}']"

                option_to_click = wait.until(EC.element_to_be_clickable((By.XPATH, option_xpath)))
                print(f"  Found option element for '{tournament_text}'. Clicking...")
                try: option_to_click.click()
                except (ElementClickInterceptedException, ElementNotInteractableException): driver.execute_script("arguments[0].click();", option_to_click)
                print("  Option selected.")

                # --- Wait for Page Update ---
                print("  Waiting for match list to update...")
                time.sleep(1.5)
                try:
                    match_list_locator = (By.CSS_SELECTOR, f"{GAMELIST_ITEMS_CONTAINER[1]} {MATCH_ELEMENT_MARKER[1]}")
                    wait_short.until(EC.presence_of_element_located(match_list_locator))
                    print("  Match list updated.")
                except TimeoutException:
                    print(f"  Warning: No match elements found quickly after selecting '{tournament_text}'.")
                    continue

                # --- Scrape Matches ---
                print("  Scraping matches...")
                time.sleep(1.0)
                gamelist_items_container_element = wait.until(EC.presence_of_element_located(GAMELIST_ITEMS_CONTAINER))
                match_elements = gamelist_items_container_element.find_elements(*MATCH_ELEMENT_MARKER)
                print(f"  Found {len(match_elements)} match elements for '{tournament_text}'.")

                # (Match processing loop remains the same)
                for match_index, match_element in enumerate(match_elements):
                    try:
                        p1_name, p2_name, p1_odds, p2_odds = "N/A", "N/A", None, None
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
                        else: print(f"    Warning: Found {len(odds_containers)} odds button containers for match {match_index+1}, expected 2.")
                        if p1_name and p1_name != "N/A" and p2_name and p2_name != "N/A" and p1_odds is not None and p2_odds is not None:
                            match_dict = {'tournament': tournament_text.replace("Tennis - ", "").strip(), 'p1_name': p1_name, 'p2_name': p2_name, 'p1_odds': p1_odds, 'p2_odds': p2_odds}
                            all_matches_data.append(match_dict)
                            if match_index < 5: print(f"    Extracted Match {match_index+1}: {p1_name} ({p1_odds}) vs {p2_name} ({p2_odds})")
                            elif match_index == 5: print("    (Further match extraction logs for this tournament suppressed...)")
                        else: print(f"    Skipping match {match_index+1} due to missing data.")
                    except NoSuchElementException as e_inner: print(f"    Error finding element within match {match_index+1}: {e_inner}. Check relative selectors.")
                    except StaleElementReferenceException: print(f"    Warning: Stale element reference processing match {match_index+1}. Skipping."); continue
                    except Exception as e_match: print(f"    Unexpected error processing match {match_index+1}: {e_match}"); traceback.print_exc(limit=1)

            # (Outer loop error handling remains the same)
            except (ElementNotInteractableException, ElementClickInterceptedException) as e_interact: print(f"Error interacting with dropdown/option for '{tournament_text}': {e_interact}. Skipping."); try: driver.find_element(By.TAG_NAME, 'body').click(); time.sleep(0.5); except: pass; continue
            except TimeoutException: print(f"Error: Timed out waiting for elements during processing of '{tournament_text}'. Skipping."); continue
            except StaleElementReferenceException: print(f"Error: Element became stale while processing '{tournament_text}'. Attempting to continue loop."); continue
            except Exception as e_loop: print(f"Error processing tournament '{tournament_text}': {e_loop}"); traceback.print_exc(limit=1); continue

        print("\nFinished processing all selected tournaments.")
    # --- Outer Error Handling & Cleanup ---
    except TimeoutException: print(f"Error: Timed out on initial page load or finding dropdown trigger. Check selectors/page load."); try: print(f"Page Title at Timeout: {driver.title}"); except Exception: pass
    except NoSuchElementException as e_main: print(f"Error: Could not find critical initial element: {e_main}. Check initial selectors.");
    except Exception as e_outer: print(f"An unexpected error occurred during scraping: {e_outer}"); traceback.print_exc()
    finally:
        if driver: driver.quit(); print("Browser closed.")

    # --- Final DataFrame Creation ---
    # [No changes]
    if not all_matches_data: print("\nNo match data collected from Betcenter."); return pd.DataFrame()
    print(f"\nCollected data for {len(all_matches_data)} matches in total.")
    try:
        final_df = pd.DataFrame(all_matches_data); final_df['scrape_timestamp_utc'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')
        final_df['p1_name'] = final_df['p1_name'].astype(str).str.strip().str.lower(); final_df['p2_name'] = final_df['p2_name'].astype(str).str.strip().str.lower()
        final_df = final_df.drop_duplicates(subset=['tournament', 'p1_name', 'p2_name']); print(f"DataFrame shape after dropping duplicates: {final_df.shape}")
        print("Created final DataFrame:"); print(final_df.head()); return final_df
    except Exception as df_err: print(f"Error creating or processing final DataFrame: {df_err}"); traceback.print_exc(); return pd.DataFrame()

# --- Main Execution ---
# [No changes]
if __name__ == "__main__":
    print("Starting Betcenter.be tennis odds scraping process (Dropdown Strategy)...")
    odds_df = scrape_betcenter_tennis()
    if not odds_df.empty:
        print("\n--- Saving Betcenter Data ---")
        saved_filepath = save_data_to_dated_csv(data=odds_df, base_filename=BASE_FILENAME, output_dir=DATA_DIR)
        if saved_filepath: print(f"Betcenter data saving process completed successfully. File: {saved_filepath}")
        else: print("Betcenter data saving process failed.")
    else: print("\n--- No Betcenter odds data scraped. ---")
