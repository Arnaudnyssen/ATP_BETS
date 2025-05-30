# betcenter_odds_scraper.py (Final Version - Title Case Names - Debug Code Removed)
# Scrapes Betcenter, handles cookies, waits for game tag, filters doubles/live.
# Saves results with Title Case names to match Sackmann data.

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
    StaleElementReferenceException, ElementNotInteractableException,
    ElementClickInterceptedException
)

# Webdriver Manager import
try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    print("Warning: webdriver-manager not found. ChromeDriver might need to be manually managed.")
    ChromeDriverManager = None

# --- Configuration ---
# Set to True when running in GitHub Actions or headless environment
RUN_HEADLESS = True # Defaulting to True for typical CI/CD use
# ---------------------------------------------------

BASE_URL = "https://www.betcenter.be/fr/tennis"
WAIT_TIMEOUT = 30 # General timeout for initial elements
WAIT_TIMEOUT_UPDATE = 20 # Timeout for waiting for the match list update
WAIT_TIMEOUT_OPTIONS_LOOP = 15 # Timeout for waiting for options to reappear in loop
WAIT_TIMEOUT_COOKIE = 10 # Shorter timeout specifically for the cookie banner
DATA_DIR = "data_archive" # Subdirectory for saving CSV files
BASE_FILENAME = "betcenter_odds" # Consistent filename
DATE_FORMAT = "%Y%m%d"
# DEBUG_SCREENSHOT_DIR constant removed

# --- SELECTORS ---
COOKIE_REJECT_BUTTON_ID = "cookiescript_reject"
DROPDOWN_TRIGGER_SELECTOR = (By.CSS_SELECTOR, "#filter-league .filter-select")
DROPDOWN_OPTION_SELECTOR = (By.CSS_SELECTOR, ".filter-select__option")
GAMELIST_ITEMS_CONTAINER = (By.CSS_SELECTOR, "#content-container > div > home-page > section > div > games-list > div > gamelist > div")
MATCH_ELEMENT_MARKER = (By.CSS_SELECTOR, "div.gamelist__event")
GAME_TAG_SELECTOR = (By.TAG_NAME, "game")
PLAYER_1_NAME_SELECTOR = (By.CSS_SELECTOR, "div.game-header--team-name-0")
PLAYER_2_NAME_SELECTOR = (By.CSS_SELECTOR, "div.game-header--team-name-1")
ODDS_BUTTON_CONTAINER_SELECTOR = (By.CSS_SELECTOR, "odd-button")
ODDS_VALUE_RELATIVE_SELECTOR = (By.CSS_SELECTOR, "div.odd-button__value > div")

# --- Helper Functions ---
def setup_driver() -> Optional[webdriver.Chrome]:
    """Sets up the Chrome WebDriver, respecting the RUN_HEADLESS flag."""
    print("Setting up Chrome WebDriver...")
    options = ChromeOptions()
    if RUN_HEADLESS:
        print("Running in HEADLESS mode.")
        options.add_argument("--headless=new")
    else:
        print("Running in VISIBLE mode (browser window will open).")
    options.add_argument("--no-sandbox"); options.add_argument("--disable-dev-shm-usage"); options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1200"); options.add_argument('--log-level=1')
    options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36')
    driver = None; service = None; chromedriver_path = None
    if ChromeDriverManager:
        try:
            print("Attempting to use webdriver-manager..."); chromedriver_path = ChromeDriverManager().install()
            print(f"webdriver-manager found/installed ChromeDriver at: {chromedriver_path}"); service = ChromeService(executable_path=chromedriver_path)
        except Exception as e: print(f"webdriver-manager failed: {e}. Will try system PATH or manual path next."); chromedriver_path = None
    if not chromedriver_path: print("Attempting to use ChromeDriver from system PATH..."); service = None
    try:
        if service: driver = webdriver.Chrome(service=service, options=options); print(f"Using ChromeDriver from: {service.path}")
        else: driver = webdriver.Chrome(options=options); print("Using ChromeDriver assumed to be in system PATH.")
        print("Chrome WebDriver setup successful."); return driver
    except WebDriverException as e:
        if "executable needs to be in PATH" in str(e) or "cannot find chrome binary" in str(e) or "session not created" in str(e):
             print("\n--- ChromeDriver Error ---"); print("Selenium couldn't find or use the ChromeDriver.")
             print("Possible Solutions:"); print("1. Install ChromeDriver using Homebrew: `brew install chromedriver`")
             print("2. Ensure Google Chrome browser is installed and up-to-date."); print("3. Check Chrome & ChromeDriver version compatibility.")
             print(f"   (Error details: {e})"); print("--------------------------\n")
        else: print(f"WebDriver setup failed with an unexpected error: {e}")
        traceback.print_exc(); return None
    except Exception as e: print(f"An unexpected error occurred during WebDriver setup: {e}"); traceback.print_exc(); return None

def parse_odds_value(odds_text: str) -> Optional[float]:
    """Converts odds text (e.g., '1,85') to float."""
    if not odds_text: return None
    try: return float(odds_text.replace(',', '.'))
    except ValueError: print(f"Warning: Could not convert odds text '{odds_text}' to float."); return None

def save_data_to_dated_csv(data: pd.DataFrame, base_filename: str, output_dir: str) -> Optional[str]:
    """Saves the DataFrame to a dated CSV file."""
    if data is None or data.empty: print("No data provided or DataFrame is empty. Nothing to save."); return None
    script_dir = os.path.dirname(os.path.abspath(__file__)); absolute_output_dir = os.path.join(script_dir, output_dir)
    try: os.makedirs(absolute_output_dir, exist_ok=True); print(f"Ensured output directory exists: '{absolute_output_dir}'")
    except OSError as e: print(f"Error creating output directory '{absolute_output_dir}': {e}"); return None
    today_date_str = datetime.now().strftime(DATE_FORMAT); filename = f"{base_filename}_{today_date_str}.csv"
    output_path = os.path.join(absolute_output_dir, filename); print(f"Attempting to save data to: {output_path}")
    try: data.to_csv(output_path, index=False, encoding='utf-8'); print(f"Successfully saved data to: {output_path}"); return output_path
    except Exception as e: print(f"Error saving data to CSV file '{output_path}': {e}"); traceback.print_exc(); return None

# --- save_debug_info function definition removed ---

# --- Main Scraping Function ---
def scrape_betcenter_tennis() -> pd.DataFrame:
    """
    Scrapes tennis match odds from Betcenter.be/fr/tennis. Handles cookie banner.
    """
    driver = setup_driver()
    if driver is None: print("Failed to initialize WebDriver. Exiting."); return pd.DataFrame()

    all_matches_data = []
    start_time = time.time()

    try:
        print(f"Navigating to {BASE_URL}...")
        driver.get(BASE_URL)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        wait_cookie = WebDriverWait(driver, WAIT_TIMEOUT_COOKIE)
        wait_update = WebDriverWait(driver, WAIT_TIMEOUT_UPDATE)
        wait_options_loop = WebDriverWait(driver, WAIT_TIMEOUT_OPTIONS_LOOP)

        # --- Handle Cookie Banner ---
        print("Checking for cookie banner...")
        try:
            cookie_reject_button = wait_cookie.until(EC.element_to_be_clickable((By.ID, COOKIE_REJECT_BUTTON_ID)))
            print(f"Found cookie banner button (ID: {COOKIE_REJECT_BUTTON_ID}). Clicking...")
            try: cookie_reject_button.click()
            except (ElementClickInterceptedException, ElementNotInteractableException):
                print("  Direct click failed for cookie button, trying JavaScript click...")
                driver.execute_script("arguments[0].click();", cookie_reject_button)
            print("Clicked cookie reject/necessary button.")
            time.sleep(1.0)
        except TimeoutException: print("Cookie banner reject button not found or not clickable within timeout.")
        except Exception as e_cookie: print(f"An error occurred trying to handle the cookie banner: {e_cookie}")

        print("Pausing briefly before interacting with page elements...")
        time.sleep(2)

        # --- Find Dropdown Trigger ---
        print(f"Waiting for dropdown TRIGGER element ({DROPDOWN_TRIGGER_SELECTOR[1]})...")
        trigger_element = wait.until(EC.element_to_be_clickable(DROPDOWN_TRIGGER_SELECTOR))
        print("Dropdown trigger found and clickable.")

        # --- Get and Filter Tournament Options (Initial Pass) ---
        valid_tournament_texts = []
        try:
            print("Clicking dropdown trigger to get initial options list...")
            try: trigger_element.click()
            except (ElementClickInterceptedException, ElementNotInteractableException):
                print("  Direct click failed, trying JavaScript click for trigger...")
                driver.execute_script("arguments[0].click();", trigger_element)
            print("Clicked dropdown trigger.")
            print(f"Waiting for the first dropdown OPTION ({DROPDOWN_OPTION_SELECTOR[1]}) to appear...")
            wait.until(EC.visibility_of_element_located(DROPDOWN_OPTION_SELECTOR))
            print("At least one option element found and visible.")
            print(f"Finding all options ({DROPDOWN_OPTION_SELECTOR[1]})...")
            time.sleep(1.5)
            option_elements = driver.find_elements(*DROPDOWN_OPTION_SELECTOR)
            print(f"Found {len(option_elements)} potential option elements. Filtering...")
            for option_element in option_elements:
                try:
                    if not option_element.is_displayed(): continue
                    option_text = option_element.text.strip()
                    if not option_text: continue
                    option_text_lower = option_text.lower()
                    if ("atp" in option_text_lower or "challenger" in option_text_lower) and \
                       "itf" not in option_text_lower and \
                       "double" not in option_text_lower: # Exclude doubles
                        if option_text not in valid_tournament_texts:
                             valid_tournament_texts.append(option_text)
                             print(f"  Adding valid tournament: {option_text}")
                except StaleElementReferenceException: print("  Warning: Option became stale while reading text."); continue
                except Exception as e_opt_filter: print(f"  Warning: Error reading option text: {e_opt_filter}"); continue
            print("  Closing dropdown after getting texts (clicking body)...")
            try: driver.find_element(By.TAG_NAME, 'body').click(); time.sleep(0.5)
            except Exception as e_close: print(f"  Warning: Could not click body to close dropdown ({e_close}).")
        except TimeoutException:
            print(f"Error: Timed out waiting for ANY options matching '{DROPDOWN_OPTION_SELECTOR[1]}'.")
            # Calls to save_debug_info removed
            return pd.DataFrame()
        except Exception as e_get_options:
            print(f"Error getting dropdown options: {e_get_options}")
            # Calls to save_debug_info removed
            traceback.print_exc(limit=1)
            return pd.DataFrame()

        if not valid_tournament_texts: print("No valid ATP or Challenger (non-Double) tournament options found after filtering."); return pd.DataFrame()
        print(f"\nFound {len(valid_tournament_texts)} relevant tournaments to scrape.")

        # --- Iterate Through Filtered Tournaments ---
        for i, tournament_text in enumerate(valid_tournament_texts):
            print(f"\n--- Processing Tournament {i+1}/{len(valid_tournament_texts)}: {tournament_text} ---")
            option_clicked_successfully = False
            try:
                # --- Open Dropdown ---
                print(f"  Re-opening dropdown to select '{tournament_text}'...")
                trigger_element = wait.until(EC.element_to_be_clickable(DROPDOWN_TRIGGER_SELECTOR))
                try: trigger_element.click()
                except (ElementClickInterceptedException, ElementNotInteractableException):
                    print("  Direct click failed, trying JavaScript click for trigger...")
                    driver.execute_script("arguments[0].click();", trigger_element)
                print("  Dropdown trigger clicked.")
                time.sleep(1.0)

                # --- Find and Click Specific Option by Re-finding List ---
                print(f"  Waiting up to {WAIT_TIMEOUT_OPTIONS_LOOP}s for options list to reappear...")
                try:
                    wait_options_loop.until(EC.visibility_of_element_located(DROPDOWN_OPTION_SELECTOR))
                    print("  Options list reappeared. Finding all visible options...")
                    time.sleep(1.0)
                    current_options = driver.find_elements(*DROPDOWN_OPTION_SELECTOR)
                    print(f"  Found {len(current_options)} options in the list.")
                    option_found_in_list = False
                    for current_option in current_options:
                        try:
                            if not current_option.is_displayed(): continue
                            current_option_text = current_option.text.strip()
                            if current_option_text == tournament_text:
                                option_found_in_list = True
                                print(f"  Found matching option element for '{tournament_text}'. Attempting to click...")
                                try: driver.execute_script("arguments[0].scrollIntoViewIfNeeded(true);", current_option); time.sleep(0.3)
                                except Exception as scroll_err: print(f"    Warning: Could not scroll option into view: {scroll_err}")
                                try:
                                    WebDriverWait(driver, 5).until(EC.element_to_be_clickable(current_option))
                                    print(f"    Element '{tournament_text}' deemed clickable. Clicking now...")
                                    current_option.click()
                                    option_clicked_successfully = True; print(f"    Successfully clicked (direct) on option: '{tournament_text}'")
                                except (ElementClickInterceptedException, ElementNotInteractableException, TimeoutException) as click_err:
                                    print(f"    Direct click failed ({type(click_err).__name__}), trying JavaScript click for option...")
                                    driver.execute_script("arguments[0].click();", current_option)
                                    option_clicked_successfully = True; print(f"    Successfully clicked (JS) on option: '{tournament_text}'")
                                except StaleElementReferenceException: print("    ERROR: Option became stale just before clicking."); option_clicked_successfully = False; break
                                print("  Option selected."); break
                        except StaleElementReferenceException: print("    Warning: Option became stale while iterating list."); continue
                        except Exception as inner_opt_err: print(f"    Warning: Error processing an option element: {inner_opt_err}"); continue
                    if not option_found_in_list: print(f"  ERROR: Target option '{tournament_text}' not found in the visible list after re-opening.")
                except TimeoutException:
                    print(f"  ERROR: Timed out ({WAIT_TIMEOUT_OPTIONS_LOOP}s) waiting for options list to reappear.")
                    # Calls to save_debug_info removed
                except Exception as e_refind:
                    print(f"  ERROR: Unexpected error while re-finding/clicking option: {e_refind}")
                    # Calls to save_debug_info removed

                if not option_clicked_successfully:
                    print("  Skipping match scraping for this tournament as option was not clicked successfully.")
                    try: body_element = driver.find_element(By.TAG_NAME, 'body'); body_element.click(); time.sleep(0.5)
                    except Exception as e_close_skip: pass
                    continue

                # --- Wait for PRESENCE of first GAME tag ---
                print(f"  Waiting up to {WAIT_TIMEOUT_UPDATE}s for first 'game' tag to be PRESENT...")
                game_tag_locator_str = f"{GAMELIST_ITEMS_CONTAINER[1]} {MATCH_ELEMENT_MARKER[1]} {GAME_TAG_SELECTOR[1]}"
                game_tag_locator = (By.CSS_SELECTOR, game_tag_locator_str)
                print(f"  (Waiting for locator: '{game_tag_locator[1]}')")
                update_successful = False
                try:
                    wait_update.until(EC.presence_of_element_located(game_tag_locator))
                    print("  First 'game' tag is PRESENT. Assuming match list container is ready.")
                    update_successful = True
                    time.sleep(1.5)
                except TimeoutException:
                    print(f"  TIMEOUT ({WAIT_TIMEOUT_UPDATE}s) waiting for first 'game' tag to be PRESENT.")
                    try: container_after = driver.find_element(*GAMELIST_ITEMS_CONTAINER); print("  --- Container HTML AT TIMEOUT ---"); print(container_after.get_attribute('outerHTML')); print("  -------------------------------")
                    except Exception as e_debug_timeout: print(f"  Error getting container HTML at timeout: {e_debug_timeout}")
                    # Calls to save_debug_info removed
                    update_successful = False
                except Exception as e_wait:
                     print(f"  Unexpected error during 'game' tag wait: {e_wait}")
                     # Calls to save_debug_info removed
                     update_successful = False

                if not update_successful: print("  Skipping match scraping due to update failure/timeout."); continue

                # --- Scrape Matches ---
                print("  Scraping matches...")
                gamelist_items_container_element = wait.until(EC.presence_of_element_located(GAMELIST_ITEMS_CONTAINER))
                match_event_elements = gamelist_items_container_element.find_elements(*MATCH_ELEMENT_MARKER)
                print(f"  Found {len(match_event_elements)} match event elements for '{tournament_text}'.")
                if not match_event_elements: print("  Warning: Update successful, but no match event elements found.")

                processed_count = 0
                for match_index, match_element in enumerate(match_event_elements):
                    try:
                        # --- Filter for UPCOMING matches ---
                        game_element = match_element.find_element(*GAME_TAG_SELECTOR)
                        game_inner_div = game_element.find_element(By.CSS_SELECTOR, "div.game")
                        game_classes = game_inner_div.get_attribute("class")
                        if "game--live" in game_classes: print(f"    Skipping match {match_index+1} as it is live."); continue
                        elif "game--upcoming" not in game_classes: print(f"    Skipping match {match_index+1} as it is not marked as upcoming (Classes: {game_classes})."); continue

                        # --- Scrape upcoming match ---
                        p1_name, p2_name, p1_odds, p2_odds = "N/A", "N/A", None, None
                        try: p1_name_el = match_element.find_element(*PLAYER_1_NAME_SELECTOR); p1_name = " ".join(p1_name_el.text.split())
                        except NoSuchElementException: print(f"    Warning: P1 name not found for upcoming match {match_index+1}.")
                        try: p2_name_el = match_element.find_element(*PLAYER_2_NAME_SELECTOR); p2_name = " ".join(p2_name_el.text.split())
                        except NoSuchElementException: print(f"    Warning: P2 name not found for upcoming match {match_index+1}.")
                        try:
                            odds_containers = match_element.find_elements(*ODDS_BUTTON_CONTAINER_SELECTOR)
                            if len(odds_containers) >= 2:
                                p1_odds_el = odds_containers[0].find_element(*ODDS_VALUE_RELATIVE_SELECTOR); p1_odds = parse_odds_value(p1_odds_el.text)
                                p2_odds_el = odds_containers[1].find_element(*ODDS_VALUE_RELATIVE_SELECTOR); p2_odds = parse_odds_value(p2_odds_el.text)
                            else: print(f"    Warning: Found {len(odds_containers)} odds containers for upcoming match {match_index+1}, expected 2.")
                        except NoSuchElementException: print(f"    Warning: Could not find odds value element for upcoming match {match_index+1}.")
                        except Exception as e_odds_extract: print(f"    Warning: Error extracting odds for upcoming match {match_index+1}: {e_odds_extract}")

                        if p1_name and p1_name != "N/A" and p2_name and p2_name != "N/A" and p1_odds is not None and p2_odds is not None:
                            clean_tournament_name = tournament_text.replace("Tennis - ", "").strip()
                            match_dict = {'tournament': clean_tournament_name, 'p1_name': p1_name, 'p2_name': p2_name, 'p1_odds': p1_odds, 'p2_odds': p2_odds}
                            all_matches_data.append(match_dict)
                            processed_count += 1
                            if processed_count <= 3: print(f"    Extracted Upcoming Match {processed_count}: {p1_name} ({p1_odds}) vs {p2_name} ({p2_odds})")
                            elif processed_count == 4: print("    (Further upcoming match extraction logs suppressed...)")
                        else: print(f"    Skipping upcoming match {match_index+1} due to missing data.")
                    except NoSuchElementException as e_inner: print(f"    Error finding element within match {match_index+1}: {e_inner}. Check relative selectors.")
                    except StaleElementReferenceException: print(f"    Warning: Stale element reference processing match {match_index+1}. Skipping."); continue
                    except Exception as e_match: print(f"    Unexpected error processing match {match_index+1}: {e_match}"); traceback.print_exc(limit=1)
            except Exception as e_loop:
                print(f"An unexpected error occurred processing tournament '{tournament_text}': {e_loop}")
                # Calls to save_debug_info removed
                traceback.print_exc(limit=1)
                print("Attempting to continue with the next tournament...")
                continue
        print("\nFinished processing all selected tournaments.")
    except Exception as e_outer:
        print(f"\nA critical unexpected error occurred during scraping: {e_outer}")
        # Calls to save_debug_info removed
        traceback.print_exc()
    finally:
        if 'driver' in locals() and driver is not None:
            try: driver.quit(); print("Browser closed.")
            except Exception as e_quit: print(f"Error quitting driver: {e_quit}")

    # --- Final DataFrame Creation ---
    if not all_matches_data: print("\nNo match data collected from Betcenter."); return pd.DataFrame()
    print(f"\nCollected data for {len(all_matches_data)} matches in total.")
    end_time = time.time(); print(f"Total scraping time: {end_time - start_time:.2f} seconds")
    try:
        final_df = pd.DataFrame(all_matches_data)
        final_df['scrape_timestamp_utc'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')
        # --- *** Use .title() for Name Standardization *** ---
        final_df['p1_name'] = final_df['p1_name'].astype(str).str.strip().str.title()
        final_df['p2_name'] = final_df['p2_name'].astype(str).str.strip().str.title()
        # ----------------------------------------------------
        final_df = final_df.drop_duplicates(subset=['tournament', 'p1_name', 'p2_name'])
        print(f"DataFrame shape after dropping duplicates: {final_df.shape}")
        print("Created final DataFrame:")
        print(final_df.head())
        return final_df
    except Exception as df_err: print(f"Error creating or processing final DataFrame: {df_err}"); traceback.print_exc(); return pd.DataFrame()

# --- Main Execution Block ---
if __name__ == "__main__":
    print("="*50); print(" Starting Betcenter.be Odds Scraper (Final Version)"); print("="*50)
    # Set RUN_HEADLESS = True for GitHub Actions
    # Set RUN_HEADLESS = False for local debugging
    if not RUN_HEADLESS: print("INFO: Script will open a visible Chrome window.")
    else: print("INFO: Script running in headless mode.")

    odds_df = scrape_betcenter_tennis()
    if not odds_df.empty:
        print("\n--- Saving Results ---")
        saved_filepath = save_data_to_dated_csv(
            data=odds_df,
            base_filename=BASE_FILENAME,
            output_dir=DATA_DIR
        )
        if saved_filepath: print(f"Data saving process completed successfully.\nFile saved to: {os.path.abspath(saved_filepath)}")
        else: print("Data saving process failed.")
    else: print("\n--- No Betcenter odds data scraped. ---")
    print("\nScript finished.")
