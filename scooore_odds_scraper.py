# scooore_odds_scraper.py (Using Chrome, Parse aria-label, Increased Timeout)

import pandas as pd
import numpy as np
import time
import traceback
import re # For parsing aria-label
from typing import List, Dict, Any, Optional, Tuple
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
BASE_URL = "https://www.scooore.be/fr/sports/sports-hub/tennis"
WAIT_TIMEOUT = 45 # Keep increased timeout
DATA_DIR = "data_archive"
BASE_FILENAME = "scooore_odds"
DATE_FORMAT = "%Y%m%d"

# --- SELECTORS (Revised Strategy) ---
MAIN_CONTENT_ID = "KambiBC-content" # For initial page load wait
# Selector for the UL containing all matches for a tournament
# Assuming the old MATCH_LIST_CONTAINER_SELECTOR is correct for the UL
MATCH_LIST_CONTAINER_SELECTOR = (By.XPATH, "//*[@id='KambiBC-content']//section/section/section/section/section/ul")
# Selector for individual match items (li) relative to the container UL
MATCH_ITEM_SELECTOR = (By.XPATH, "./li")
# Selector for player names within an li (Keep as fallback/verification)
# Using contains(@class) is generally better than absolute paths
PLAYER_NAME_SELECTOR = (By.XPATH, ".//div[contains(@class, 'participant-name')]") # Simplified selector - find divs with this class
# Selector for the elements (likely buttons or divs containing them) that hold the odds AND the aria-label
# This targets the button directly, assuming aria-label might be on it or its child div.
# We will get the aria-label from the button's child div if necessary.
# Let's try finding the button first based on common Kambi structure patterns.
# This looks for button elements that are likely odds buttons within the match item.
ODDS_BUTTON_SELECTOR = (By.XPATH, ".//div[contains(@class, 'outcome-button')]//button")
# If the above is too generic, maybe target based on the structure you found:
# ODDS_BUTTON_SELECTOR = (By.XPATH, ".//div/div/div[1]/div/div/button") # Relative to li

# --- Helper Functions ---

def setup_driver() -> Optional[webdriver.Chrome]:
    """Sets up and returns a headless Chrome WebDriver instance."""
    # (No changes from previous version scooore_scraper_debug_v1)
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
    """Converts odds text (e.g., '2.60' or '2,60') to float."""
    if not odds_text: return None
    try: return float(odds_text.replace(',', '.'))
    except ValueError: print(f"Warning: Could not convert odds text '{odds_text}' to float."); return None

def parse_aria_label(label: str) -> Optional[Tuple[str, str, str, float]]:
    """
    Parses the Scooore aria-label string to extract player names and odds.
    Example: "Pari sur Sonego, Lorenzo vs Martinez, Pedro - Cotes du match - Martinez, Pedro  à 2.60"
    Returns: Tuple (p1_name, p2_name, target_player_name, odds_value) or None if parsing fails.
    """
    if not label: return None
    try:
        # Regex to capture names and odds value
        # Adjust regex if the format varies significantly
        pattern = r"Pari sur (.*?) vs (.*?) - Cotes du match - (.*?) à ([\d,.]+)"
        match = re.search(pattern, label)
        if match:
            p1_name = match.group(1).strip()
            p2_name = match.group(2).strip()
            target_player_name = match.group(3).strip()
            odds_str = match.group(4).strip()
            odds_value = parse_odds_value(odds_str)

            if p1_name and p2_name and target_player_name and odds_value is not None:
                # Basic cleaning/standardization (e.g., remove extra spaces)
                p1_name = ' '.join(p1_name.split())
                p2_name = ' '.join(p2_name.split())
                target_player_name = ' '.join(target_player_name.split())
                return p1_name, p2_name, target_player_name, odds_value
        else:
            print(f"Warning: Regex did not match aria-label format: '{label}'")
            return None
    except Exception as e:
        print(f"Error parsing aria-label '{label}': {e}")
        return None
    return None

def save_data_to_dated_csv(data: pd.DataFrame, base_filename: str, output_dir: str) -> Optional[str]:
    """Saves the DataFrame to a dated CSV file."""
    # (No changes from previous version scooore_scraper_debug_v1)
    if data is None or data.empty: print("No data provided or DataFrame is empty. Nothing to save."); return None
    script_dir = os.path.dirname(os.path.abspath(__file__)); absolute_output_dir = os.path.join(script_dir, output_dir)
    try: os.makedirs(absolute_output_dir, exist_ok=True); print(f"Ensured output directory exists: '{absolute_output_dir}'")
    except OSError as e: print(f"Error creating output directory '{absolute_output_dir}': {e}"); return None
    today_date_str = datetime.now().strftime(DATE_FORMAT); filename = f"{base_filename}_{today_date_str}.csv"
    output_path = os.path.join(absolute_output_dir, filename); print(f"Attempting to save data to: {output_path}")
    try: data.to_csv(output_path, index=False, encoding='utf-8'); print(f"Successfully saved data to: {output_path}"); return output_path
    except Exception as e: print(f"Error saving data to CSV file '{output_path}': {e}"); traceback.print_exc(); return None

# --- Scraper Function (Revised Strategy) ---
def scooore_odds_scraper_refactored(driver: webdriver.Chrome, wait: WebDriverWait, tournament_name: str) -> List[Dict[str, Any]]:
    """
    Scrapes odds for the currently selected tournament using Chrome.
    Prioritizes parsing aria-label from odds buttons/elements.
    Returns a list of dictionaries representing matches. Empty list on failure.
    """
    matches_data = []
    print(f"\n--- Scraping Tournament: {tournament_name} ---")
    try:
        print(f"Waiting for match list container (selector: {MATCH_LIST_CONTAINER_SELECTOR[1]})...")
        match_list_container = wait.until(EC.presence_of_element_located(MATCH_LIST_CONTAINER_SELECTOR))
        print("Match list container found.")
        time.sleep(2.0) # Allow dynamic content within list to settle
        match_elements = match_list_container.find_elements(*MATCH_ITEM_SELECTOR)
        print(f"Found {len(match_elements)} potential match list items (li).")

        if not match_elements:
            print(f"No match items (li) found for {tournament_name}.")
            return []

        # Iterate through each match item (li)
        for index, match_element in enumerate(match_elements):
            print(f"  Processing Match Item {index + 1}...")
            match_info = {'tournament': tournament_name}
            p1_name_fallback = "N/A"
            p2_name_fallback = "N/A"

            # Fallback: Try getting player names via structure first
            try:
                player_name_elements = match_element.find_elements(*PLAYER_NAME_SELECTOR)
                if len(player_name_elements) >= 2:
                    p1_name_fallback = " ".join(player_name_elements[0].text.split())
                    p2_name_fallback = " ".join(player_name_elements[1].text.split())
                    print(f"    Fallback Names: P1='{p1_name_fallback}', P2='{p2_name_fallback}'")
                else:
                     print(f"    Warning: Found {len(player_name_elements)} player name elements using fallback selector.")
            except Exception as e_name:
                print(f"    Warning: Error getting fallback player names: {e_name}")

            # Primary Strategy: Find odds buttons/divs and parse aria-label
            odds_extracted_count = 0
            try:
                # Find elements likely containing odds and aria-label
                # Adjust ODDS_BUTTON_SELECTOR if needed based on inspection
                odds_elements = match_element.find_elements(*ODDS_BUTTON_SELECTOR)
                print(f"    Found {len(odds_elements)} potential odds elements.")

                if len(odds_elements) >= 2:
                    for odd_element in odds_elements[:2]: # Process the first two found
                        aria_label_value = None
                        # Try getting aria-label from the button itself or its immediate child div
                        try:
                            # Check button first
                            aria_label_value = odd_element.get_attribute('aria-label')
                            if not aria_label_value:
                                # If not on button, check immediate child div (common pattern)
                                child_div = odd_element.find_element(By.XPATH, "./div")
                                aria_label_value = child_div.get_attribute('aria-label')
                        except Exception:
                            print(f"    Warning: Could not find aria-label on odd element or its child div.")
                            continue # Skip this odd element if no label found

                        if aria_label_value:
                            print(f"    Parsing aria-label: '{aria_label_value}'")
                            parsed_data = parse_aria_label(aria_label_value)
                            if parsed_data:
                                p1_name_aria, p2_name_aria, target_player_aria, odds_value_aria = parsed_data
                                print(f"      Parsed: P1='{p1_name_aria}', P2='{p2_name_aria}', Target='{target_player_aria}', Odds={odds_value_aria}")

                                # Store names from the first successfully parsed label
                                if 'p1_name' not in match_info:
                                    match_info['p1_name'] = p1_name_aria
                                    match_info['p2_name'] = p2_name_aria

                                # Assign odds to the correct player based on target_player_aria
                                # Compare target_player_aria with p1_name_aria (case-insensitive)
                                if target_player_aria.lower() == p1_name_aria.lower():
                                    if 'p1_odds' not in match_info:
                                        match_info['p1_odds'] = odds_value_aria
                                        odds_extracted_count += 1
                                    else: print(f"    Warning: P1 odds already found for this match.")
                                elif target_player_aria.lower() == p2_name_aria.lower():
                                     if 'p2_odds' not in match_info:
                                        match_info['p2_odds'] = odds_value_aria
                                        odds_extracted_count += 1
                                     else: print(f"    Warning: P2 odds already found for this match.")
                                else:
                                    print(f"    Warning: Target player '{target_player_aria}' in aria-label doesn't match P1 or P2 ('{p1_name_aria}', '{p2_name_aria}').")
                            else:
                                print(f"    Warning: Failed to parse aria-label.")
                        else:
                             print(f"    Warning: Found odd element but aria-label attribute was empty or missing.")
                else:
                     print(f"    Warning: Found {len(odds_elements)} odds elements, expected at least 2.")

            except StaleElementReferenceException:
                print(f"    Warning: Stale element reference trying to find odds elements. Skipping odds for this match.")
            except Exception as e_odds:
                print(f"    Error finding/parsing odds elements: {e_odds}")

            # Validation: Check if we got both odds and names
            if 'p1_odds' in match_info and 'p2_odds' in match_info:
                # Use names from aria-label if available, else use fallback
                if 'p1_name' not in match_info: match_info['p1_name'] = p1_name_fallback
                if 'p2_name' not in match_info: match_info['p2_name'] = p2_name_fallback

                if match_info['p1_name'] != "N/A" and match_info['p2_name'] != "N/A":
                    print(f"    SUCCESS: Extracted Match {index + 1}: {match_info['p1_name']} ({match_info.get('p1_odds')}) vs {match_info['p2_name']} ({match_info.get('p2_odds')})")
                    matches_data.append(match_info)
                else:
                    print(f"    Skipping match {index + 1} due to missing player names (fallback failed?).")
            else:
                print(f"    Skipping match {index + 1} due to missing odds (extracted {odds_extracted_count}/2).")


    except TimeoutException:
        print(f"Error: Timed out waiting for match list container for {tournament_name}.")
    except NoSuchElementException:
        print(f"Error: Could not find primary match list container for {tournament_name}.")
    except Exception as e_outer:
        print(f"An unexpected error occurred while scraping {tournament_name}: {e_outer}")
        traceback.print_exc()

    print(f"--- Finished scraping {tournament_name}, extracted {len(matches_data)} valid matches. ---")
    return matches_data


# --- Main Scraping Orchestrator ---
def scrape_all_scoore_tournaments(scrape_challenger=False) -> pd.DataFrame:
    """
    Navigates Scooore, finds ATP or Challenger tournaments, scrapes odds for each,
    and returns a consolidated DataFrame. Uses Chrome.
    """
    # (No changes from previous version scooore_scraper_debug_v1 in this function)
    # It calls the updated scooore_odds_scraper_refactored function now.
    driver = setup_driver()
    if driver is None: return pd.DataFrame()
    all_matches_data = []
    try:
        print(f"Navigating to {BASE_URL}...")
        driver.get(BASE_URL)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        print("Page navigation initiated. Waiting for main content...")
        try: print(f"Page Title after load attempt: {driver.title}")
        except Exception as debug_err: print(f"Error getting debug info (title/source): {debug_err}")
        print(f"Waiting up to {WAIT_TIMEOUT}s for element with ID '{MAIN_CONTENT_ID}' to be present...")
        wait.until(EC.presence_of_element_located((By.ID, MAIN_CONTENT_ID)))
        print(f"Element '{MAIN_CONTENT_ID}' found. Main content area loaded.")
        time.sleep(1)
        target_category_text = "Challenger" if scrape_challenger else "ATP"
        print(f"Targeting {target_category_text} tournaments.")
        if scrape_challenger:
            try:
                print("Attempting to navigate to Challenger tournaments...")
                challenger_button = wait.until(EC.element_to_be_clickable(CHALLENGER_CATEGORY_SELECTOR))
                driver.execute_script("arguments[0].scrollIntoView(true);", challenger_button); time.sleep(0.5)
                challenger_button.click(); print("Clicked Challenger category selector.")
                wait.until(EC.presence_of_element_located(TOURNAMENT_LINK_SELECTOR)); print("Waited for Challenger tournaments to load.")
                time.sleep(1.5)
            except (NoSuchElementException, TimeoutException) as e: print(f"Error finding or clicking Challenger category selector: {e}. Scraping default (ATP).")
        tournament_elements_info = []
        try:
            print(f"Waiting for tournament links (selector: {TOURNAMENT_LINK_SELECTOR[1]})...")
            tournament_elements_list = wait.until(EC.visibility_of_all_elements_located(TOURNAMENT_LINK_SELECTOR))
            print(f"Found {len(tournament_elements_list)} tournament links/buttons.")
            if not tournament_elements_list: print("No tournament links found. Exiting."); return pd.DataFrame()
            for i in range(len(tournament_elements_list)):
                 try:
                     current_elements = wait.until(EC.visibility_of_all_elements_located(TOURNAMENT_LINK_SELECTOR))
                     if i < len(current_elements):
                         el = current_elements[i]; name = el.text.strip()
                         if name: tournament_elements_info.append({'index': i, 'name': name})
                         else: print(f"Warning: Tournament element at index {i} has empty text.")
                     else: print(f"Warning: Index {i} out of bounds after re-finding elements."); break
                 except StaleElementReferenceException: print(f"Warning: Tournament element at index {i} became stale while getting name. Trying to continue."); continue
            print(f"Identified {len(tournament_elements_info)} tournaments with names to process.")
            processed_tournaments = 0
            for tourney_info in tournament_elements_info:
                tourney_index = tourney_info['index']; tourney_name = tourney_info['name']
                print(f"\nProcessing tournament {processed_tournaments + 1}/{len(tournament_elements_info)}: {tourney_name}")
                try:
                    current_tournament_elements = wait.until(EC.visibility_of_all_elements_located(TOURNAMENT_LINK_SELECTOR))
                    if tourney_index < len(current_tournament_elements):
                        element_to_click = current_tournament_elements[tourney_index]
                        print(f"Clicking tournament element for '{tourney_name}'...")
                        try: element_to_click.click()
                        except Exception as click_err: print(f"Direct click failed ({click_err}), trying JavaScript click..."); driver.execute_script("arguments[0].click();", element_to_click)
                        wait.until(EC.presence_of_element_located(MATCH_LIST_CONTAINER_SELECTOR)); print("Tournament page loaded, starting scrape...")
                        # *** Calls the updated function ***
                        tournament_data = scooore_odds_scraper_refactored(driver, wait, tourney_name)
                        all_matches_data.extend(tournament_data); processed_tournaments += 1
                        print("Navigating back to main Tennis Hub URL..."); driver.get(BASE_URL)
                        wait.until(EC.presence_of_element_located((By.ID, MAIN_CONTENT_ID))); print("Reloaded main page.")
                        time.sleep(1.5)
                        if scrape_challenger:
                             try:
                                 challenger_button = wait.until(EC.element_to_be_clickable(CHALLENGER_CATEGORY_SELECTOR))
                                 driver.execute_script("arguments[0].scrollIntoView(true);", challenger_button); time.sleep(0.5)
                                 challenger_button.click()
                                 wait.until(EC.presence_of_element_located(TOURNAMENT_LINK_SELECTOR)); print("Re-selected Challenger category.")
                                 time.sleep(1.5)
                             except Exception as e_reclick: print(f"Warning: Failed to re-click Challenger category: {e_reclick}. May affect next iteration.")
                    else: print(f"Warning: Tournament index {tourney_index} out of bounds ({len(current_tournament_elements)} elements found). Skipping '{tourney_name}'."); continue
                except StaleElementReferenceException:
                    print(f"Error: Tournament link for '{tourney_name}' became stale before/during processing. Skipping.")
                    try: # Attempt recovery
                        driver.get(BASE_URL); wait.until(EC.presence_of_element_located((By.ID, MAIN_CONTENT_ID)))
                        if scrape_challenger: challenger_button = wait.until(EC.element_to_be_clickable(CHALLENGER_CATEGORY_SELECTOR)); challenger_button.click(); time.sleep(1.5); wait.until(EC.presence_of_element_located(TOURNAMENT_LINK_SELECTOR))
                        print("Refreshed main page after stale element error.")
                    except Exception as refresh_err: print(f"CRITICAL: Failed to refresh page after stale element error: {refresh_err}. Aborting loop."); break
                    continue
                except Exception as loop_error:
                    print(f"Error processing tournament '{tourney_name}': {loop_error}"); traceback.print_exc(limit=2)
                    try: # Attempt recovery
                        driver.get(BASE_URL); wait.until(EC.presence_of_element_located((By.ID, MAIN_CONTENT_ID)))
                        if scrape_challenger: challenger_button = wait.until(EC.element_to_be_clickable(CHALLENGER_CATEGORY_SELECTOR)); challenger_button.click(); time.sleep(1.5); wait.until(EC.presence_of_element_located(TOURNAMENT_LINK_SELECTOR))
                        print("Navigated back to main page after loop error.")
                    except Exception as nav_err: print(f"CRITICAL: Failed to navigate back after loop error: {nav_err}. Aborting loop."); break
        except (NoSuchElementException, TimeoutException) as e: print(f"Error: Could not find initial tournament links list: {e}")
        except Exception as e: print(f"An unexpected error occurred finding/looping tournaments: {e}"); traceback.print_exc()
    finally:
        if driver: driver.quit(); print("Browser closed.")
    # --- Final DataFrame Creation ---
    if not all_matches_data: print("\nNo match data collected from any tournament."); return pd.DataFrame()
    print(f"\nCollected data for {len(all_matches_data)} matches in total.")
    try:
        final_df = pd.DataFrame(all_matches_data)
        final_df['scrape_timestamp_utc'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')
        # Apply consistent cleaning (lowercase) for merging later
        final_df['p1_name'] = final_df['p1_name'].astype(str).str.strip().str.lower()
        final_df['p2_name'] = final_df['p2_name'].astype(str).str.strip().str.lower()
        final_df = final_df.drop_duplicates(subset=['tournament', 'p1_name', 'p2_name'])
        print(f"DataFrame shape after dropping duplicates: {final_df.shape}")
        print("Created final DataFrame:"); print(final_df.head())
        return final_df
    except Exception as df_err: print(f"Error creating or processing final DataFrame: {df_err}"); traceback.print_exc(); return pd.DataFrame()

# --- Main Execution ---
if __name__ == "__main__":
    # (No changes from previous version scooore_scraper_debug_v1)
    print("Starting Scooore odds scraping process (using Chrome)...")
    odds_df_atp = scrape_all_scoore_tournaments(scrape_challenger=False)
    time.sleep(2)
    odds_df_challenger = scrape_all_scoore_tournaments(scrape_challenger=True)
    if not odds_df_atp.empty and not odds_df_challenger.empty:
        odds_df = pd.concat([odds_df_atp, odds_df_challenger], ignore_index=True)
        odds_df = odds_df.drop_duplicates(subset=['tournament', 'p1_name', 'p2_name'])
        print("\nCombined ATP and Challenger data.")
    elif not odds_df_atp.empty: odds_df = odds_df_atp; print("\nUsing only ATP data.")
    elif not odds_df_challenger.empty: odds_df = odds_df_challenger; print("\nUsing only Challenger data.")
    else: odds_df = pd.DataFrame()
    if not odds_df.empty:
        print(f"\n--- Final Combined Data (Shape: {odds_df.shape}) ---"); print(odds_df.head())
        print("\n--- Saving Scooore Data ---")
        saved_filepath = save_data_to_dated_csv(data=odds_df, base_filename=BASE_FILENAME, output_dir=DATA_DIR)
        if saved_filepath: print(f"Scooore data saving process completed successfully. File: {saved_filepath}")
        else: print("Scooore data saving process failed.")
    else: print("\n--- No Scooore odds data scraped from ATP or Challenger. ---")

