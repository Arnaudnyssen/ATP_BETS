# scooore_odds_scraper.py (Using Chrome)

import pandas as pd
import numpy as np
import time
import traceback
from typing import List, Dict, Any, Optional
from datetime import datetime
import os

# Selenium imports (CHANGED to Chrome)
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

# Webdriver Manager import (CHANGED to ChromeDriverManager)
try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    print("Warning: webdriver-manager not found.")
    ChromeDriverManager = None

# --- Configuration ---
BASE_URL = "https://www.scooore.be/fr/sports/sports-hub/tennis"
WAIT_TIMEOUT = 15 # Seconds to wait for elements
DATA_DIR = "data_archive" # Directory to save the output CSV
BASE_FILENAME = "scooore_odds" # Base name for the output file
DATE_FORMAT = "%Y%m%d" # Date format for the filename

# --- SELECTORS based on User Input ---
# WARNING: These XPaths can be brittle. Consider CSS selectors or shorter XPaths.
CHALLENGER_CATEGORY_SELECTOR = (By.XPATH, "/html/body/div[1]/div[2]/div/div[3]/div[2]/div/div/div/div/section/div[2]/div[2]/div/div[1]/div[1]/div/div/div/ul/li[3]/div")
TOURNAMENT_LINK_SELECTOR = (By.XPATH, "//*[@id='KambiBC-content']//section/section/div[1]/div[2]/div/div/div/ul/li/div")
MATCH_LIST_CONTAINER_SELECTOR = (By.XPATH, "//*[@id='KambiBC-content']//section/section/section/section/section/ul")
MATCH_ITEM_SELECTOR = (By.XPATH, "./li") # Relative to MATCH_LIST_CONTAINER_SELECTOR
PLAYER_NAME_SELECTOR = (By.XPATH, ".//a/div[1]/div[2]/div/div/div[1]/div[contains(@class, 'participant-name') or contains(@class, 'player-name')]")
ODDS_VALUE_SELECTOR = (By.XPATH, ".//div/div/div[1]/div/div/button/div/div/div[2]/div")

# --- Helper Functions ---

# --- WebDriver Setup (CHANGED to Chrome, mirrors tennis_abstract_scraper) ---
def setup_driver() -> Optional[webdriver.Chrome]:
    """Sets up and returns a headless Chrome WebDriver instance."""
    print("Setting up Chrome WebDriver...")
    options = ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument('--log-level=1') # Reduce log verbosity

    # Define potential chromedriver paths
    chromedriver_path_apt = "/usr/bin/chromedriver" # Common path on Linux runners
    chromedriver_path_wdm = None
    if ChromeDriverManager:
         try:
              print("Attempting to install/use ChromeDriver via webdriver-manager...")
              # Get the path from webdriver-manager
              chromedriver_path_wdm = ChromeDriverManager().install()
              print(f"webdriver-manager path: {chromedriver_path_wdm}")
         except Exception as e:
              print(f"Could not get path from webdriver-manager: {e}")

    driver = None
    service = None
    try:
        # Prioritize system path if it exists
        if os.path.exists(chromedriver_path_apt):
            print(f"Using chromedriver from apt path: {chromedriver_path_apt}")
            service = ChromeService(executable_path=chromedriver_path_apt)
            driver = webdriver.Chrome(service=service, options=options)
        # Fallback to webdriver-manager path if available
        elif chromedriver_path_wdm and os.path.exists(chromedriver_path_wdm):
             print(f"Using chromedriver from webdriver-manager path: {chromedriver_path_wdm}")
             service = ChromeService(executable_path=chromedriver_path_wdm)
             driver = webdriver.Chrome(service=service, options=options)
        # Final fallback: let Selenium try to find chromedriver in PATH
        else:
             print("Chromedriver not found at specific paths, attempting PATH...")
             driver = webdriver.Chrome(options=options) # No service specified

        print("Chrome WebDriver setup successful.")
        return driver
    except WebDriverException as e:
        print(f"WebDriver setup failed: {e}")
        traceback.print_exc()
        if driver: driver.quit()
        return None
    except Exception as e:
         print(f"An unexpected error occurred during Chrome WebDriver setup: {e}")
         traceback.print_exc()
         if driver: driver.quit()
         return None
# --- End WebDriver Setup Change ---

def parse_odds(odds_text: str) -> Optional[float]:
    """Converts odds text (e.g., '1,85') to float, handling commas."""
    if not odds_text: return None
    try: return float(odds_text.replace(',', '.'))
    except ValueError:
        print(f"Warning: Could not convert odds text '{odds_text}' to float.")
        return None

# --- Function to save data to dated CSV (No changes needed) ---
def save_data_to_dated_csv(data: pd.DataFrame, base_filename: str, output_dir: str) -> Optional[str]:
    """
    Saves the provided DataFrame to a CSV file with today's date in the filename,
    inside the specified output directory. Creates the directory if it doesn't exist.
    Returns the full path of the saved file, or None on failure.
    """
    if data is None or data.empty:
        print("No data provided or DataFrame is empty. Nothing to save.")
        return None

    # Ensure output directory exists relative to the script location
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

# --- Scraper Function (CHANGED driver type hint) ---
def scooore_odds_scraper_refactored(driver: webdriver.Chrome, wait: WebDriverWait, tournament_name: str) -> List[Dict[str, Any]]:
    """
    Scrapes odds for the currently selected tournament using Chrome.
    Assumes tournament is already clicked.
    Returns a list of dictionaries representing matches. Empty list on failure.
    """
    matches_data = []
    print(f"\n--- Scraping Tournament: {tournament_name} ---")
    try:
        # Wait for the match list container (UL element)
        print(f"Waiting for match list container (selector: {MATCH_LIST_CONTAINER_SELECTOR[1]})...")
        match_list_container = wait.until(EC.presence_of_element_located(MATCH_LIST_CONTAINER_SELECTOR))
        print("Match list container found.")

        # Find all individual match list items (li) within the container
        # Adding a small delay might help if elements load dynamically after container appears
        time.sleep(0.5)
        match_elements = match_list_container.find_elements(*MATCH_ITEM_SELECTOR)
        print(f"Found {len(match_elements)} potential match list items (li).")

        if not match_elements:
            print(f"No match items (li) found for {tournament_name} using selector '{MATCH_ITEM_SELECTOR[1]}'.")
            return []

        # Iterate through each found match element (li)
        for index, match_element in enumerate(match_elements):
            p1_name, p2_name, p1_odds, p2_odds = "N/A", "N/A", None, None
            try:
                # Find player names *within* this specific match element (li)
                player_name_elements = match_element.find_elements(*PLAYER_NAME_SELECTOR)
                if len(player_name_elements) >= 2:
                    # Use .text attribute and clean whitespace
                    p1_name = " ".join(player_name_elements[0].text.split())
                    p2_name = " ".join(player_name_elements[1].text.split())
                else:
                    print(f"Warning: Found {len(player_name_elements)} player names in match {index+1}, expected 2.")

                # Find odds values *within* this specific match element (li)
                odds_value_elements = match_element.find_elements(*ODDS_VALUE_SELECTOR)
                if len(odds_value_elements) >= 2:
                    p1_odds = parse_odds(odds_value_elements[0].text)
                    p2_odds = parse_odds(odds_value_elements[1].text)
                else:
                    print(f"Warning: Found {len(odds_value_elements)} odds values in match {index+1}, expected 2.")

                # Basic validation
                if p1_name != "N/A" and p2_name != "N/A" and p1_odds is not None and p2_odds is not None:
                    match_dict = {
                        'p1_name': p1_name, 'p2_name': p2_name,
                        'p1_odds': p1_odds, 'p2_odds': p2_odds,
                        'tournament': tournament_name
                    }
                    matches_data.append(match_dict)
                    # Optional: Print extracted match for debugging
                    # print(f"  Extracted Match {index+1}: {p1_name} ({p1_odds}) vs {p2_name} ({p2_odds})")
                else:
                    print(f"  Skipping match {index+1} in {tournament_name} due to missing data (Name1: {p1_name}, Name2: {p2_name}, Odds1: {p1_odds}, Odds2: {p2_odds})...")

            except StaleElementReferenceException:
                print(f"Warning: Stale element reference processing match {index+1}. Skipping match.")
                continue # Skip to the next match element
            except NoSuchElementException as e:
                print(f"Error finding element within match {index+1}: {e}")
            except Exception as e_inner:
                print(f"Unexpected error processing match {index+1}: {e_inner}")
                traceback.print_exc(limit=1)

    except TimeoutException:
        print(f"Error: Timed out waiting for match list for {tournament_name}.")
    except NoSuchElementException:
        print(f"Error: Could not find primary match list container for {tournament_name}.")
    except Exception as e_outer:
        print(f"An unexpected error occurred while scraping {tournament_name}: {e_outer}")
        traceback.print_exc()

    print(f"--- Finished scraping {tournament_name}, extracted {len(matches_data)} valid matches. ---")
    return matches_data


# --- Main Scraping Orchestrator (No changes needed here) ---
def scrape_all_scoore_tournaments(scrape_challenger=False) -> pd.DataFrame:
    """
    Navigates Scooore, finds ATP or Challenger tournaments, scrapes odds for each,
    and returns a consolidated DataFrame. Uses Chrome.
    """
    driver = setup_driver() # Uses the updated setup_driver for Chrome
    if driver is None:
        return pd.DataFrame()

    all_matches_data = []
    try:
        print(f"Navigating to {BASE_URL}...")
        driver.get(BASE_URL)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        # Wait for a known stable element on the page after load
        wait.until(EC.presence_of_element_located((By.ID, "KambiBC-content"))) # Example ID
        print("Main Kambi content area loaded.")
        time.sleep(1) # Small pause for dynamic content

        target_category_text = "Challenger" if scrape_challenger else "ATP"
        print(f"Targeting {target_category_text} tournaments.")

        if scrape_challenger:
            try:
                print("Attempting to navigate to Challenger tournaments...")
                # Wait for the specific Challenger button/link to be clickable
                challenger_button = wait.until(EC.element_to_be_clickable(CHALLENGER_CATEGORY_SELECTOR))
                # Scroll into view if needed (sometimes helps)
                driver.execute_script("arguments[0].scrollIntoView(true);", challenger_button)
                time.sleep(0.5) # Pause after scroll
                challenger_button.click()
                print("Clicked Challenger category selector.")
                # Wait for content to potentially reload - look for tournament links again
                wait.until(EC.presence_of_element_located(TOURNAMENT_LINK_SELECTOR))
                print("Waited for Challenger tournaments to load.")
                time.sleep(1) # Extra pause after click/load
            except (NoSuchElementException, TimeoutException) as e:
                print(f"Error finding or clicking Challenger category selector: {e}. Scraping default (ATP).")
                # Reset scrape_challenger if navigation failed? Or proceed with ATP?
                # For now, proceed assuming ATP might be visible.

        # Find all visible tournament links/buttons
        tournament_elements_info = []
        try:
            print(f"Waiting for tournament links (selector: {TOURNAMENT_LINK_SELECTOR[1]})...")
            # Ensure the elements are present AND visible might be more robust
            tournament_elements_list = wait.until(EC.visibility_of_all_elements_located(TOURNAMENT_LINK_SELECTOR))
            print(f"Found {len(tournament_elements_list)} tournament links/buttons.")
            if not tournament_elements_list:
                 print("No tournament links found. Exiting.")
                 return pd.DataFrame()

            # Get names and indices carefully, handling potential staleness
            for i in range(len(tournament_elements_list)):
                 try:
                     # Re-find the list in each iteration to reduce stale elements
                     current_elements = wait.until(EC.visibility_of_all_elements_located(TOURNAMENT_LINK_SELECTOR))
                     if i < len(current_elements):
                         el = current_elements[i]
                         name = el.text.strip()
                         if name:
                             tournament_elements_info.append({'index': i, 'name': name})
                             # print(f"  Found tournament: {name} (Index: {i})")
                         else:
                             print(f"Warning: Tournament element at index {i} has empty text.")
                     else:
                         print(f"Warning: Index {i} out of bounds after re-finding elements.")
                         break # Stop if list size changed unexpectedly
                 except StaleElementReferenceException:
                     print(f"Warning: Tournament element at index {i} became stale while getting name. Trying to continue.")
                     # We might miss one, but try to proceed
                     continue

            print(f"Identified {len(tournament_elements_info)} tournaments with names to process.")

            # Iterate through found tournaments by index, re-finding elements each time
            processed_tournaments = 0
            for tourney_info in tournament_elements_info:
                tourney_index = tourney_info['index'] # Use the original index for lookup
                tourney_name = tourney_info['name']
                print(f"\nProcessing tournament {processed_tournaments + 1}/{len(tournament_elements_info)}: {tourney_name}")
                try:
                    # Re-find the list of tournament elements *before* clicking
                    current_tournament_elements = wait.until(EC.visibility_of_all_elements_located(TOURNAMENT_LINK_SELECTOR))
                    if tourney_index < len(current_tournament_elements):
                        element_to_click = current_tournament_elements[tourney_index]
                        print(f"Clicking tournament element for '{tourney_name}'...")
                        # Use JavaScript click as a fallback if direct click fails
                        try:
                            element_to_click.click()
                        except Exception as click_err:
                            print(f"Direct click failed ({click_err}), trying JavaScript click...")
                            driver.execute_script("arguments[0].click();", element_to_click)

                        # Wait for something specific on the tournament page to load
                        # (e.g., the match list container) before scraping
                        wait.until(EC.presence_of_element_located(MATCH_LIST_CONTAINER_SELECTOR))
                        print("Tournament page loaded, starting scrape...")

                        # Scrape the data for this tournament
                        tournament_data = scooore_odds_scraper_refactored(driver, wait, tourney_name)
                        all_matches_data.extend(tournament_data)
                        processed_tournaments += 1

                        # Navigate back or refresh carefully
                        print("Navigating back to main Tennis Hub URL...")
                        driver.get(BASE_URL)
                        wait.until(EC.presence_of_element_located((By.ID, "KambiBC-content")))
                        print("Reloaded main page.")
                        time.sleep(1) # Pause after navigation

                        # Re-select Challenger category if needed for the next iteration
                        if scrape_challenger:
                             try:
                                 challenger_button = wait.until(EC.element_to_be_clickable(CHALLENGER_CATEGORY_SELECTOR))
                                 driver.execute_script("arguments[0].scrollIntoView(true);", challenger_button)
                                 time.sleep(0.5)
                                 challenger_button.click()
                                 wait.until(EC.presence_of_element_located(TOURNAMENT_LINK_SELECTOR)) # Wait for links again
                                 print("Re-selected Challenger category.")
                                 time.sleep(1) # Pause after category click
                             except Exception as e_reclick:
                                 print(f"Warning: Failed to re-click Challenger category: {e_reclick}. May affect next iteration.")
                                 # Consider breaking or resetting scrape_challenger flag if this is critical
                    else:
                        print(f"Warning: Tournament index {tourney_index} out of bounds ({len(current_tournament_elements)} elements found). Skipping '{tourney_name}'.")
                        continue

                except StaleElementReferenceException:
                    print(f"Error: Tournament link for '{tourney_name}' became stale before/during processing. Skipping.")
                    # Attempt to recover by refreshing the main page for the next loop
                    try:
                        driver.get(BASE_URL)
                        wait.until(EC.presence_of_element_located((By.ID, "KambiBC-content")))
                        if scrape_challenger: # Re-click challenger if needed
                             challenger_button = wait.until(EC.element_to_be_clickable(CHALLENGER_CATEGORY_SELECTOR))
                             challenger_button.click(); time.sleep(1)
                             wait.until(EC.presence_of_element_located(TOURNAMENT_LINK_SELECTOR))
                        print("Refreshed main page after stale element error.")
                    except Exception as refresh_err:
                         print(f"CRITICAL: Failed to refresh page after stale element error: {refresh_err}. Aborting loop.")
                         break # Exit loop if recovery fails
                    continue # Continue to the next tournament info item
                except Exception as loop_error:
                    print(f"Error processing tournament '{tourney_name}': {loop_error}")
                    traceback.print_exc(limit=2)
                    # Optional: Try to navigate back for the next loop
                    try:
                        driver.get(BASE_URL)
                        wait.until(EC.presence_of_element_located((By.ID, "KambiBC-content")))
                        if scrape_challenger: # Re-click challenger if needed
                             challenger_button = wait.until(EC.element_to_be_clickable(CHALLENGER_CATEGORY_SELECTOR))
                             challenger_button.click(); time.sleep(1)
                             wait.until(EC.presence_of_element_located(TOURNAMENT_LINK_SELECTOR))
                        print("Navigated back to main page after loop error.")
                    except Exception as nav_err:
                         print(f"CRITICAL: Failed to navigate back after loop error: {nav_err}. Aborting loop.")
                         break # Exit loop if navigation fails

        except (NoSuchElementException, TimeoutException) as e:
            print(f"Error: Could not find initial tournament links list: {e}")
        except Exception as e:
            print(f"An unexpected error occurred finding/looping tournaments: {e}")
            traceback.print_exc()

    finally:
        if driver:
            driver.quit()
            print("Browser closed.")

    # --- Final DataFrame Creation ---
    if not all_matches_data:
        print("\nNo match data collected from any tournament.")
        return pd.DataFrame()

    print(f"\nCollected data for {len(all_matches_data)} matches in total.")
    try:
        final_df = pd.DataFrame(all_matches_data)
        # Add timestamp
        final_df['scrape_timestamp_utc'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')
        # Clean player names (strip whitespace, lowercase) for better matching later
        final_df['p1_name'] = final_df['p1_name'].str.strip().str.lower()
        final_df['p2_name'] = final_df['p2_name'].str.strip().str.lower()
        # Drop duplicates based on cleaned names and tournament
        final_df = final_df.drop_duplicates(subset=['tournament', 'p1_name', 'p2_name'])
        print(f"DataFrame shape after dropping duplicates: {final_df.shape}")
        print("Created final DataFrame:")
        print(final_df.head())
        return final_df
    except Exception as df_err:
        print(f"Error creating or processing final DataFrame: {df_err}")
        traceback.print_exc()
        return pd.DataFrame() # Return empty on error


# --- Main Execution ---
if __name__ == "__main__":
    print("Starting Scooore odds scraping process (using Chrome)...")
    # Set scrape_challenger=True or False as needed
    # Example: Scrape both ATP (default) and Challenger tournaments
    odds_df_atp = scrape_all_scoore_tournaments(scrape_challenger=False)
    odds_df_challenger = scrape_all_scoore_tournaments(scrape_challenger=True)

    # Combine results if both were scraped
    if not odds_df_atp.empty and not odds_df_challenger.empty:
        odds_df = pd.concat([odds_df_atp, odds_df_challenger], ignore_index=True)
        # Drop duplicates again after combining
        odds_df = odds_df.drop_duplicates(subset=['tournament', 'p1_name', 'p2_name'])
        print("\nCombined ATP and Challenger data.")
    elif not odds_df_atp.empty:
        odds_df = odds_df_atp
        print("\nUsing only ATP data.")
    elif not odds_df_challenger.empty:
        odds_df = odds_df_challenger
        print("\nUsing only Challenger data.")
    else:
        odds_df = pd.DataFrame() # Ensure it's an empty DataFrame if nothing was scraped

    if not odds_df.empty:
        print(f"\n--- Final Combined Data (Shape: {odds_df.shape}) ---")
        print(odds_df.head())
        print("\n--- Saving Scooore Data ---")
        saved_filepath = save_data_to_dated_csv(
            data=odds_df,
            base_filename=BASE_FILENAME,
            output_dir=DATA_DIR
        )
        if saved_filepath:
             print(f"Scooore data saving process completed successfully. File: {saved_filepath}")
        else:
             print("Scooore data saving process failed.")

    else:
        print("\n--- No Scooore odds data scraped from ATP or Challenger. ---")

