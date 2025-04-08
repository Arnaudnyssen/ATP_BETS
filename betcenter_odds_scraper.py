# betcenter_odds_scraper.py (Wait for Element Count Strategy + Debugging)

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
    print("Warning: webdriver-manager not found.")
    ChromeDriverManager = None

# --- Configuration ---
BASE_URL = "https://www.betcenter.be/fr/tennis"
WAIT_TIMEOUT = 30 # General timeout for initial elements
# Increased timeout specifically for waiting for the match list update
WAIT_TIMEOUT_UPDATE = 35 # Increased from 20
DATA_DIR = "data_archive"
BASE_FILENAME = "betcenter_odds"
DATE_FORMAT = "%Y%m%d"
DEBUG_SCREENSHOT_DIR = "debug_screenshots" # Directory for screenshots on error

# --- SELECTORS ---
# !!! USER ACTION REQUIRED: Verify these selectors AFTER selecting a tournament manually !!!
DROPDOWN_TRIGGER_SELECTOR = (By.CSS_SELECTOR, "#filter-league .filter-select")
DROPDOWN_OPTION_SELECTOR = (By.CSS_SELECTOR, ".filter-select__option")

# Container holding the list of matches *after* a tournament is selected
# VERIFY THIS SELECTOR MANUALLY in browser dev tools after filtering!
GAMELIST_ITEMS_CONTAINER = (By.CSS_SELECTOR, "#content-container > div > home-page > section > div > games-list > div > gamelist > div")
# Marker for individual match elements within the container
# VERIFY THIS SELECTOR MANUALLY in browser dev tools after filtering!
MATCH_ELEMENT_MARKER = (By.CSS_SELECTOR, "div.gamelist_event")

# --- Selectors RELATIVE to a MATCH_ELEMENT_MARKER ---
# VERIFY THESE SELECTORS MANUALLY relative to a match element after filtering!
PLAYER_1_NAME_SELECTOR = (By.CSS_SELECTOR, "div.game-header--team-name-0")
PLAYER_2_NAME_SELECTOR = (By.CSS_SELECTOR, "div.game-header--team-name-1")
ODDS_BUTTON_CONTAINER_SELECTOR = (By.CSS_SELECTOR, "odd-button") # Container for an odds button
ODDS_VALUE_RELATIVE_SELECTOR = (By.CSS_SELECTOR, "div.odd-button__value > div") # The odds value itself

# --- Custom Expected Condition ---
class number_of_elements_present_in_container(object):
    """
    An expectation for checking that the number of elements matching a locator
    found within a parent container locator is at least a certain number.
    """
    def __init__(self, container_locator, child_locator, min_count=1):
        self.container_locator = container_locator
        self.child_locator = child_locator
        self.min_count = min_count
        self.last_exception = None # Store last exception for debugging

    def __call__(self, driver):
        try:
            # Find the container element first
            container = driver.find_element(*self.container_locator)
            # Then find child elements *within* that container
            children = container.find_elements(*self.child_locator)
            count = len(children)
            # Optional: Add a print for debugging the wait itself
            # print(f"DEBUG WAIT: Found {count} children using {self.child_locator} in container {self.container_locator}")
            return count >= self.min_count
        except (NoSuchElementException, StaleElementReferenceException) as e:
            # Container not found or stale, means update might be happening or failed
            self.last_exception = e # Store the exception
            # print(f"DEBUG WAIT: Condition not met (Container not found or stale): {e}") # Debug print
            return False
        except Exception as e:
             # Catch other potential errors during find_elements
             self.last_exception = e
             # print(f"DEBUG WAIT: Condition not met (Other error): {e}") # Debug print
             return False


# --- Helper Functions (setup_driver, parse_odds_value, save_data_to_dated_csv) ---
def setup_driver() -> Optional[webdriver.Chrome]:
    # (No changes needed from your provided script)
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
    # (No changes needed from your provided script)
    if not odds_text: return None
    try: return float(odds_text.replace(',', '.'))
    except ValueError: print(f"Warning: Could not convert odds text '{odds_text}' to float."); return None

def save_data_to_dated_csv(data: pd.DataFrame, base_filename: str, output_dir: str) -> Optional[str]:
    # (No changes needed from your provided script)
    if data is None or data.empty: print("No data provided or DataFrame is empty. Nothing to save."); return None
    script_dir = os.path.dirname(os.path.abspath(__file__)); absolute_output_dir = os.path.join(script_dir, output_dir)
    try: os.makedirs(absolute_output_dir, exist_ok=True); print(f"Ensured output directory exists: '{absolute_output_dir}'")
    except OSError as e: print(f"Error creating output directory '{absolute_output_dir}': {e}"); return None
    today_date_str = datetime.now().strftime(DATE_FORMAT); filename = f"{base_filename}_{today_date_str}.csv"
    output_path = os.path.join(absolute_output_dir, filename); print(f"Attempting to save data to: {output_path}")
    try: data.to_csv(output_path, index=False, encoding='utf-8'); print(f"Successfully saved data to: {output_path}"); return output_path
    except Exception as e: print(f"Error saving data to CSV file '{output_path}': {e}"); traceback.print_exc(); return None

def save_debug_info(driver: webdriver.Chrome, filename_prefix: str):
    """Saves screenshot and page source for debugging."""
    try:
        os.makedirs(DEBUG_SCREENSHOT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        screenshot_filename = os.path.join(DEBUG_SCREENSHOT_DIR, f"{filename_prefix}_{timestamp}.png")
        html_filename = os.path.join(DEBUG_SCREENSHOT_DIR, f"{filename_prefix}_{timestamp}.html")

        driver.save_screenshot(screenshot_filename)
        print(f"  Saved debug screenshot: {screenshot_filename}")

        with open(html_filename, 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print(f"  Saved debug HTML source: {html_filename}")

    except Exception as debug_e:
        print(f"  Error saving debug info: {debug_e}")

# --- Main Scraping Function ---
def scrape_betcenter_tennis() -> pd.DataFrame:
    """
    Scrapes tennis match odds from Betcenter.be/fr/tennis using custom dropdown interaction.
    Excludes ITF tournaments. Uses element count wait strategy for page updates.
    Requires user verification of container/match selectors post-filter.
    """
    driver = setup_driver()
    if driver is None: return pd.DataFrame()

    all_matches_data = []

    try:
        print(f"Navigating to {BASE_URL}...")
        driver.get(BASE_URL)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        # Use the updated timeout for waits after selection
        wait_update = WebDriverWait(driver, WAIT_TIMEOUT_UPDATE)

        print("Pausing briefly for initial page elements...")
        time.sleep(5) # Keep a small initial pause

        # --- Find Dropdown Trigger ---
        print(f"Waiting for dropdown TRIGGER element ({DROPDOWN_TRIGGER_SELECTOR[1]})...")
        trigger_element = wait.until(EC.element_to_be_clickable(DROPDOWN_TRIGGER_SELECTOR))
        print("Dropdown trigger found and clickable.")

        # --- Get and Filter Tournament Options ---
        valid_tournament_texts = []
        try:
            print("Clicking dropdown trigger to get options...")
            # Use JavaScript click as a fallback if direct click is intercepted
            try:
                trigger_element.click()
            except (ElementClickInterceptedException, ElementNotInteractableException):
                print("  Direct click failed, trying JavaScript click for trigger...")
                driver.execute_script("arguments[0].click();", trigger_element)
            print("Clicked dropdown trigger.")

            print(f"Waiting for the first dropdown OPTION ({DROPDOWN_OPTION_SELECTOR[1]}) to appear...")
            # Wait for visibility of at least one option
            wait.until(EC.visibility_of_element_located(DROPDOWN_OPTION_SELECTOR))
            print("At least one option element found and visible.")

            print(f"Finding all options ({DROPDOWN_OPTION_SELECTOR[1]})...")
            time.sleep(1.0) # Increase pause slightly after options appear
            option_elements = driver.find_elements(*DROPDOWN_OPTION_SELECTOR)
            print(f"Found {len(option_elements)} potential option elements. Filtering...")

            # Filter options for ATP/Challenger, excluding ITF
            for option_element in option_elements:
                try:
                    # Check if the element is still valid and displayed
                    if not option_element.is_displayed():
                        continue
                    option_text = option_element.text.strip()
                    if not option_text: # Skip empty options
                        continue
                    option_text_lower = option_text.lower()
                    # Filter logic: include ATP or Challenger, exclude ITF
                    if ("atp" in option_text_lower or "challenger" in option_text_lower) and "itf" not in option_text_lower:
                        if option_text not in valid_tournament_texts:
                             valid_tournament_texts.append(option_text)
                             print(f"  Adding valid tournament: {option_text}")
                except StaleElementReferenceException:
                    print("  Warning: Option became stale while reading text."); continue
                except Exception as e_opt_filter:
                    print(f"  Warning: Error reading option text: {e_opt_filter}"); continue

            print("  Closing dropdown after getting texts (clicking body)...")
            try:
                # Click body to close - might need adjustment if body click doesn't work
                driver.find_element(By.TAG_NAME, 'body').click()
                time.sleep(0.5)
            except Exception as e_close:
                print(f"  Warning: Could not click body to close dropdown ({e_close}).")

        except TimeoutException:
             print(f"Error: Timed out waiting for ANY options matching '{DROPDOWN_OPTION_SELECTOR[1]}'. Selector guess is likely wrong or page load failed.")
             save_debug_info(driver, "options_timeout")
             if driver: driver.quit(); print("Browser closed due to option finding failure.")
             return pd.DataFrame()
        except Exception as e_get_options:
             print(f"Error getting dropdown options: {e_get_options}")
             save_debug_info(driver, "options_error")
             traceback.print_exc(limit=1)
             if driver: driver.quit(); print("Browser closed due to option finding failure.")
             return pd.DataFrame()

        if not valid_tournament_texts:
            print("No valid ATP or Challenger tournament options found after filtering.")
            return pd.DataFrame()

        print(f"\nFound {len(valid_tournament_texts)} relevant tournaments to scrape.")

        # --- Iterate Through Filtered Tournaments ---
        for i, tournament_text in enumerate(valid_tournament_texts):
            print(f"\n--- Processing Tournament {i+1}/{len(valid_tournament_texts)}: {tournament_text} ---")
            try:
                # --- DEBUG: Print Container HTML BEFORE clicking ---
                try:
                    container_before = driver.find_element(*GAMELIST_ITEMS_CONTAINER)
                    print("  --- Container HTML BEFORE selection ---")
                    # Print only first 500 chars to avoid flooding logs
                    print(container_before.get_attribute('outerHTML')[:500] + "...")
                    print("  ------------------------------------")
                except NoSuchElementException:
                    print("  Container not found BEFORE selection (might be normal if it loads later).")
                except Exception as e_debug:
                    print(f"  Error getting container HTML before click: {e_debug}")

                # --- Open Dropdown ---
                print(f"  Re-opening dropdown to select '{tournament_text}'...")
                trigger_element = wait.until(EC.element_to_be_clickable(DROPDOWN_TRIGGER_SELECTOR))
                try:
                    trigger_element.click()
                except (ElementClickInterceptedException, ElementNotInteractableException):
                    print("  Direct click failed, trying JavaScript click for trigger...")
                    driver.execute_script("arguments[0].click();", trigger_element)
                print("  Dropdown trigger clicked.")
                time.sleep(0.5) # Small pause after click

                # --- Find and Click Specific Option ---
                print(f"  Waiting for option '{tournament_text}' to be clickable...")
                # Using XPath to find by exact text is often reliable for custom dropdowns
                option_xpath = f"//div[contains(@class, 'filter-select__option') and normalize-space()='{tournament_text}']"
                # Ensure the option is clickable
                option_to_click = wait.until(EC.element_to_be_clickable((By.XPATH, option_xpath)))
                print(f"  Found option element for '{tournament_text}'. Clicking...")
                try:
                    option_to_click.click()
                except (ElementClickInterceptedException, ElementNotInteractableException):
                     print("  Direct click failed, trying JavaScript click for option...")
                     driver.execute_script("arguments[0].click();", option_to_click)
                print("  Option selected.")
                time.sleep(1.0) # Pause after selection before waiting for update

                # --- Wait for Page Update using Custom Element Count Condition ---
                print(f"  Waiting up to {WAIT_TIMEOUT_UPDATE}s for match list to update...")
                print(f"  (Expecting at least 1 element matching '{MATCH_ELEMENT_MARKER[1]}' inside '{GAMELIST_ITEMS_CONTAINER[1]}')")
                update_successful = False
                wait_condition = number_of_elements_present_in_container(
                    GAMELIST_ITEMS_CONTAINER,
                    MATCH_ELEMENT_MARKER,
                    min_count=1 # Wait for at least one match element
                )
                try:
                    wait_update.until(wait_condition)
                    print("  Match list updated (found at least one match element within container).")
                    update_successful = True
                    time.sleep(1.5) # Brief pause after successful wait for rendering
                except TimeoutException:
                    print(f"  TIMEOUT ({WAIT_TIMEOUT_UPDATE}s) waiting for match elements to appear in container.")
                    # --- DEBUG: Print Container HTML and Screenshot on Timeout ---
                    try:
                        container_after = driver.find_element(*GAMELIST_ITEMS_CONTAINER)
                        print("  --- Container HTML AT TIMEOUT ---")
                        print(container_after.get_attribute('outerHTML'))
                        print("  -------------------------------")
                    except NoSuchElementException:
                        print("  Container element not found AT TIMEOUT.")
                    except Exception as e_debug_timeout:
                         print(f"  Error getting container HTML at timeout: {e_debug_timeout}")
                    # Save screenshot and HTML source
                    save_debug_info(driver, f"update_timeout_{tournament_text.replace(' ','_')[:20]}")
                    # Check the last exception from the custom wait condition
                    if hasattr(wait_condition, 'last_exception') and wait_condition.last_exception:
                        print(f"  Last exception during wait: {wait_condition.last_exception}")
                    update_successful = False
                except Exception as e_wait:
                     print(f"  Unexpected error during custom wait: {e_wait}")
                     save_debug_info(driver, f"wait_error_{tournament_text.replace(' ','_')[:20]}")
                     update_successful = False

                if not update_successful:
                     print("  Skipping to next tournament due to update failure/timeout.")
                     continue # Skip to the next tournament in the list

                # --- Scrape Matches ---
                print("  Scraping matches...")
                # Re-find container and matches AFTER successful wait to ensure freshness
                gamelist_items_container_element = wait.until(EC.presence_of_element_located(GAMELIST_ITEMS_CONTAINER))
                # Use find_elements relative to the container
                match_elements = gamelist_items_container_element.find_elements(*MATCH_ELEMENT_MARKER)
                print(f"  Found {len(match_elements)} match elements for '{tournament_text}'.")

                if not match_elements:
                     print("  Warning: Update seemed successful, but no match elements found inside container. Verify selectors post-filter.")

                # Loop through found match elements
                for match_index, match_element in enumerate(match_elements):
                    try:
                        p1_name, p2_name, p1_odds, p2_odds = "N/A", "N/A", None, None

                        # Extract Player Names (relative to match_element)
                        try:
                            p1_name_el = match_element.find_element(*PLAYER_1_NAME_SELECTOR)
                            p1_name = " ".join(p1_name_el.text.split()) # Clean whitespace
                        except NoSuchElementException:
                            print(f"    Warning: P1 name not found for match {match_index+1}.")
                        try:
                            p2_name_el = match_element.find_element(*PLAYER_2_NAME_SELECTOR)
                            p2_name = " ".join(p2_name_el.text.split()) # Clean whitespace
                        except NoSuchElementException:
                            print(f"    Warning: P2 name not found for match {match_index+1}.")

                        # Extract Odds (relative to match_element)
                        try:
                            # Find the containers for odds buttons/values
                            odds_containers = match_element.find_elements(*ODDS_BUTTON_CONTAINER_SELECTOR)
                            if len(odds_containers) >= 2:
                                # Assume first is P1 odds, second is P2 odds
                                p1_odds_el = odds_containers[0].find_element(*ODDS_VALUE_RELATIVE_SELECTOR)
                                p1_odds = parse_odds_value(p1_odds_el.text)
                                p2_odds_el = odds_containers[1].find_element(*ODDS_VALUE_RELATIVE_SELECTOR)
                                p2_odds = parse_odds_value(p2_odds_el.text)
                            else:
                                print(f"    Warning: Found {len(odds_containers)} odds button containers for match {match_index+1}, expected 2.")
                        except NoSuchElementException:
                             print(f"    Warning: Could not find odds value element within odds container for match {match_index+1}.")
                        except Exception as e_odds_extract:
                             print(f"    Warning: Error extracting odds for match {match_index+1}: {e_odds_extract}")

                        # Add to list if data seems valid
                        if p1_name and p1_name != "N/A" and p2_name and p2_name != "N/A" and p1_odds is not None and p2_odds is not None:
                            # Clean tournament name slightly
                            clean_tournament_name = tournament_text.replace("Tennis - ", "").strip()
                            match_dict = {
                                'tournament': clean_tournament_name,
                                'p1_name': p1_name,
                                'p2_name': p2_name,
                                'p1_odds': p1_odds,
                                'p2_odds': p2_odds
                            }
                            all_matches_data.append(match_dict)
                            # Log first few extractions per tournament
                            if match_index < 3:
                                print(f"    Extracted Match {match_index+1}: {p1_name} ({p1_odds}) vs {p2_name} ({p2_odds})")
                            elif match_index == 3:
                                print("    (Further match extraction logs for this tournament suppressed...)")
                        else:
                            print(f"    Skipping match {match_index+1} due to missing data (P1: '{p1_name}', P2: '{p2_name}', O1: {p1_odds}, O2: {p2_odds}).")

                    except NoSuchElementException as e_inner:
                        print(f"    Error finding element within match {match_index+1}: {e_inner}. Check relative selectors.")
                    except StaleElementReferenceException:
                        print(f"    Warning: Stale element reference processing match {match_index+1}. Skipping."); continue
                    except Exception as e_match:
                        print(f"    Unexpected error processing match {match_index+1}: {e_match}"); traceback.print_exc(limit=1)

            # --- Error Handling for the main tournament loop ---
            except (ElementNotInteractableException, ElementClickInterceptedException) as e_interact:
                print(f"Error interacting with dropdown/option for '{tournament_text}': {e_interact}. Skipping.");
                save_debug_info(driver, f"interaction_error_{tournament_text.replace(' ','_')[:20]}")
                # Try to close dropdown if it's stuck open
                try: driver.find_element(By.TAG_NAME, 'body').click(); time.sleep(0.5);
                except: pass
                continue # Skip to next tournament
            except TimeoutException:
                print(f"Error: Timed out waiting for elements during processing of '{tournament_text}'. Skipping.");
                save_debug_info(driver, f"loop_timeout_{tournament_text.replace(' ','_')[:20]}")
                continue
            except StaleElementReferenceException:
                print(f"Error: Element became stale while processing '{tournament_text}'. Attempting to continue loop.");
                # No screenshot here as it's often recoverable by continuing
                continue
            except Exception as e_loop:
                print(f"Error processing tournament '{tournament_text}': {e_loop}");
                save_debug_info(driver, f"loop_error_{tournament_text.replace(' ','_')[:20]}")
                traceback.print_exc(limit=1);
                continue # Attempt to continue with the next tournament

        print("\nFinished processing all selected tournaments.")

    # --- Outer Error Handling & Cleanup ---
    except TimeoutException as e_main_timeout:
        print(f"Error: Timed out on initial page load or finding dropdown trigger: {e_main_timeout}");
        save_debug_info(driver, "initial_timeout")
        try: print(f"Page Title at Timeout: {driver.title}");
        except Exception: pass
    except NoSuchElementException as e_main_nse:
        print(f"Error: Could not find critical initial element: {e_main_nse}. Check initial selectors.");
        save_debug_info(driver, "initial_nse")
    except Exception as e_outer:
        print(f"An unexpected error occurred during scraping: {e_outer}");
        save_debug_info(driver, "outer_error")
        traceback.print_exc()
    finally:
        if 'driver' in locals() and driver is not None:
            try:
                driver.quit();
                print("Browser closed.")
            except Exception as e_quit:
                print(f"Error quitting driver: {e_quit}")

    # --- Final DataFrame Creation ---
    if not all_matches_data:
        print("\nNo match data collected from Betcenter.");
        return pd.DataFrame()

    print(f"\nCollected data for {len(all_matches_data)} matches in total.")
    try:
        final_df = pd.DataFrame(all_matches_data);
        final_df['scrape_timestamp_utc'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')
        # Clean player names (strip whitespace, lowercase for potential matching later)
        final_df['p1_name'] = final_df['p1_name'].astype(str).str.strip().str.lower()
        final_df['p2_name'] = final_df['p2_name'].astype(str).str.strip().str.lower()
        # Drop duplicates based on cleaned names and tournament
        final_df = final_df.drop_duplicates(subset=['tournament', 'p1_name', 'p2_name'])
        print(f"DataFrame shape after dropping duplicates: {final_df.shape}")
        print("Created final DataFrame:");
        print(final_df.head());
        return final_df
    except Exception as df_err:
        print(f"Error creating or processing final DataFrame: {df_err}");
        traceback.print_exc();
        return pd.DataFrame()

# --- Main Execution ---
if __name__ == "__main__":
    print("Starting Betcenter.be tennis odds scraping process (Element Count Wait Strategy)...")
    odds_df = scrape_betcenter_tennis()

    if not odds_df.empty:
        print("\n--- Saving Betcenter Data ---")
        saved_filepath = save_data_to_dated_csv(
            data=odds_df,
            base_filename=BASE_FILENAME,
            output_dir=DATA_DIR
        )
        if saved_filepath:
             print(f"Betcenter data saving process completed successfully. File: {saved_filepath}")
        else:
             print("Betcenter data saving process failed.")
    else:
        print("\n--- No Betcenter odds data scraped. ---")

