# tennis_abstract_scraper.py (v3 - Optimized Results Parsing & Wait Fix)
# Scrapes probabilities and completed results using Selenium.
# Optimizes results parsing by using innerText and a simpler regex.
# Includes fix for Selenium wait condition in probas_scraper.

import time
import re
import html
from typing import List, Optional, Any, Tuple, Dict
import os
import traceback
from datetime import datetime

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException, StaleElementReferenceException

# Webdriver Manager import
try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    print("Warning: webdriver-manager not found.")
    ChromeDriverManager = None

# --- Utility Functions ---
try:
    from process_data import create_merge_key, preprocess_player_name
except ImportError:
    print("ERROR: Cannot import helper functions from process_data.py. Ensure it's accessible.")
    def create_merge_key(text: str) -> str: return ""
    def preprocess_player_name(name: str) -> Tuple[str, str]: return name, ""

# --- Constants ---
BASE_URL = "http://www.tennisabstract.com/"
WAIT_TIMEOUT = 25
RESULTS_FORECASTS_LINK_TEXT = "Results and Forecasts"
CURRENT_EVENTS_TABLE_ID = "current-events"
ROUND_ORDER = ["R128", "R64", "R32", "R16", "QF", "SF", "F", "W"]
PLAYER_URL_PATTERN = "player.cgi?p="

# --- NEW Simpler Regex for Plain Text Results ---
# Assumes format like: "Round: Winner Name (Maybe Country) d. Loser Name (Maybe Country) Score"
# Uses non-greedy matching for names to handle middle names/initials
RESULT_TEXT_REGEX = re.compile(
    r"^(?P<Round>[A-Za-z0-9]+):\s+" # Round indicator
    r"(?P<WinnerName>.+?)\s+" # Winner Name (non-greedy)
    r"(?:\([A-Z]{3}\)\s+)?" # Optional Winner Country Code
    r"d\.\s+" # Defeated indicator
    r"(?P<LoserName>.+?)\s+" # Loser Name (non-greedy)
    r"(?:\([A-Z]{3}\)\s+)?" # Optional Loser Country Code
    r"(?P<Score>[\d\-\s\(\)\[\]/]+(?:RET|WO)?)$", # Score
    re.IGNORECASE
)
# -----------------------------------------------

# --- WebDriver Setup ---
def setup_driver() -> Optional[webdriver.Chrome]:
    """Sets up and returns a headless Chrome WebDriver instance."""
    print("Setting up Chrome WebDriver...")
    options = ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox"); options.add_argument("--disable-dev-shm-usage"); options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080"); options.add_argument('--log-level=1')
    chromedriver_path_apt = "/usr/bin/chromedriver"; chromedriver_path_wdm = None
    if ChromeDriverManager:
         try: print("Attempting to install/use ChromeDriver via webdriver-manager..."); chromedriver_path_wdm = ChromeDriverManager().install(); print(f"webdriver-manager path: {chromedriver_path_wdm}")
         except Exception as e: print(f"Could not get path from webdriver-manager: {e}")
    driver = None; service = None
    try:
        if chromedriver_path_wdm and os.path.exists(chromedriver_path_wdm):
            print(f"Using chromedriver from webdriver-manager path: {chromedriver_path_wdm}")
            service = ChromeService(executable_path=chromedriver_path_wdm)
            driver = webdriver.Chrome(service=service, options=options)
        elif os.path.exists(chromedriver_path_apt):
            print(f"Using chromedriver from apt path: {chromedriver_path_apt}")
            service = ChromeService(executable_path=chromedriver_path_apt)
            driver = webdriver.Chrome(service=service, options=options)
        else:
            print("Chromedriver not found at specific paths, attempting PATH...")
            driver = webdriver.Chrome(options=options)
        print("Chrome WebDriver setup successful."); return driver
    except WebDriverException as e:
        if "executable needs to be in PATH" in str(e) or "cannot find chrome binary" in str(e) or "session not created" in str(e): print("\n--- ChromeDriver Error ---"); print("Selenium couldn't find or use the ChromeDriver."); print("Possible Solutions:"); print("1. Install ChromeDriver using Homebrew: `brew install chromedriver`"); print("2. Ensure Google Chrome browser is installed and up-to-date."); print("3. Check Chrome & ChromeDriver version compatibility."); print(f"   (Error details: {e})"); print("--------------------------\n")
        else: print(f"WebDriver setup failed with an unexpected error: {e}")
        traceback.print_exc(); return None
    except Exception as e: print(f"An unexpected error occurred during Chrome WebDriver setup: {e}"); traceback.print_exc(); return None

# --- tourneys_url ---
def tourneys_url() -> List[str]:
    """Scrapes Tennis Abstract homepage to find tournament forecast URLs."""
    print(f"Attempting to find tournament URLs from {BASE_URL}...")
    driver = setup_driver()
    if driver is None: return []
    ls_tourneys_urls = []
    try:
        print(f"Navigating to {BASE_URL}...")
        driver.get(BASE_URL)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        print("\n--- Strategy 1: Waiting for current events table and using specific cell selectors ---")
        try:
            events_table = wait.until(EC.presence_of_element_located((By.ID, CURRENT_EVENTS_TABLE_ID)))
            print(f"Table with ID '{CURRENT_EVENTS_TABLE_ID}' found.")
            men_tour_cell_selector = f"table#{CURRENT_EVENTS_TABLE_ID} > tbody > tr:nth-child(1) > td:nth-child(2)"
            challenger_tour_cell_selector = f"table#{CURRENT_EVENTS_TABLE_ID} > tbody > tr:nth-child(1) > td:nth-child(3)"
            target_cells_selectors = [men_tour_cell_selector, challenger_tour_cell_selector]
            found_links_strategy1 = False
            for selector in target_cells_selectors:
                try:
                    target_cell = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, selector)))
                    forecast_links = target_cell.find_elements(By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)
                    print(f"  Found {len(forecast_links)} '{RESULTS_FORECASTS_LINK_TEXT}' links in cell: '{selector}'")
                    for link in forecast_links:
                        href = link.get_attribute("href")
                        if href and href.startswith("http") and href not in ls_tourneys_urls: ls_tourneys_urls.append(href); print(f"    Added URL: {href}"); found_links_strategy1 = True
                except Exception as e: print(f"  Warning: Error processing cell '{selector}': {type(e).__name__}")
            if not found_links_strategy1: print("--- Strategy 1 yielded no URLs. Trying Strategy 2. ---")
            else: print("--- Strategy 1 completed. ---")
        except Exception as e: print(f"Error during Strategy 1: {e}")
        if not ls_tourneys_urls and 'events_table' in locals() and events_table:
             print("\n--- Strategy 2: Searching for links within the entire current events table ---")
             try:
                 all_table_forecast_links = events_table.find_elements(By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)
                 print(f"  Found {len(all_table_forecast_links)} '{RESULTS_FORECASTS_LINK_TEXT}' links within table.")
                 found_links_strategy2 = False
                 for link in all_table_forecast_links:
                     url = link.get_attribute("href")
                     if url and url.startswith("http") and url not in ls_tourneys_urls:
                         url_lower = url.lower(); is_relevant = 'atp' in url_lower or 'challenger' in url_lower
                         if is_relevant: ls_tourneys_urls.append(url); print(f"    Added relevant URL: {url}"); found_links_strategy2 = True
                 if not found_links_strategy2: print("--- Strategy 2 yielded no relevant URLs. Trying Strategy 3. ---")
                 else: print("--- Strategy 2 completed. ---")
             except Exception as e: print(f"Error during Strategy 2: {e}")
        if not ls_tourneys_urls:
             print("\n--- Strategy 3: Falling back to searching entire page for links ---")
             try:
                 wait.until(EC.presence_of_element_located((By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)))
                 all_page_forecast_links = driver.find_elements(By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)
                 print(f"  Found {len(all_page_forecast_links)} '{RESULTS_FORECASTS_LINK_TEXT}' links page-wide.")
                 found_links_strategy3 = False
                 for link in all_page_forecast_links:
                     url = link.get_attribute("href")
                     if url and url.startswith("http") and url not in ls_tourneys_urls:
                         url_lower = url.lower(); is_relevant = 'atp' in url_lower or 'challenger' in url_lower
                         if is_relevant: ls_tourneys_urls.append(url); print(f"    Added relevant URL: {url}"); found_links_strategy3 = True
                 if not found_links_strategy3: print("--- Strategy 3 yielded no relevant URLs. ---")
                 else: print("--- Strategy 3 completed. ---")
             except Exception as e: print(f"Error during Strategy 3: {e}")
        print(f"\nFinished URL search. Found {len(ls_tourneys_urls)} relevant URLs.")
    except Exception as e: print(f"An critical error occurred in tourneys_url: {e}"); traceback.print_exc()
    finally:
        if driver: print("Closing WebDriver for tourneys_url..."); driver.quit(); print("WebDriver closed.")
    return list(dict.fromkeys(ls_tourneys_urls))


# --- probas_scraper ---
def probas_scraper(url: str, driver: webdriver.Chrome) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Scrapes BOTH the probability table AND completed results from a Tennis Abstract tournament URL using Selenium.
    Uses optimized text parsing for results.
    Returns two lists: (upcoming_matchups, completed_results)
    """
    print(f"\nAttempting to scrape matchups and results from: {url}")
    matchups = []
    results = []
    headers = []
    header_map = {}
    wait = WebDriverWait(driver, WAIT_TIMEOUT)

    try:
        print(f"Navigating to {url}...")
        driver.get(url)

        # --- 1. Scrape Probabilities (Forecast Table) ---
        forecast_span_id = "forecast"
        probability_table = None
        try:
            print(f"Waiting for forecast span (ID: {forecast_span_id})...")
            forecast_span = wait.until(EC.presence_of_element_located((By.ID, forecast_span_id)))
            table_locator = (By.CSS_SELECTOR, f"span#{forecast_span_id} table")
            probability_table = wait.until(EC.presence_of_element_located(table_locator))

            # MODIFICATION START: Wait for a table row (tr) instead of just any table cell (td)
            # This provides a slightly stronger guarantee that the table content has started loading.
            # Old line: wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"span#{forecast_span_id} table td")))
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"span#{forecast_span_id} table tr")))
            # MODIFICATION END

            print("Located the probability table.")

            rows = probability_table.find_elements(By.TAG_NAME, "tr")
            print(f"Found {len(rows)} rows in the probability table.")
            if rows:
                last_player_row_data = None; last_player_name = ""
                for i, row in enumerate(rows):
                    try:
                        cells = row.find_elements(By.XPATH, ".//td | .//th")
                        if not cells: continue
                        row_data = [c.text.strip() for c in cells]; is_header = False
                        if not headers and len(row_data) > 3:
                            potential_headers_upper = [h.upper() for h in row_data]
                            round_headers_found = [r for r in ROUND_ORDER if r in potential_headers_upper]
                            if "PLAYER" in potential_headers_upper or len(set(round_headers_found)) >= 2:
                                 headers = [h if h else f"Unknown_{j}" for j, h in enumerate(row_data)]; header_map = {name: index for index, name in enumerate(headers)}; is_header = True; last_player_row_data = None
                        if is_header: continue
                        first_cell_element = cells[0]; player_name = ""; is_player_row = False
                        try:
                            player_link = first_cell_element.find_element(By.TAG_NAME, "a")
                            if PLAYER_URL_PATTERN in player_link.get_attribute("href"):
                                player_name = player_link.text.strip()
                                if first_cell_element.value_of_css_property('font-style') != 'italic': is_player_row = True
                        except NoSuchElementException:
                            first_cell_text = first_cell_element.text.strip()
                            if first_cell_text.upper() != "BYE" and first_cell_text and any(c.isalpha() for c in first_cell_text):
                                player_name = first_cell_text
                                if first_cell_element.value_of_css_property('font-style') != 'italic': is_player_row = True
                        if not is_player_row: last_player_row_data = None; continue
                        while len(row_data) < len(headers): row_data.append(None)
                        if last_player_row_data is None: last_player_row_data = row_data; last_player_name = player_name
                        else:
                            player1_name = last_player_name; player2_name = player_name
                            row1_data = last_player_row_data; row2_data = row_data
                            match_round, p1_prob, p2_prob = None, None, None
                            for round_idx, current_round_name in enumerate(ROUND_ORDER):
                                next_round_name = ROUND_ORDER[round_idx + 1] if round_idx + 1 < len(ROUND_ORDER) else None
                                if next_round_name and next_round_name in header_map:
                                    prob_col_index = header_map[next_round_name]
                                    try:
                                        p1_prob_str = row1_data[prob_col_index].replace('%', '').strip() if row1_data[prob_col_index] else ""
                                        p2_prob_str = row2_data[prob_col_index].replace('%', '').strip() if row2_data[prob_col_index] else ""
                                        if p1_prob_str and p2_prob_str:
                                            p1_f = float(p1_prob_str); p2_f = float(p2_prob_str)
                                            if 99.0 < (p1_f + p2_f) < 101.0 and (p1_f > 0 or p2_f > 0): match_round = current_round_name; p1_prob = p1_f; p2_prob = p2_f; break
                                    except (ValueError, TypeError, IndexError): continue
                            if match_round is not None and p1_prob is not None and p2_prob is not None: matchups.append({"Player1": player1_name, "Player2": player2_name, "P1_Prob": p1_prob, "P2_Prob": p2_prob, "Round": match_round})
                            last_player_row_data = None; last_player_name = ""
                    except StaleElementReferenceException: print(f"  StaleElementReferenceException processing prob row. Skipping."); last_player_row_data = None; continue
                    except Exception as row_err: print(f"  Unexpected error processing prob row: {row_err}"); traceback.print_exc(limit=1); last_player_row_data = None
                print(f"Extracted {len(matchups)} matchups from forecast table.")
                if not headers: print("Warning: Could not identify header row in forecast table.")
        except (TimeoutException, NoSuchElementException) as e:
            print(f"Error finding forecast table/span on {url}: {type(e).__name__}")

        # --- 2. Scrape Completed Results (Optimized) ---
        print("Attempting to scrape completed results...")
        try:
            completed_span = driver.find_element(By.ID, "completed")
            completed_text = completed_span.text # Get plain text content
            if not completed_text or completed_text == '\xa0': # '\xa0' is &nbsp;
                 print("  Completed results span is empty or contains only non-breaking space.")
            else:
                # Split the text into lines
                match_lines = completed_text.splitlines()
                print(f"  Found {len(match_lines)} potential result lines in completed span text.")
                # Extract tournament key from URL for results data
                # Assuming URL format like .../YYYY-TournamentName.html
                url_parts = url.split('/')
                tournament_name_part = url_parts[-1].replace('.html', '') if url_parts else url
                tournament_key = create_merge_key(tournament_name_part) # Use helper
                scrape_date = datetime.now().strftime("%Y-%m-%d") # Use consistent format

                for line in match_lines:
                    line_cleaned = line.strip()
                    if not line_cleaned: continue

                    # Apply the NEW text-based regex
                    match = RESULT_TEXT_REGEX.search(line_cleaned)
                    if match:
                        data = match.groupdict()
                        # Names are already plain text, but unescape just in case
                        winner_name_raw = html.unescape(data.get('WinnerName', '').strip())
                        loser_name_raw = html.unescape(data.get('LoserName', '').strip())
                        round_val = data.get('Round', '').strip()
                        score = data.get('Score', '').strip()

                        # Standardize names and create keys using helpers
                        winner_display, winner_key = preprocess_player_name(winner_name_raw)
                        loser_display, loser_key = preprocess_player_name(loser_name_raw)

                        if winner_key and loser_key:
                            results.append({
                                'ResultDate': scrape_date, # Date results were scraped
                                'TournamentKey': tournament_key, # Add tournament key
                                'Round': round_val,
                                'WinnerName': winner_display,
                                'LoserName': loser_display,
                                'WinnerNameKey': winner_key, # Add winner key
                                'LoserNameKey': loser_key, # Add loser key
                                'Score': score
                            })
                        else:
                            print(f"  Warning: Could not generate keys for result: W='{winner_name_raw}', L='{loser_name_raw}'")
                    # else: # Debug non-matches
                    #    if len(line_cleaned) > 10 and 'd.' in line_cleaned:
                    #        print(f"  Text Regex non-match line: {line_cleaned[:250]}")

                print(f"  Extracted {len(results)} completed matches using text regex.")

        except NoSuchElementException:
            print("  Could not find span with id='completed'. No results extracted.")
        except Exception as e_results:
            print(f"  An error occurred scraping completed results: {e_results}")
            traceback.print_exc(limit=1)

    except WebDriverException as e:
        print(f"WebDriver error accessing {url}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in probas_scraper for {url}: {e}")
        traceback.print_exc()

    # Return both lists (driver is closed by the caller in p_sack_preproc)
    return matchups, results


# --- Example Usage ---
if __name__ == "__main__":
    print("--- Testing tourneys_url ---")
    tournament_urls = tourneys_url()
    if tournament_urls:
        print(f"\nFound URLs:\n" + "\n".join(tournament_urls))
        main_driver = setup_driver()
        if main_driver:
            all_scraped_matchups = []
            all_scraped_results = []
            for i, url_to_scrape in enumerate(tournament_urls):
                print(f"\n--- Testing probas_scraper on URL {i+1}/{len(tournament_urls)}: {url_to_scrape} ---")
                scraped_matchups, scraped_results = probas_scraper(url_to_scrape, main_driver)
                if scraped_matchups: print(f"  Successfully scraped {len(scraped_matchups)} matchups from this URL.")
                else: print(f"  No matchups extracted from URL: {url_to_scrape}")
                if scraped_results: print(f"  Successfully scraped {len(scraped_results)} results from this URL.")
                else: print(f"  No results extracted from URL: {url_to_scrape}")
                all_scraped_matchups.extend(scraped_matchups)
                all_scraped_results.extend(scraped_results)
                time.sleep(1) # Be polite

            if all_scraped_matchups: print(f"\n--- Summary: Scraped a total of {len(all_scraped_matchups)} Matchups ---")
            else: print("\n--- Summary: No matchups extracted from any URL. ---")
            if all_scraped_results: print(f"\n--- Summary: Scraped a total of {len(all_scraped_results)} Results ---")
            else: print("\n--- Summary: No results extracted from any URL. ---")

            print("Closing main WebDriver...")
            main_driver.quit()
            print("WebDriver closed.")
        else: print("Failed to setup main WebDriver for testing.")
    else: print("\nNo tournament URLs found by any strategy.")
