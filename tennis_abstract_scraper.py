# tennis_abstract_scraper.py (Fix for missing matches)

import time
import re
from typing import List, Optional, Any, Tuple, Dict
import os
import traceback

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

# --- Constants ---
BASE_URL = "http://www.tennisabstract.com/"
WAIT_TIMEOUT = 25 # Increased slightly for potentially complex pages
RESULTS_FORECASTS_LINK_TEXT = "Results and Forecasts"
CURRENT_EVENTS_TABLE_ID = "current-events"
# Define standard round progression for parsing - IMPORTANT for finding match round
ROUND_ORDER = ["R128", "R64", "R32", "R16", "QF", "SF", "F", "W"]
PLAYER_URL_PATTERN = "player.cgi?p=" # To identify player links

# --- WebDriver Setup (Remains the same) ---
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

# --- tourneys_url (Remains the same) ---
def tourneys_url() -> List[str]:
    """Scrapes Tennis Abstract homepage to find tournament forecast URLs."""
    # (Code is the same as previous version - tennis_scraper_py_fix_headers_20250403)
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
            # Selectors targeting ATP and Challenger columns specifically
            men_tour_cell_selector = f"table#{CURRENT_EVENTS_TABLE_ID} > tbody > tr:nth-child(1) > td:nth-child(2)"
            challenger_tour_cell_selector = f"table#{CURRENT_EVENTS_TABLE_ID} > tbody > tr:nth-child(1) > td:nth-child(3)"
            target_cells_selectors = [men_tour_cell_selector, challenger_tour_cell_selector]
            found_links_strategy1 = False
            for selector in target_cells_selectors:
                try:
                    # Wait for cell and links within it
                    target_cell = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, selector)))
                    # Find links specifically by text within this cell
                    forecast_links = target_cell.find_elements(By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)
                    print(f"  Found {len(forecast_links)} '{RESULTS_FORECASTS_LINK_TEXT}' links in cell: '{selector}'")
                    for link in forecast_links:
                        href = link.get_attribute("href")
                        if href and href.startswith("http") and href not in ls_tourneys_urls:
                            ls_tourneys_urls.append(href); print(f"    Added URL: {href}"); found_links_strategy1 = True
                except Exception as e: print(f"  Warning: Error processing cell '{selector}': {type(e).__name__}") # More concise error
            if not found_links_strategy1: print("--- Strategy 1 yielded no URLs. Trying Strategy 2. ---")
            else: print("--- Strategy 1 completed. ---")
        except Exception as e: print(f"Error during Strategy 1: {e}")

        # Strategy 2: If Strategy 1 failed, search entire table (more broad)
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

        # Strategy 3: Fallback to searching the whole page (least specific)
        if not ls_tourneys_urls:
             print("\n--- Strategy 3: Falling back to searching entire page for links ---")
             try:
                 # Wait for at least one such link to be present anywhere
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
    # Remove duplicates while preserving order
    return list(dict.fromkeys(ls_tourneys_urls))


# --- REVISED probas_scraper ---
def probas_scraper(url: str) -> List[Dict[str, Any]]:
    """
    Scrapes the probability table from a Tennis Abstract tournament URL,
    identifies matchups, and extracts head-to-head probabilities.

    Args:
        url (str): The URL of the tournament forecast page.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary
                               represents a match with keys like 'Player1', 'Player2',
                               'P1_Prob', 'P2_Prob', 'Round'. Returns empty list on failure.
    """
    print(f"\nAttempting to scrape matchups from: {url}")
    driver = setup_driver()
    if driver is None:
        print(f"Failed to setup WebDriver for {url}. Aborting.")
        return []

    matchups = []
    try:
        print(f"Navigating to {url}...")
        driver.get(url)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)

        # Locate the forecast table span and the table within it
        forecast_span_id = "forecast"
        probability_table = None
        try:
            print(f"Waiting for forecast span (ID: {forecast_span_id})...")
            # Wait for the span itself first
            forecast_span = wait.until(EC.presence_of_element_located((By.ID, forecast_span_id)))
            print("Forecast span found.")
            # Then wait for a table *within* that span to be present
            table_locator = (By.CSS_SELECTOR, f"span#{forecast_span_id} table")
            probability_table = wait.until(EC.presence_of_element_located(table_locator))
            # Maybe add a small wait for content *within* the table cells
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"span#{forecast_span_id} table td")))
            print("Located the probability table element and waited for cell content.")
        except (TimeoutException, NoSuchElementException) as e:
            print(f"Error finding probability table within span#{forecast_span_id} on {url}: {type(e).__name__}")
            # Attempt to find *any* table if the specific one fails? (Less reliable)
            try:
                print("Fallback: Trying to find any table on the page...")
                probability_table = wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                print("Fallback successful: Found a table.")
            except (TimeoutException, NoSuchElementException):
                print("Fallback failed: Could not find any table.")
                return [] # Return empty list if table not found

        # --- Matchup Parsing Logic (Revised) ---
        headers = []
        header_map = {} # Map header name to column index
        rows = probability_table.find_elements(By.TAG_NAME, "tr")
        print(f"Found {len(rows)} rows in the table.")
        if not rows: return []

        last_player_row_data = None # Buffer for the first player in a potential matchup
        last_player_name = ""

        for i, row in enumerate(rows):
            try:
                cells = row.find_elements(By.XPATH, ".//td | .//th") # Get both header and data cells
                if not cells:
                    # print(f"  Row {i+1}: Skipping empty row.")
                    last_player_row_data = None # Reset buffer if row is empty
                    continue

                row_data = [c.text.strip() for c in cells]

                # --- Header Row Identification ---
                # More robust header check: look for 'Player' and at least one Round name
                is_header = False
                if not headers and len(row_data) > 3: # Header should have multiple columns
                    potential_headers = [h.upper() for h in row_data] # Use uppercase for comparison
                    if "PLAYER" in potential_headers and any(r in potential_headers for r in ROUND_ORDER):
                        headers = [h if h else f"Unknown_{j}" for j, h in enumerate(row_data)] # Handle empty headers
                        header_map = {name: index for index, name in enumerate(headers)}
                        print(f"Captured Headers (Row {i+1}): {headers}")
                        is_header = True
                        last_player_row_data = None # Reset buffer after finding header

                if is_header: continue # Skip further processing for header row

                # --- Player Row Identification & Pairing ---
                # Check if the first cell contains a player link
                first_cell_element = cells[0]
                player_name = ""
                is_player_row = False
                try:
                    player_link = first_cell_element.find_element(By.TAG_NAME, "a")
                    if PLAYER_URL_PATTERN in player_link.get_attribute("href"):
                        player_name = player_link.text.strip()
                        # Handle potential italicized names (completed matches) - skip them
                        if first_cell_element.value_of_css_property('font-style') == 'italic':
                             print(f"  Row {i+1}: Skipping italicized player row: {player_name}")
                             last_player_row_data = None # Reset buffer
                             continue # Skip completed match rows

                        is_player_row = True
                        # print(f"  Row {i+1}: Identified Player Row: {player_name}")
                    else:
                         # print(f"  Row {i+1}: First cell has a link, but not a player link.")
                         pass
                except NoSuchElementException:
                    # If no link, check if it's 'Bye' or just text
                    first_cell_text = first_cell_element.text.strip()
                    if first_cell_text.upper() == "BYE":
                        # print(f"  Row {i+1}: Skipping 'Bye' row.")
                        last_player_row_data = None # Reset buffer
                        continue
                    elif first_cell_text and any(c.isalpha() for c in first_cell_text):
                        # Could be a player without a link (less common, maybe qualifiers?)
                        # Treat as potential player for now, but less certain
                        player_name = first_cell_text
                        # is_player_row = True # Decide if you want to include these
                        # print(f"  Row {i+1}: Identified potential player row (no link): {player_name}")
                        pass # For now, let's primarily rely on linked players
                    else:
                         # print(f"  Row {i+1}: Row does not start with player link, 'Bye', or text name.")
                         pass


                if not is_player_row:
                    # If it's not a player row (or Bye/Header), reset the buffer
                    # print(f"  Row {i+1}: Not a player row, resetting buffer.")
                    last_player_row_data = None
                    continue

                # --- Process Player Row ---
                # Pad row data if shorter than headers
                while len(row_data) < len(headers): row_data.append(None)

                if last_player_row_data is None:
                    # This is the first player in a potential pair
                    # print(f"  Row {i+1}: Storing as potential Player 1: {player_name}")
                    last_player_row_data = row_data
                    last_player_name = player_name
                else:
                    # This is the second player row, attempt to form a matchup
                    player1_name = last_player_name
                    player2_name = player_name
                    row1_data = last_player_row_data
                    row2_data = row_data
                    # print(f"  Row {i+1}: Potential Player 2: {player2_name}. Attempting matchup with {player1_name}")

                    # Find the relevant round and probabilities
                    match_round = None
                    p1_prob = None
                    p2_prob = None

                    # Iterate through round columns *defined in ROUND_ORDER*
                    # Start from R64 because R128 probs might be in R64 col, etc.
                    for round_idx, current_round_name in enumerate(ROUND_ORDER):
                        next_round_name = ROUND_ORDER[round_idx + 1] if round_idx + 1 < len(ROUND_ORDER) else None

                        if next_round_name and next_round_name in header_map:
                            prob_col_index = header_map[next_round_name]

                            try:
                                # Extract probabilities from the *next* round's column
                                p1_prob_str = row1_data[prob_col_index].replace('%', '').strip()
                                p2_prob_str = row2_data[prob_col_index].replace('%', '').strip()

                                # Check if BOTH are valid numbers before summing
                                if p1_prob_str and p2_prob_str:
                                    p1_f = float(p1_prob_str)
                                    p2_f = float(p2_prob_str)

                                    # Check if these seem like valid matchup probabilities (sum close to 100)
                                    # Use a tolerance for floating point comparison
                                    if 99.0 < (p1_f + p2_f) < 101.0 and (p1_f > 0 or p2_f > 0):
                                        # If probabilities sum correctly, the *current* round is the match round
                                        match_round = current_round_name
                                        p1_prob = p1_f
                                        p2_prob = p2_f
                                        print(f"  MATCH FOUND: {player1_name} vs {player2_name} (Round: {match_round}, Probs Col: {next_round_name}, Probs: {p1_prob}%, {p2_prob}%)")
                                        break # Found the active round for this matchup
                                else:
                                    # print(f"    Skipping round '{current_round_name}': Missing probability in column '{next_round_name}'.")
                                    pass

                            except (ValueError, TypeError) as prob_err:
                                # print(f"    Skipping round '{current_round_name}': Error converting probability in column '{next_round_name}' ({prob_err}).")
                                continue # Ignore if conversion fails
                            except IndexError:
                                print(f"    Skipping round '{current_round_name}': Index error accessing column '{next_round_name}' (Index: {prob_col_index}). Headers: {len(headers)}, Row1: {len(row1_data)}, Row2: {len(row2_data)}")
                                continue # Ignore if index is out of bounds

                    # If a valid matchup was found, add it to the list
                    if match_round is not None and p1_prob is not None and p2_prob is not None:
                        matchups.append({
                            "Player1": player1_name,
                            "Player2": player2_name,
                            "P1_Prob": p1_prob,
                            "P2_Prob": p2_prob,
                            "Round": match_round
                        })

                    # Reset buffer regardless of whether a match was found, start looking for next pair
                    last_player_row_data = None
                    last_player_name = ""

            except StaleElementReferenceException:
                print(f"  Row {i+1}: StaleElementReferenceException encountered. Skipping row.")
                last_player_row_data = None # Reset buffer
                continue
            except Exception as row_err:
                 print(f"  Row {i+1}: Unexpected error processing row: {row_err}")
                 traceback.print_exc(limit=1)
                 last_player_row_data = None # Reset buffer on unexpected error

        print(f"Extracted {len(matchups)} matchups from {url}.")
        if not headers:
             print("Warning: Could not identify header row. Matchup extraction might be unreliable.")
        if not matchups and len(rows) > 1:
             print("Warning: No matchups extracted despite finding table rows. Check table structure or parsing logic (e.g., player identification, probability columns).")

    except Exception as e:
        print(f"An unexpected error occurred in probas_scraper for {url}: {e}")
        traceback.print_exc()
    finally:
        if driver:
            print(f"Closing WebDriver for {url}...")
            driver.quit()
            print("WebDriver closed.")

    return matchups


# --- Example Usage (Remains the same) ---
if __name__ == "__main__":
    print("--- Testing tourneys_url ---")
    tournament_urls = tourneys_url()
    if tournament_urls:
        print(f"\nFound URLs:\n" + "\n".join(tournament_urls))

        # --- Test scraping ALL found URLs ---
        all_scraped_matchups = []
        for i, url_to_scrape in enumerate(tournament_urls):
            print(f"\n--- Testing probas_scraper on URL {i+1}/{len(tournament_urls)}: {url_to_scrape} ---")
            scraped_matchups = probas_scraper(url_to_scrape)
            if scraped_matchups:
                print(f"  Successfully scraped {len(scraped_matchups)} matchups from this URL.")
                all_scraped_matchups.extend(scraped_matchups) # Add to overall list
            else:
                print(f"  No matchups extracted from URL: {url_to_scrape}")
            time.sleep(1) # Small pause between requests

        # --- Print summary of ALL scraped matchups ---
        if all_scraped_matchups:
             print(f"\n--- Summary: Scraped a total of {len(all_scraped_matchups)} Matchups ---")
             # Print first 10 matchups for preview
             for i, match in enumerate(all_scraped_matchups[:10]):
                 print(f"  Match {i+1}: {match}")
             if len(all_scraped_matchups) > 10: print("  ...")
        else:
             print("\n--- Summary: No matchups extracted from any URL. ---")

    else:
        print("\nNo tournament URLs found by any strategy.")
