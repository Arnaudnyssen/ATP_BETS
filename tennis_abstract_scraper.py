# tennis_abstract_scraper.py (Extracting Matchups)

import time
import re
from typing import List, Optional, Any, Tuple, Dict # Added Dict
import os
import traceback

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException

# Webdriver Manager import
try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    print("Warning: webdriver-manager not found.")
    ChromeDriverManager = None

# --- Constants ---
BASE_URL = "http://www.tennisabstract.com/"
WAIT_TIMEOUT = 20
RESULTS_FORECASTS_LINK_TEXT = "Results and Forecasts"
CURRENT_EVENTS_TABLE_ID = "current-events"
# Define standard round progression for parsing
ROUND_ORDER = ["R128", "R64", "R32", "R16", "QF", "SF", "F", "W"]

# --- WebDriver Setup (Remains the same) ---
def setup_driver() -> Optional[webdriver.Chrome]:
    """Sets up and returns a headless Chrome WebDriver instance."""
    # (Code is the same as previous version - tennis_scraper_py_fix_headers_20250403)
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
            men_tour_cell_selector = f"table#{CURRENT_EVENTS_TABLE_ID} > tbody > tr:nth-child(1) > td:nth-child(2)"
            challenger_tour_cell_selector = f"table#{CURRENT_EVENTS_TABLE_ID} > tbody > tr:nth-child(1) > td:nth-child(3)"
            target_cells_selectors = [men_tour_cell_selector, challenger_tour_cell_selector]
            found_links_strategy1 = False
            for selector in target_cells_selectors:
                try:
                    target_cell = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    forecast_links = target_cell.find_elements(By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)
                    print(f"  Found {len(forecast_links)} '{RESULTS_FORECASTS_LINK_TEXT}' links in cell: '{selector}'")
                    for link in forecast_links:
                        href = link.get_attribute("href")
                        if href and href.startswith("http") and href not in ls_tourneys_urls:
                            ls_tourneys_urls.append(href); print(f"    Added URL: {href}"); found_links_strategy1 = True
                except Exception as e: print(f"  Warning: Error processing cell '{selector}': {e}")
            if not found_links_strategy1: print("--- Strategy 1 yielded no URLs. Trying Strategy 2. ---")
            else: print("--- Strategy 1 succeeded. ---")
        except Exception as e: print(f"Error during Strategy 1: {e}")

        if not ls_tourneys_urls and 'events_table' in locals() and events_table:
             print("\n--- Strategy 2: Searching for links within the current events table ---")
             try:
                 all_table_forecast_links = events_table.find_elements(By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)
                 print(f"  Found {len(all_table_forecast_links)} links within table.")
                 found_links_strategy2 = False
                 for link in all_table_forecast_links:
                     url = link.get_attribute("href")
                     if url and url.startswith("http") and url not in ls_tourneys_urls:
                         url_lower = url.lower(); is_relevant = 'atp' in url_lower or 'challenger' in url_lower
                         if is_relevant: ls_tourneys_urls.append(url); print(f"    Added relevant URL: {url}"); found_links_strategy2 = True
                 if not found_links_strategy2: print("--- Strategy 2 yielded no relevant URLs. Trying Strategy 3. ---")
                 else: print("--- Strategy 2 succeeded. ---")
             except Exception as e: print(f"Error during Strategy 2: {e}")

        if not ls_tourneys_urls:
             print("\n--- Strategy 3: Falling back to searching entire page for links ---")
             try:
                 wait.until(EC.presence_of_element_located((By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)))
                 all_page_forecast_links = driver.find_elements(By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)
                 print(f"  Found {len(all_page_forecast_links)} links page-wide.")
                 found_links_strategy3 = False
                 for link in all_page_forecast_links:
                     url = link.get_attribute("href")
                     if url and url.startswith("http") and url not in ls_tourneys_urls:
                         url_lower = url.lower(); is_relevant = 'atp' in url_lower or 'challenger' in url_lower
                         if is_relevant: ls_tourneys_urls.append(url); print(f"    Added relevant URL: {url}"); found_links_strategy3 = True
                 if not found_links_strategy3: print("--- Strategy 3 yielded no relevant URLs. ---")
                 else: print("--- Strategy 3 succeeded. ---")
             except Exception as e: print(f"Error during Strategy 3: {e}")
        print(f"\nFinished URL search. Found {len(ls_tourneys_urls)} relevant URLs.")
    except Exception as e: print(f"An critical error occurred in tourneys_url: {e}"); traceback.print_exc()
    finally:
        if driver: print("Closing WebDriver for tourneys_url..."); driver.quit(); print("WebDriver closed.")
    return list(dict.fromkeys(ls_tourneys_urls))


# --- NEW probas_scraper ---
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
        return [] # Return empty list on driver failure

    matchups = []
    try:
        print(f"Navigating to {url}...")
        driver.get(url)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)

        # Locate the forecast table (same as before)
        forecast_span_id = "forecast"
        probability_table = None
        try:
            print(f"Waiting for forecast span (ID: {forecast_span_id})...")
            forecast_span = wait.until(EC.presence_of_element_located((By.ID, forecast_span_id)))
            print("Forecast span found.")
            cell_locator = (By.CSS_SELECTOR, f"span#{forecast_span_id} table td")
            print(f"Waiting for table content (any 'td') within span#{forecast_span_id}...")
            wait.until(EC.presence_of_element_located(cell_locator))
            print("Table content appears to be loaded.")
            table_locator = (By.CSS_SELECTOR, f"span#{forecast_span_id} table")
            probability_table = forecast_span.find_element(*table_locator)
            print("Located the probability table element.")
        except (TimeoutException, NoSuchElementException) as e:
            print(f"Error finding probability table within span#{forecast_span_id} on {url}: {type(e).__name__}")
            return [] # Return empty list if table not found

        # --- Matchup Parsing Logic ---
        headers = []
        header_map = {} # Map header name to column index
        rows = probability_table.find_elements(By.TAG_NAME, "tr")
        print(f"Found {len(rows)} rows in the table.")
        if not rows: return []

        player_rows_buffer = [] # Store consecutive player rows

        for i, row in enumerate(rows):
            cells = row.find_elements(By.XPATH, ".//td | .//th")
            if not cells: continue # Skip empty rows

            # Attempt to identify and store the header row
            if not headers and len(cells) > 2: # Assume header has multiple columns
                potential_headers = [c.text.strip() for c in cells]
                # Check if it looks like a header (contains 'Player' and round names)
                if "Player" in potential_headers and any(r in potential_headers for r in ROUND_ORDER):
                    headers = [h if h else f"Unknown_{j}" for j, h in enumerate(potential_headers)] # Handle empty headers
                    header_map = {name: index for index, name in enumerate(headers)}
                    print(f"Captured Headers: {headers}")
                    player_rows_buffer = [] # Reset buffer after finding header
                    continue # Move to next row after processing header

            # If headers are found, process potential player rows
            if headers:
                # Basic check: Does the first cell look like a player name (contains letters)?
                # This helps filter out rows starting with numbers/symbols, addressing the previous issue.
                first_cell_text = cells[0].text.strip()
                if first_cell_text and any(c.isalpha() for c in first_cell_text):
                    # Store the row's cell data
                    row_data = [c.text.strip() for c in cells]
                    # Pad row data if shorter than headers (e.g., missing columns)
                    while len(row_data) < len(headers): row_data.append(None)
                    player_rows_buffer.append(row_data)
                else:
                    # If row doesn't start like a player name, it breaks the sequence
                    player_rows_buffer = [] # Reset buffer

                # If we have collected two consecutive player rows, process them as a matchup
                if len(player_rows_buffer) == 2:
                    row1_data = player_rows_buffer[0]
                    row2_data = player_rows_buffer[1]

                    # Extract player names (assuming first column)
                    player1_name = row1_data[0]
                    player2_name = row2_data[0]

                    # Find the relevant round and probabilities
                    match_round = None
                    p1_prob = None
                    p2_prob = None

                    # Iterate through round columns in order (e.g., R128, R64... W)
                    for idx, round_name in enumerate(ROUND_ORDER):
                        next_round_name = ROUND_ORDER[idx + 1] if idx + 1 < len(ROUND_ORDER) else None
                        if next_round_name and next_round_name in header_map:
                            prob_col_index = header_map[next_round_name]

                            try:
                                # Extract probabilities from the *next* round's column
                                p1_prob_str = row1_data[prob_col_index].replace('%', '').strip()
                                p2_prob_str = row2_data[prob_col_index].replace('%', '').strip()

                                # Check if these seem like valid matchup probabilities (sum close to 100)
                                p1_f = float(p1_prob_str) if p1_prob_str else 0.0
                                p2_f = float(p2_prob_str) if p2_prob_str else 0.0

                                # Use a tolerance for floating point comparison
                                if 99.0 < (p1_f + p2_f) < 101.0 and (p1_f > 0 or p2_f > 0):
                                    match_round = round_name # This is the round they are *currently* in
                                    p1_prob = p1_f
                                    p2_prob = p2_f
                                    print(f"  Found Match: {player1_name} vs {player2_name} (Round: {match_round}, Probs: {p1_prob}%, {p2_prob}%)")
                                    break # Found the active round for this matchup
                            except (ValueError, TypeError, IndexError):
                                continue # Ignore if conversion fails or index is out of bounds

                    # If a valid matchup was found, add it to the list
                    if match_round is not None and p1_prob is not None and p2_prob is not None:
                        matchups.append({
                            "Player1": player1_name,
                            "Player2": player2_name,
                            "P1_Prob": p1_prob,
                            "P2_Prob": p2_prob,
                            "Round": match_round
                        })

                    # Reset buffer for the next pair
                    player_rows_buffer = []

            else: # Headers not found yet, skip row processing
                continue

        print(f"Extracted {len(matchups)} matchups from {url}.")
        if not headers:
             print("Warning: Could not identify header row.")
        if not matchups:
             print("Warning: No matchups extracted. Check table structure or parsing logic.")

    except Exception as e:
        print(f"An unexpected error occurred in probas_scraper for {url}: {e}")
        traceback.print_exc()
    finally:
        if driver:
            print(f"Closing WebDriver for {url}...")
            driver.quit()
            print("WebDriver closed.")

    return matchups


# --- Example Usage (Modified to show matchup dicts) ---
if __name__ == "__main__":
    print("--- Testing tourneys_url ---")
    tournament_urls = tourneys_url()
    if tournament_urls:
        print(f"\nFound URLs:\n" + "\n".join(tournament_urls))
        if tournament_urls:
            first_url_to_scrape = tournament_urls[0] # Test only the first URL
            print(f"\n--- Testing probas_scraper on first URL: {first_url_to_scrape} ---")
            scraped_matchups = probas_scraper(first_url_to_scrape)

            if scraped_matchups:
                print(f"\nScraped Matchups ({len(scraped_matchups)}):")
                # Print first 5 matchups for preview
                for i, match in enumerate(scraped_matchups[:5]):
                    print(f"  Match {i+1}: {match}")
                if len(scraped_matchups) > 5: print("  ...")
            else:
                print(f"\nNo matchups extracted from the first URL: {first_url_to_scrape}")
    else:
        print("\nNo tournament URLs found by any strategy.")

