# tennis_abstract_scraper.py (Return Headers from probas_scraper)

import time
import re
from typing import List, Optional, Any, Tuple # Added Tuple
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
    print("Warning: webdriver-manager not found. Local execution might require manual chromedriver setup.")
    ChromeDriverManager = None

# --- Constants ---
BASE_URL = "http://www.tennisabstract.com/"
WAIT_TIMEOUT = 20
RESULTS_FORECASTS_LINK_TEXT = "Results and Forecasts"
CURRENT_EVENTS_TABLE_ID = "current-events"

# --- WebDriver Setup (Remains the same) ---
def setup_driver() -> Optional[webdriver.Chrome]:
    """
    Sets up and returns a headless Chrome WebDriver instance.
    Uses chromedriver installed via apt in GitHub Actions.
    Falls back to webdriver-manager for local execution if available.
    """
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
              print("Ensure network connectivity if webdriver-manager needs to download.")

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
        print(f"WebDriver setup failed: {e}")
        if "net::ERR_CONNECTION_REFUSED" in str(e):
             print("Hint: Connection refused often means the WebDriver process couldn't start or be reached.")
        if "cannot find Chrome binary" in str(e):
             print("Hint: Ensure Google Chrome or Chromium browser is installed on the system/runner.")
        if driver: driver.quit()
        return None
    except Exception as e:
         print(f"An unexpected error occurred during Chrome WebDriver setup: {e}")
         traceback.print_exc()
         if driver: driver.quit()
         return None

# --- Scraping Functions ---

# --- tourneys_url (Remains the same as previous robust version) ---
def tourneys_url() -> List[str]:
    """
    Scrapes Tennis Abstract homepage to find URLs for ATP/Challenger tournament
    Results and Forecasts pages, using multiple strategies for robustness.
    (Content is the same as tennis_scraper_py_fix_20250403 version)
    """
    print(f"Attempting to find tournament URLs from {BASE_URL}...")
    driver = setup_driver()
    if driver is None:
        print("Failed to setup WebDriver in tourneys_url. Aborting.")
        return []

    ls_tourneys_urls = []
    try:
        print(f"Navigating to {BASE_URL}...")
        driver.get(BASE_URL)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)

        # --- Strategy 1: Wait for table, then use specific CSS selectors ---
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
                    print(f"  Found target cell using selector: '{selector}'")
                    forecast_links = target_cell.find_elements(By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)
                    print(f"    Found {len(forecast_links)} '{RESULTS_FORECASTS_LINK_TEXT}' links in this cell.")
                    for link in forecast_links:
                        href = link.get_attribute("href")
                        if href and href.startswith("http") and href not in ls_tourneys_urls:
                            ls_tourneys_urls.append(href)
                            print(f"      Added URL: {href}")
                            found_links_strategy1 = True
                except TimeoutException: print(f"  Warning: Timed out waiting for cell '{selector}'.")
                except NoSuchElementException: print(f"  Warning: Could not find cell using selector '{selector}'.")
                except Exception as e: print(f"  Warning: Error processing cell '{selector}': {e}")

            if found_links_strategy1: print("--- Strategy 1 succeeded. ---")
            else:
                 print("--- Strategy 1 yielded no URLs. Trying Strategy 2. ---")
                 try:
                     print("--- Table HTML (for debugging Strategy 1 failure) ---")
                     print(events_table.get_attribute('outerHTML')[:1000] + "...")
                     print("----------------------------------------------------")
                 except Exception as log_e: print(f"Could not log table HTML: {log_e}")
        except TimeoutException: print(f"Error: Timed out waiting for the main events table (ID: {CURRENT_EVENTS_TABLE_ID}). Cannot proceed with Strategy 1 or 2.")
        except NoSuchElementException: print(f"Error: Could not find the main events table (ID: {CURRENT_EVENTS_TABLE_ID}). Cannot proceed with Strategy 1 or 2.")
        except Exception as e:
            print(f"An unexpected error occurred during Strategy 1: {e}")
            traceback.print_exc()

        # --- Strategy 2: Search for links within the table ---
        if not ls_tourneys_urls and 'events_table' in locals() and events_table:
             print("\n--- Strategy 2: Searching for links within the current events table ---")
             try:
                 all_table_forecast_links = events_table.find_elements(By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)
                 print(f"  Found {len(all_table_forecast_links)} '{RESULTS_FORECASTS_LINK_TEXT}' links within the table.")
                 found_links_strategy2 = False
                 if all_table_forecast_links:
                     print("  Filtering URLs found within the table...")
                     for link in all_table_forecast_links:
                         url = link.get_attribute("href")
                         if url and url.startswith("http") and url not in ls_tourneys_urls:
                             url_lower = url.lower(); is_relevant = 'atp' in url_lower or 'challenger' in url_lower
                             if is_relevant:
                                 ls_tourneys_urls.append(url); print(f"    Added relevant URL: {url}"); found_links_strategy2 = True
                             else: print(f"    Skipping URL (missing 'atp'/'challenger'): {url}")
                 else: print("  No links with the exact text found within the table.")
                 if found_links_strategy2: print("--- Strategy 2 succeeded. ---")
                 else: print("--- Strategy 2 yielded no relevant URLs. Trying Strategy 3. ---")
             except Exception as e:
                  print(f"An unexpected error occurred during Strategy 2: {e}"); traceback.print_exc()
                  print("--- Strategy 2 failed. Trying Strategy 3. ---")

        # --- Strategy 3: Fallback to searching the entire page ---
        if not ls_tourneys_urls:
             print("\n--- Strategy 3: Falling back to searching entire page for links ---")
             try:
                 wait.until(EC.presence_of_element_located((By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)))
                 all_page_forecast_links = driver.find_elements(By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)
                 print(f"  Found {len(all_page_forecast_links)} '{RESULTS_FORECASTS_LINK_TEXT}' links page-wide.")
                 found_links_strategy3 = False
                 if all_page_forecast_links:
                     print("  Filtering URLs found page-wide...")
                     for link in all_page_forecast_links:
                         url = link.get_attribute("href")
                         if url and url.startswith("http") and url not in ls_tourneys_urls:
                             url_lower = url.lower(); is_relevant = 'atp' in url_lower or 'challenger' in url_lower
                             if is_relevant:
                                 ls_tourneys_urls.append(url); print(f"    Added relevant URL: {url}"); found_links_strategy3 = True
                             else: print(f"    Skipping URL (missing 'atp'/'challenger'): {url}")
                 else: print("  No links with the exact text found anywhere on the page.")
                 if found_links_strategy3: print("--- Strategy 3 succeeded. ---")
                 else: print("--- Strategy 3 yielded no relevant URLs. ---")
             except TimeoutException: print("  Error: Timed out waiting for links during page-wide fallback search.")
             except NoSuchElementException: print("  Error: No 'Results and Forecasts' links found during page-wide fallback search.")
             except Exception as e: print(f"An unexpected error occurred during Strategy 3: {e}"); traceback.print_exc()

        print(f"\nFinished URL search. Found {len(ls_tourneys_urls)} relevant tournament URLs.")
        if not ls_tourneys_urls: print("WARNING: No relevant tournament URLs were found using any strategy.")

    except TimeoutException: print(f"Error: Page load timed out for {BASE_URL}")
    except WebDriverException as e: print(f"WebDriver error while getting tournament URLs: {e}"); traceback.print_exc()
    except Exception as e: print(f"An critical unexpected error occurred in tourneys_url function: {e}"); traceback.print_exc()
    finally:
        if driver: print("Closing WebDriver for tourneys_url..."); driver.quit(); print("WebDriver closed.")

    final_urls = list(dict.fromkeys(ls_tourneys_urls))
    if len(final_urls) < len(ls_tourneys_urls): print(f"Removed {len(ls_tourneys_urls) - len(final_urls)} duplicate URLs.")
    return final_urls


# MODIFIED probas_scraper to return (headers, data)
def probas_scraper(url: str) -> Tuple[List[str], List[Any]]:
    """
    Scrapes the probability table from a given Tennis Abstract tournament URL.
    Targets the table dynamically loaded into the 'forecast' span.
    Returns a tuple containing:
        - list[str]: The identified header strings.
        - list[Any]: The flat list of extracted table data points.
    Returns ([], []) if scraping fails or no data/headers found.
    """
    print(f"\nAttempting to scrape probability table from: {url}")
    driver = setup_driver()
    if driver is None:
        print(f"Failed to setup WebDriver in probas_scraper for {url}. Aborting.")
        return [], [] # Return empty lists on driver failure

    table_data = []
    identified_headers = [] # Initialize list to store headers

    try:
        print(f"Navigating to {url}...")
        driver.get(url)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)

        forecast_span_id = "forecast"
        try:
            print(f"Waiting for forecast span (ID: {forecast_span_id})...")
            forecast_span = wait.until(EC.presence_of_element_located((By.ID, forecast_span_id)))
            print("Forecast span found.")
            cell_locator = (By.CSS_SELECTOR, f"span#{forecast_span_id} table td")
            print(f"Waiting for table content (any 'td') within span#{forecast_span_id}...")
            wait.until(EC.presence_of_element_located(cell_locator))
            print("Table content appears to be loaded (found at least one 'td').")
            table_locator = (By.CSS_SELECTOR, f"span#{forecast_span_id} table")
            probability_table = forecast_span.find_element(*table_locator)
            print("Located the probability table element.")
        except TimeoutException:
            print(f"Error: Timed out waiting for the probability table content within span#{forecast_span_id} on {url}")
            # Log span content if timeout occurs after span is found
            try:
                 if 'forecast_span' in locals() and forecast_span:
                     print("--- Forecast Span HTML (Timeout Debug) ---"); print(forecast_span.get_attribute('outerHTML')[:1000] + "..."); print("------------------------------------------")
            except Exception as log_e: print(f"Could not log forecast span HTML: {log_e}")
            return [], [] # Return empty lists
        except NoSuchElementException:
             print(f"Error: Could not find the probability table within span#{forecast_span_id} on {url}")
             # Log span content if table not found after span is found
             try:
                 if 'forecast_span' in locals() and forecast_span:
                     print("--- Forecast Span HTML (No Table Debug) ---"); print(forecast_span.get_attribute('outerHTML')[:1000] + "..."); print("-------------------------------------------")
             except Exception as log_e: print(f"Could not log forecast span HTML: {log_e}")
             return [], [] # Return empty lists

        # --- Table Row Processing ---
        rows = probability_table.find_elements(By.TAG_NAME, "tr")
        print(f"Found {len(rows)} rows in the table.")
        if not rows:
             print("Warning: No rows found within the located table.")
             return [], [] # Return empty lists if no rows found

        header_pattern = re.compile(r"Player|R16|QF|SF|F|W") # Simple header check pattern
        header_found = False # Flag to ensure we only capture the first header row

        for i, row in enumerate(rows):
            cells = row.find_elements(By.XPATH, ".//td | .//th")
            if not cells:
                print(f"Skipping empty row {i+1}.")
                continue

            # --- Header Identification and Skipping Logic ---
            first_cell_text = cells[0].text.strip()
            is_likely_header = False
            # Check if it looks like a header row based on content
            if header_pattern.match(first_cell_text) and len(cells) > 1:
                 header_like_count = sum(1 for c in cells if header_pattern.match(c.text.strip()) or '%' in c.text)
                 if header_like_count >= len(cells) // 2:
                      is_likely_header = True

            if is_likely_header:
                 # **** CAPTURE HEADERS ****
                 if not header_found: # Only capture the first header row encountered
                     identified_headers = [c.text.strip() for c in cells]
                     # Handle potential empty strings in headers (like the one after 'Player')
                     identified_headers = [h if h else "Unknown" for h in identified_headers] # Replace empty strings
                     print(f"Captured Headers (Row {i+1}): {identified_headers}")
                     header_found = True
                 else:
                      print(f"Skipping likely repeated header row {i+1}: {' | '.join([c.text.strip() for c in cells[:4]])}...")
                 continue # Skip processing this row further (whether first or repeated header)
            # --- End Header Logic ---

            # --- Data Extraction (only if it's not a header row) ---
            row_data = []
            for cell in cells:
                cell_text = cell.text.replace('\u00a0', ' ').strip()
                if cell_text:
                    row_data.append(cell_text)
            table_data.extend(row_data) # Add extracted data to the flat list

        print(f"Extracted {len(table_data)} non-empty raw data points from the table (flat list).")

        # --- Data Post-processing (Percentage Conversion) ---
        processed_data = []
        for x in table_data:
             if isinstance(x, str) and "%" in x:
                 try:
                     processed_data.append(float(x.replace('%', '').strip()))
                 except ValueError:
                      print(f"Warning: Could not convert '{x}' to float. Keeping as string.")
                      processed_data.append(x)
             else:
                 processed_data.append(x)
        table_data = processed_data
        print("Data processing (percentage conversion) complete.")

        # --- Final Check ---
        if not identified_headers:
            print("Warning: Could not identify a header row during scraping.")
            # Optionally, provide default headers if a common structure is known
            # identified_headers = ['Player', 'Rxx', 'QF', 'SF', 'F', 'W'] # Example default

    except TimeoutException: print(f"Error: Page load or element search timed out for {url}")
    except NoSuchElementException as e: print(f"Error: Could not find a required element on {url}: {e}")
    except WebDriverException as e: print(f"WebDriver error during scraping of {url}: {e}"); traceback.print_exc()
    except Exception as e: print(f"An unexpected error occurred in probas_scraper for {url}: {e}"); traceback.print_exc()
    finally:
        if driver: print(f"Closing WebDriver for {url}..."); driver.quit(); print("WebDriver closed.")

    # Return the identified headers and the processed data list
    return identified_headers, table_data


# --- Example Usage (Modified to show headers) ---
if __name__ == "__main__":
    print("--- Testing tourneys_url ---")
    tournament_urls = tourneys_url()
    if tournament_urls:
        print(f"\nFound URLs:\n" + "\n".join(tournament_urls))
        if tournament_urls:
            first_url_to_scrape = tournament_urls[0]
            print(f"\n--- Testing probas_scraper on first URL: {first_url_to_scrape} ---")
            # Unpack the returned tuple
            scraped_headers, scraped_data = probas_scraper(first_url_to_scrape)
            if scraped_headers:
                print(f"\nScraped Headers: {scraped_headers}")
            else:
                print("\nNo headers were identified by the scraper.")
            if scraped_data:
                print(f"\nScraped data sample (first 50 elements):\n{scraped_data[:50]}...")
                print(f"\nTotal data elements scraped: {len(scraped_data)}")
            else:
                print(f"No data scraped from the first URL: {first_url_to_scrape}")
    else:
        print("\nNo tournament URLs found by any strategy.")
