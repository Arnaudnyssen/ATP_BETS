# tennis_abstract_scraper.py (More Robust tourneys_url)

import time
import re
from typing import List, Optional, Any
import os
import traceback # Import traceback for detailed error logging

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
WAIT_TIMEOUT = 20 # Increased timeout slightly for potentially slower CI environments
RESULTS_FORECASTS_LINK_TEXT = "Results and Forecasts"
CURRENT_EVENTS_TABLE_ID = "current-events"

# --- WebDriver Setup (No changes here from your provided version) ---
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
    options.add_argument('--log-level=1') # Reduce excessive logging from Chrome/Driver

    chromedriver_path_apt = "/usr/bin/chromedriver"
    chromedriver_path_wdm = None
    if ChromeDriverManager:
         try:
              # Explicitly specify version if needed, or let it find latest
              print("Attempting to install/use ChromeDriver via webdriver-manager...")
              chromedriver_path_wdm = ChromeDriverManager().install()
              print(f"webdriver-manager path: {chromedriver_path_wdm}")
         except Exception as e:
              print(f"Could not get path from webdriver-manager: {e}")
              print("Ensure network connectivity if webdriver-manager needs to download.")

    driver = None
    service = None
    try:
        # Prioritize Apt path if it exists (common in GH Actions)
        if os.path.exists(chromedriver_path_apt):
            print(f"Using chromedriver from apt path: {chromedriver_path_apt}")
            service = ChromeService(executable_path=chromedriver_path_apt)
            driver = webdriver.Chrome(service=service, options=options)
        # Fallback to webdriver-manager path if found
        elif chromedriver_path_wdm and os.path.exists(chromedriver_path_wdm):
             print(f"Using chromedriver from webdriver-manager path: {chromedriver_path_wdm}")
             service = ChromeService(executable_path=chromedriver_path_wdm)
             driver = webdriver.Chrome(service=service, options=options)
        # Final fallback: Assume chromedriver is in PATH
        else:
             print("Chromedriver not found at specific paths, attempting PATH...")
             # No service object needed if relying on PATH
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

# REVISED tourneys_url function with enhanced robustness and logging
def tourneys_url() -> List[str]:
    """
    Scrapes Tennis Abstract homepage to find URLs for ATP/Challenger tournament
    Results and Forecasts pages, using multiple strategies for robustness.
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
            # Explicitly wait for the table container first
            events_table = wait.until(EC.presence_of_element_located((By.ID, CURRENT_EVENTS_TABLE_ID)))
            print(f"Table with ID '{CURRENT_EVENTS_TABLE_ID}' found.")

            # Define selectors relative to the table
            # Assuming ATP is the 2nd column (td index 1) and Challenger is 3rd (td index 2) in the first data row (tr index 1, skipping header tr)
            # Note: CSS nth-child is 1-based.
            men_tour_cell_selector = f"table#{CURRENT_EVENTS_TABLE_ID} > tbody > tr:nth-child(1) > td:nth-child(2)" # Check if header row exists, might need tr:nth-child(2)
            challenger_tour_cell_selector = f"table#{CURRENT_EVENTS_TABLE_ID} > tbody > tr:nth-child(1) > td:nth-child(3)" # Check if header row exists, might need tr:nth-child(2)
            # It might be safer to find the *header* text ("Men's Tour", "Challenger Tour") and then get the corresponding cell below it.
            # For now, sticking to index-based as per original code.

            target_cells_selectors = [men_tour_cell_selector, challenger_tour_cell_selector]
            found_links_strategy1 = False

            for selector in target_cells_selectors:
                try:
                    # Wait for the specific cell to be present
                    target_cell = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    print(f"  Found target cell using selector: '{selector}'")
                    # Find links with the exact text within this cell
                    forecast_links = target_cell.find_elements(By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)
                    print(f"    Found {len(forecast_links)} '{RESULTS_FORECASTS_LINK_TEXT}' links in this cell.")

                    for link in forecast_links:
                        href = link.get_attribute("href")
                        # Basic check if href looks like a valid URL and avoid duplicates
                        if href and href.startswith("http") and href not in ls_tourneys_urls:
                            # No extra filtering needed here, selector is specific
                            ls_tourneys_urls.append(href)
                            print(f"      Added URL: {href}")
                            found_links_strategy1 = True

                except TimeoutException:
                    print(f"  Warning: Timed out waiting for cell '{selector}'.")
                except NoSuchElementException:
                    print(f"  Warning: Could not find cell using selector '{selector}'.")
                except Exception as e:
                     print(f"  Warning: Error processing cell '{selector}': {e}")

            if found_links_strategy1:
                 print("--- Strategy 1 succeeded. ---")
            else:
                 print("--- Strategy 1 yielded no URLs. Trying Strategy 2. ---")
                 # Optionally log table HTML if strategy 1 fails completely
                 try:
                     print("--- Table HTML (for debugging Strategy 1 failure) ---")
                     print(events_table.get_attribute('outerHTML')[:1000] + "...") # Log first 1000 chars
                     print("----------------------------------------------------")
                 except Exception as log_e:
                     print(f"Could not log table HTML: {log_e}")


        except TimeoutException:
            print(f"Error: Timed out waiting for the main events table (ID: {CURRENT_EVENTS_TABLE_ID}). Cannot proceed with Strategy 1 or 2.")
            # Skip directly to Strategy 3 (page-wide search) if table isn't found
        except NoSuchElementException:
            print(f"Error: Could not find the main events table (ID: {CURRENT_EVENTS_TABLE_ID}). Cannot proceed with Strategy 1 or 2.")
            # Skip directly to Strategy 3
        except Exception as e:
            print(f"An unexpected error occurred during Strategy 1: {e}")
            traceback.print_exc()
            # Try subsequent strategies

        # --- Strategy 2: Search for links within the table (if Strategy 1 failed but table was found) ---
        if not ls_tourneys_urls and 'events_table' in locals() and events_table: # Only run if Strategy 1 failed AND table was found
             print("\n--- Strategy 2: Searching for links within the current events table ---")
             try:
                 # Find all links with the specific text *within* the table element
                 all_table_forecast_links = events_table.find_elements(By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)
                 print(f"  Found {len(all_table_forecast_links)} '{RESULTS_FORECASTS_LINK_TEXT}' links within the table.")
                 found_links_strategy2 = False

                 if all_table_forecast_links:
                     print("  Filtering URLs found within the table...")
                     for link in all_table_forecast_links:
                         url = link.get_attribute("href")
                         if url and url.startswith("http") and url not in ls_tourneys_urls:
                             url_lower = url.lower()
                             # Apply the keyword filter
                             is_relevant = 'atp' in url_lower or 'challenger' in url_lower
                             if is_relevant:
                                 ls_tourneys_urls.append(url)
                                 print(f"    Added relevant URL: {url}")
                                 found_links_strategy2 = True
                             else:
                                 print(f"    Skipping URL (missing 'atp'/'challenger'): {url}")
                 else:
                      print("  No links with the exact text found within the table.")

                 if found_links_strategy2:
                      print("--- Strategy 2 succeeded. ---")
                 else:
                      print("--- Strategy 2 yielded no relevant URLs. Trying Strategy 3. ---")

             except Exception as e:
                  print(f"An unexpected error occurred during Strategy 2: {e}")
                  traceback.print_exc()
                  print("--- Strategy 2 failed. Trying Strategy 3. ---")


        # --- Strategy 3: Fallback to searching the entire page (if Strategies 1 & 2 failed) ---
        if not ls_tourneys_urls:
             print("\n--- Strategy 3: Falling back to searching entire page for links ---")
             try:
                 # Wait for *any* link with the text to be present on the page
                 wait.until(EC.presence_of_element_located((By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)))
                 all_page_forecast_links = driver.find_elements(By.LINK_TEXT, RESULTS_FORECASTS_LINK_TEXT)
                 print(f"  Found {len(all_page_forecast_links)} '{RESULTS_FORECASTS_LINK_TEXT}' links page-wide.")
                 found_links_strategy3 = False

                 if all_page_forecast_links:
                     print("  Filtering URLs found page-wide...")
                     for link in all_page_forecast_links:
                         url = link.get_attribute("href")
                         if url and url.startswith("http") and url not in ls_tourneys_urls:
                             url_lower = url.lower()
                             # Apply the keyword filter
                             is_relevant = 'atp' in url_lower or 'challenger' in url_lower
                             if is_relevant:
                                 ls_tourneys_urls.append(url)
                                 print(f"    Added relevant URL: {url}")
                                 found_links_strategy3 = True
                             else:
                                 print(f"    Skipping URL (missing 'atp'/'challenger'): {url}")
                 else:
                     print("  No links with the exact text found anywhere on the page.")


                 if found_links_strategy3:
                      print("--- Strategy 3 succeeded. ---")
                 else:
                      print("--- Strategy 3 yielded no relevant URLs. ---")


             except TimeoutException:
                  print("  Error: Timed out waiting for links during page-wide fallback search.")
             except NoSuchElementException:
                   print("  Error: No 'Results and Forecasts' links found during page-wide fallback search.")
             except Exception as e:
                  print(f"An unexpected error occurred during Strategy 3: {e}")
                  traceback.print_exc()

        # --- Final Result ---
        print(f"\nFinished URL search. Found {len(ls_tourneys_urls)} relevant tournament URLs.")
        if not ls_tourneys_urls:
             print("WARNING: No relevant tournament URLs were found using any strategy.")
             # Consider logging full page source here if debugging is difficult
             # try:
             #     print("--- Full Page Source (for debugging no URLs found) ---")
             #     print(driver.page_source[:2000] + "...") # Log first 2000 chars
             #     print("------------------------------------------------------")
             # except Exception as log_e:
             #     print(f"Could not log page source: {log_e}")


    except TimeoutException:
        print(f"Error: Page load timed out for {BASE_URL}")
    except WebDriverException as e:
         print(f"WebDriver error while getting tournament URLs: {e}")
         traceback.print_exc()
    except Exception as e:
        print(f"An critical unexpected error occurred in tourneys_url function: {e}")
        traceback.print_exc()
    finally:
        if driver:
            print("Closing WebDriver for tourneys_url...")
            driver.quit()
            print("WebDriver closed.")

    # Ensure uniqueness just in case duplicates slipped through
    final_urls = list(dict.fromkeys(ls_tourneys_urls))
    if len(final_urls) < len(ls_tourneys_urls):
        print(f"Removed {len(ls_tourneys_urls) - len(final_urls)} duplicate URLs.")

    return final_urls


# --- probas_scraper function remains the same (assuming it works when given a valid URL) ---
# Make sure setup_driver() is robust as called from here too.
def probas_scraper(url: str) -> List[Any]:
    """
    Scrapes the probability table from a given Tennis Abstract tournament URL.
    Targets the table dynamically loaded into the 'forecast' span.
    (Content is the same as your provided tennis_abstract_scraper_chrome_03 version)
    """
    print(f"\nAttempting to scrape probability table from: {url}")
    driver = setup_driver()
    if driver is None:
        print(f"Failed to setup WebDriver in probas_scraper for {url}. Aborting.")
        return []

    table_data = []
    try:
        print(f"Navigating to {url}...")
        driver.get(url)
        wait = WebDriverWait(driver, WAIT_TIMEOUT) # Use the same timeout

        forecast_span_id = "forecast"
        try:
            # Wait for the span itself first
            print(f"Waiting for forecast span (ID: {forecast_span_id})...")
            forecast_span = wait.until(EC.presence_of_element_located((By.ID, forecast_span_id)))
            print("Forecast span found.")

            # Now wait for *any* table cell to appear *within* that span
            # This indicates the table content is likely loading/loaded
            cell_locator = (By.CSS_SELECTOR, f"span#{forecast_span_id} table td")
            print(f"Waiting for table content (any 'td') within span#{forecast_span_id}...")
            wait.until(EC.presence_of_element_located(cell_locator))
            print("Table content appears to be loaded (found at least one 'td').")

            # Locate the table itself within the span
            table_locator = (By.CSS_SELECTOR, f"span#{forecast_span_id} table")
            probability_table = forecast_span.find_element(*table_locator) # Find relative to span
            print("Located the probability table element.")

        except TimeoutException:
            print(f"Error: Timed out waiting for the probability table content within span#{forecast_span_id} on {url}")
            # Log span content if timeout occurs after span is found
            try:
                 if 'forecast_span' in locals() and forecast_span:
                     print("--- Forecast Span HTML (Timeout Debug) ---")
                     print(forecast_span.get_attribute('outerHTML')[:1000] + "...")
                     print("------------------------------------------")
            except Exception as log_e:
                 print(f"Could not log forecast span HTML: {log_e}")
            return []
        except NoSuchElementException:
             print(f"Error: Could not find the probability table within span#{forecast_span_id} on {url}")
             # Log span content if table not found after span is found
             try:
                 if 'forecast_span' in locals() and forecast_span:
                     print("--- Forecast Span HTML (No Table Debug) ---")
                     print(forecast_span.get_attribute('outerHTML')[:1000] + "...")
                     print("-------------------------------------------")
             except Exception as log_e:
                 print(f"Could not log forecast span HTML: {log_e}")
             return []

        # --- Table Row Processing (same as before) ---
        rows = probability_table.find_elements(By.TAG_NAME, "tr")
        print(f"Found {len(rows)} rows in the table.")
        if not rows:
             print("Warning: No rows found within the located table.")
             return [] # Return empty list if no rows found

        header_pattern = re.compile(r"Player|R16|QF|SF|F|W") # Simple header check pattern

        for i, row in enumerate(rows):
            # Get both th and td elements within the row
            cells = row.find_elements(By.XPATH, ".//td | .//th")
            if not cells:
                print(f"Skipping empty row {i+1}.")
                continue

            # --- Simple Header Row Skipping Logic ---
            # Check if the first cell looks like a header and if most other cells also look like headers or percentages
            first_cell_text = cells[0].text.strip()
            is_likely_header = False
            if header_pattern.match(first_cell_text) and len(cells) > 1:
                 # Check if most other cells are headers or contain '%'
                 header_like_count = sum(1 for c in cells if header_pattern.match(c.text.strip()) or '%' in c.text)
                 if header_like_count >= len(cells) // 2: # If at least half look like headers/percentages
                      is_likely_header = True

            if is_likely_header:
                 print(f"Skipping likely header row {i+1}: {' | '.join([c.text.strip() for c in cells[:4]])}...")
                 continue
            # --- End Header Skipping ---

            # Extract text from cells in the data row
            row_data = []
            for cell in cells:
                # Replace non-breaking spaces (&nbsp;) with regular spaces and strip whitespace
                cell_text = cell.text.replace('\u00a0', ' ').strip()
                # Only add non-empty text to avoid blank entries from empty cells
                if cell_text:
                    row_data.append(cell_text)

            # Add extracted cell data to the main list
            # This creates a flat list, which p_sack_preproc.py expects
            table_data.extend(row_data)


        print(f"Extracted {len(table_data)} non-empty raw data points from the table (flat list).")

        # --- Data Post-processing (Percentage Conversion - same as before) ---
        processed_data = []
        for x in table_data:
             if isinstance(x, str) and "%" in x: # Check type before using string method
                 try:
                     # Attempt to convert percentage string to float
                     processed_data.append(float(x.replace('%', '').strip()))
                 except ValueError:
                      # If conversion fails, keep the original string but log a warning
                      print(f"Warning: Could not convert '{x}' to float. Keeping as string.")
                      processed_data.append(x)
             else:
                 # Keep non-percentage elements as they are
                 processed_data.append(x)
        table_data = processed_data # Update table_data with processed values
        print("Data processing (percentage conversion) complete.")

    except TimeoutException:
        print(f"Error: Page load or element search timed out for {url}")
    except NoSuchElementException as e:
        print(f"Error: Could not find a required element on {url}: {e}")
    except WebDriverException as e:
         print(f"WebDriver error during scraping of {url}: {e}")
         traceback.print_exc()
    except Exception as e:
        print(f"An unexpected error occurred in probas_scraper for {url}: {e}")
        traceback.print_exc()
    finally:
        if driver:
            print(f"Closing WebDriver for {url}...")
            driver.quit()
            print("WebDriver closed.")

    return table_data


# --- Example Usage (remains the same) ---
if __name__ == "__main__":
    print("--- Testing tourneys_url ---")
    tournament_urls = tourneys_url()
    if tournament_urls:
        print(f"\nFound URLs:\n" + "\n".join(tournament_urls))
        # Scrape only the first URL for testing purposes
        if tournament_urls:
            first_url_to_scrape = tournament_urls[0]
            print(f"\n--- Testing probas_scraper on first URL: {first_url_to_scrape} ---")
            scraped_data = probas_scraper(first_url_to_scrape)
            if scraped_data:
                print(f"\nScraped data sample (first 50 elements):\n{scraped_data[:50]}...")
                print(f"\nTotal elements scraped: {len(scraped_data)}")
            else:
                print(f"No data scraped from the first URL: {first_url_to_scrape}")
    else:
        print("\nNo tournament URLs found by any strategy.")
