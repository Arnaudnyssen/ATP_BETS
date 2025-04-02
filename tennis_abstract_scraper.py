# tennis_abstract_scraper.py (Corrected URL Filter Logic)

import time
import re
from typing import List, Optional, Any
import os

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
WAIT_TIMEOUT = 15

# --- WebDriver Setup (Using Chrome - No changes here) ---
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

    chromedriver_path_apt = "/usr/bin/chromedriver"
    chromedriver_path_wdm = None
    if ChromeDriverManager:
         try:
              chromedriver_path_wdm = ChromeDriverManager().install()
              print(f"webdriver-manager path: {chromedriver_path_wdm}")
         except Exception as e:
              print(f"Could not get path from webdriver-manager: {e}")

    driver = None
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
        if driver: driver.quit()
        return None
    except Exception as e:
         print(f"An unexpected error occurred during Chrome WebDriver setup: {e}")
         if driver: driver.quit()
         return None

# --- Scraping Functions ---

# REVISED tourneys_url function with corrected filter
def tourneys_url() -> List[str]:
    """
    Scrapes Tennis Abstract homepage to find URLs for ATP/Challenger tournament
    Results and Forecasts pages, using corrected filtering logic.
    """
    print(f"Attempting to find tournament URLs from {BASE_URL}...")
    driver = setup_driver()
    if driver is None:
        print("Failed to setup WebDriver in tourneys_url. Aborting.")
        return []

    ls_tourneys_urls = []
    try:
        driver.get(BASE_URL)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)

        # --- Primary URL Extraction Logic (Revised Filter) ---
        print("Attempting URL extraction using CSS selectors for specific columns...")
        men_tour_cell_selector = "table#current-events > tbody > tr:first-child > td:nth-child(2)"
        challenger_tour_cell_selector = "table#current-events > tbody > tr:first-child > td:nth-child(3)"
        target_cells_selectors = [men_tour_cell_selector, challenger_tour_cell_selector]
        found_links_primary = False

        for selector in target_cells_selectors:
            try:
                target_cell = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                print(f"Found target cell using selector: '{selector}'")
                # Find links with the exact text "Results and Forecasts"
                forecast_links = target_cell.find_elements(By.LINK_TEXT, "Results and Forecasts")
                print(f"  Found {len(forecast_links)} 'Results and Forecasts' links in this cell.")

                for link in forecast_links:
                    href = link.get_attribute("href")
                    # Basic check if href looks like a valid URL and avoid duplicates
                    if href and href.startswith("http") and href not in ls_tourneys_urls:
                        # *** CORRECTED FILTER for Primary Method ***
                        # Since we target specific cells, finding the link text is enough.
                        # No need to check URL content here, reduces brittleness.
                        ls_tourneys_urls.append(href)
                        print(f"    Added relevant URL: {href}")
                        found_links_primary = True
                        # *** END CORRECTED FILTER ***

            except TimeoutException:
                print(f"Warning: Timed out waiting for cell '{selector}'.")
            except NoSuchElementException:
                print(f"Warning: Could not find cell using selector '{selector}'.")
            except Exception as e:
                 print(f"Warning: Error processing cell '{selector}': {e}")


        # --- Fallback Logic (Revised Filter) ---
        if not found_links_primary:
             print("Primary CSS selector method yielded no relevant URLs. Falling back...")
             try:
                 wait.until(EC.presence_of_element_located((By.LINK_TEXT, "Results and Forecasts")))
                 all_forecast_links = driver.find_elements(By.LINK_TEXT, "Results and Forecasts")
                 print(f"  [Fallback] Found {len(all_forecast_links)} potential links page-wide.")
                 if not all_forecast_links: return [] # Exit if fallback finds nothing

                 all_urls = [link.get_attribute("href") for link in all_forecast_links if link.get_attribute("href")]
                 print("  [Fallback] Filtering URLs...")
                 for url in all_urls:
                     url_lower = url.lower()
                     # *** CORRECTED FILTER for Fallback Method ***
                     # Check only for 'atp' or 'challenger' in the URL, remove 'forecasts' check
                     contains_keyword = 'atp' in url_lower or 'challenger' in url_lower
                     # *** END CORRECTED FILTER ***

                     if contains_keyword:
                         if url not in ls_tourneys_urls:
                             ls_tourneys_urls.append(url)
                             print(f"    [Fallback] Added relevant URL: {url}")
                     else:
                         # Log reason for skipping (only one reason possible now)
                         print(f"    [Fallback] Skipping URL: {url} (Reason: 'atp'/'challenger' missing)")

             except TimeoutException:
                  print("  [Fallback] Timed out waiting for links during fallback.")
                  return []
             except NoSuchElementException:
                   print("  [Fallback] No 'Results and Forecasts' links found during fallback.")
                   return []

        print(f"Found {len(ls_tourneys_urls)} relevant tournament URLs after all methods.")

    # ... (rest of the try/except/finally block remains the same) ...
    except TimeoutException:
        print(f"Error: Timed out waiting for elements on {BASE_URL}")
    except NoSuchElementException:
        print(f"Error: Could not find expected elements (e.g., 'current-events' table) on {BASE_URL}")
    except WebDriverException as e:
         print(f"WebDriver error while getting tournament URLs: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in tourneys_url: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()
            print("WebDriver closed for tourneys_url.")

    return ls_tourneys_urls


# --- probas_scraper function remains the same as in tennis_abstract_scraper_chrome_03 ---
def probas_scraper(url: str) -> List[Any]:
    """
    Scrapes the probability table from a given Tennis Abstract tournament URL.
    Targets the table dynamically loaded into the 'forecast' span.
    """
    print(f"Attempting to scrape probability table from: {url}")
    driver = setup_driver()
    if driver is None:
        print(f"Failed to setup WebDriver in probas_scraper for {url}. Aborting.")
        return []

    table_data = []
    try:
        driver.get(url)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)

        forecast_span_id = "forecast"
        try:
            wait.until(EC.presence_of_element_located((By.ID, forecast_span_id)))
            cell_locator = (By.CSS_SELECTOR, f"span#{forecast_span_id} table td")
            print(f"Waiting for table content within span#{forecast_span_id}...")
            wait.until(EC.presence_of_element_located(cell_locator))
            print("Table content appears to be loaded.")
            table_locator = (By.CSS_SELECTOR, f"span#{forecast_span_id} table")
            probability_table = driver.find_element(*table_locator)

        except TimeoutException:
            print(f"Error: Timed out waiting for the probability table content within span#{forecast_span_id} on {url}")
            return []
        except NoSuchElementException:
             print(f"Error: Could not find the probability table within span#{forecast_span_id} on {url}")
             return []

        rows = probability_table.find_elements(By.TAG_NAME, "tr")
        print(f"Found {len(rows)} rows in the table.")
        if not rows:
             print("Warning: No rows found within the located table.")
             return []

        header_pattern = re.compile(r"Player|R16|QF|SF|F|W")

        for i, row in enumerate(rows):
            cells = row.find_elements(By.XPATH, ".//td | .//th")
            if not cells: continue

            first_cell_text = cells[0].text.strip()
            is_likely_header = False
            if header_pattern.match(first_cell_text) and len(cells) > 1:
                 if all(header_pattern.match(c.text.strip()) or '%' in c.text for c in cells[1:]):
                      is_likely_header = True

            if is_likely_header:
                 print(f"Skipping likely header row {i+1}: {' | '.join([c.text.strip() for c in cells[:3]])}...")
                 continue

            for cell in cells:
                cell_text = cell.text.strip().replace('\u00a0', ' ').strip()
                if cell_text:
                    table_data.append(cell_text)

        print(f"Extracted {len(table_data)} non-empty raw data points from the table (excluding headers).")

        processed_data = []
        for x in table_data:
             if "%" in x:
                 try:
                     processed_data.append(float(x.replace('%', '').strip()))
                 except ValueError:
                      print(f"Warning: Could not convert '{x}' to float. Keeping as string.")
                      processed_data.append(x)
             else:
                 processed_data.append(x)
        table_data = processed_data
        print("Data processing (percentage conversion) complete.")

    except TimeoutException:
        print(f"Error: Page load or element search timed out for {url}")
    except NoSuchElementException as e:
        print(f"Error: Could not find a required element on {url}: {e}")
    except WebDriverException as e:
         print(f"WebDriver error during scraping of {url}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in probas_scraper for {url}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()
            print(f"WebDriver closed for {url}.")

    return table_data

# --- Example Usage ---
if __name__ == "__main__":
    print("--- Testing tourneys_url ---")
    tournament_urls = tourneys_url()
    if tournament_urls:
        print(f"\nFound URLs (showing max 5):\n{tournament_urls[:5]}...")
        if tournament_urls:
            first_url_to_scrape = tournament_urls[0]
            print(f"\n--- Testing probas_scraper on first URL: {first_url_to_scrape} ---")
            scraped_data = probas_scraper(first_url_to_scrape)
            if scraped_data:
                print(f"\nScraped data sample (first 50 elements):\n{scraped_data[:50]}...")
            else:
                print(f"No data scraped from the first URL: {first_url_to_scrape}")
    else:
        print("No tournament URLs found.")

