# tennis_abstract_scraper.py (Full Code - Reviewed and Improved)

import time
import re # Import regex for filtering
from typing import List, Optional, Any

# Selenium imports
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException

# Webdriver Manager import
try:
    # Note: webdriver-manager is used here for local execution convenience.
    # In the GitHub Action, we install geckodriver directly, so webdriver-manager isn't strictly needed there.
    # However, keeping it allows the script to potentially run locally more easily.
    from webdriver_manager.firefox import GeckoDriverManager
except ImportError:
    print("Warning: webdriver-manager not found. Local execution might require manual geckodriver setup.")
    # Set GeckoDriverManager to None so setup_driver can handle its absence if needed locally.
    # The GitHub Action workflow installs geckodriver directly, so it won't rely on this.
    GeckoDriverManager = None

# --- Constants ---
BASE_URL = "http://www.tennisabstract.com/"
# Timeout for waiting for elements (in seconds)
WAIT_TIMEOUT = 15 # Increased slightly

# --- WebDriver Setup (Updated) ---
def setup_driver(max_retries=2) -> Optional[webdriver.Firefox]:
    """
    Sets up and returns a headless Firefox WebDriver instance with retries.
    Explicitly specifies geckodriver path for robustness in controlled environments (like Actions).
    Falls back to webdriver-manager if path fails or for local use.
    """
    options = FirefoxOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # options.add_argument("--disable-gpu") # Often not needed

    # Path where geckodriver is installed in the GitHub Action
    geckodriver_path_action = "/usr/local/bin/geckodriver"

    for attempt in range(max_retries):
        print(f"Attempt {attempt + 1} of {max_retries} to setup WebDriver...")
        driver = None # Initialize driver to None for this attempt
        try:
            # Try using the explicit path first (ideal for GitHub Actions)
            print(f"Attempting setup with explicit path: {geckodriver_path_action}")
            service = FirefoxService(executable_path=geckodriver_path_action)
            time.sleep(2) # Small delay before attempting connection
            driver = webdriver.Firefox(service=service, options=options)
            print("WebDriver setup successful using explicit path.")
            return driver # Return successfully initialized driver

        except (WebDriverException, FileNotFoundError) as e_path:
            print(f"Setup with explicit path failed: {e_path}")
            # Fallback to webdriver-manager if explicit path fails or if GeckoDriverManager is available (for local use)
            if GeckoDriverManager:
                print("Falling back to webdriver-manager...")
                try:
                    service = FirefoxService(GeckoDriverManager().install())
                    time.sleep(2)
                    driver = webdriver.Firefox(service=service, options=options)
                    print("WebDriver setup successful using webdriver-manager.")
                    return driver
                except (WebDriverException, ValueError) as e_wdm:
                     print(f"Setup with webdriver-manager also failed: {e_wdm}")
                     # Proceed to retry logic based on the original exception type if needed
                     e = e_wdm # Use the latest error for retry logic
                except Exception as e_other_wdm:
                     print(f"Unexpected error during webdriver-manager setup: {e_other_wdm}")
                     e = e_other_wdm
            else:
                print("webdriver-manager not available, cannot use fallback.")
                e = e_path # Use the original error for retry logic

            # Retry logic based on the error encountered
            if isinstance(e, WebDriverException) and "Read timed out" in str(e) and attempt < max_retries - 1:
                print("Timeout detected, retrying after longer delay...")
                time.sleep(5) # Wait longer before retrying on timeout
            elif attempt < max_retries - 1:
                 print(f"Retrying after delay due to error: {type(e).__name__}")
                 time.sleep(3) # Shorter delay for other errors before retry
            else:
                 print("Max retries reached or non-retriable error during setup.")
                 # Ensure driver is cleaned up if partially initialized during failed attempt
                 if driver:
                      driver.quit()
                 return None # Failed after all retries or non-retriable error

        except Exception as e_unexpected:
             # Catch other potential unexpected errors during setup
             print(f"Attempt {attempt + 1} failed: An unexpected error occurred during WebDriver setup: {e_unexpected}")
             if attempt < max_retries - 1:
                  time.sleep(3)
             else:
                  print("Max retries reached after unexpected error. Failed to setup WebDriver.")
                  if driver: # Ensure cleanup
                       driver.quit()
                  return None # Failed after all retries

    return None # Should not be reached if loop logic is correct, but added for safety


# --- Scraping Functions ---

def tourneys_url() -> List[str]:
    """
    Scrapes Tennis Abstract homepage to find URLs for ATP/Challenger tournament
    Results and Forecasts pages, targeting specific table sections.
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

        # Wait for the 'current-events' table to be present
        current_events_table = wait.until(EC.presence_of_element_located(
            (By.ID, "current-events")
        ))
        print("Found 'current-events' table.")

        # Find the specific cells for Men's Tour and Challenger Tour
        # XPath to find the <td> under the <th> containing "Current Men's Tour"
        mens_tour_cell_xpath = "//th[contains(text(), \"Current Men's Tour\")]/../following-sibling::tr/td[count(//th[contains(text(), \"Current Men's Tour\")]/preceding-sibling::th)+1]"
        # XPath to find the <td> under the <th> containing "Current Challenger Tour"
        challenger_tour_cell_xpath = "//th[contains(text(), \"Current Challenger Tour\")]/../following-sibling::tr/td[count(//th[contains(text(), \"Current Challenger Tour\")]/preceding-sibling::th)+1]"

        target_cells = []
        try:
            mens_cell = current_events_table.find_element(By.XPATH, mens_tour_cell_xpath)
            target_cells.append(mens_cell)
            print("Found Men's Tour cell.")
        except NoSuchElementException:
            print("Warning: Could not find the specific Men's Tour cell.")

        try:
            challenger_cell = current_events_table.find_element(By.XPATH, challenger_tour_cell_xpath)
            target_cells.append(challenger_cell)
            print("Found Challenger Tour cell.")
        except NoSuchElementException:
            print("Warning: Could not find the specific Challenger Tour cell.")

        if not target_cells:
             print("Error: Could not find any target cells for tournament links.")
             # Attempt fallback: search all links on page (less targeted)
             print("Falling back to searching all 'Results and Forecasts' links...")
             all_forecast_links = wait.until(EC.presence_of_all_elements_located(
                 (By.PARTIAL_LINK_TEXT, "Results and Forecasts")
             ))
             if not all_forecast_links:
                  print("Fallback failed: No 'Results and Forecasts' links found on page.")
                  return []
             else:
                  print(f"Found {len(all_forecast_links)} potential links via fallback.")
                  all_urls = [link.get_attribute("href") for link in all_forecast_links if link.get_attribute("href")]
                  ls_tourneys_urls = [
                      url for url in all_urls
                      if ('atp' in url.lower() or 'challenger' in url.lower()) and 'forecasts' in url.lower()
                  ]

        else:
            # Find links specifically within the targeted cells
            for cell in target_cells:
                forecast_links = cell.find_elements(By.PARTIAL_LINK_TEXT, "Results and Forecasts")
                for link in forecast_links:
                    href = link.get_attribute("href")
                    if href and href not in ls_tourneys_urls: # Avoid duplicates
                        ls_tourneys_urls.append(href)

        print(f"Found {len(ls_tourneys_urls)} relevant tournament URLs.")

    except TimeoutException:
        print(f"Error: Timed out waiting for elements on {BASE_URL}")
    except NoSuchElementException:
        print(f"Error: Could not find expected elements (e.g., 'current-events' table) on {BASE_URL}")
    except WebDriverException as e:
         print(f"WebDriver error while getting tournament URLs: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in tourneys_url: {e}")
    finally:
        if driver:
            driver.quit()
            print("WebDriver closed for tourneys_url.")

    return ls_tourneys_urls


def probas_scraper(url: str) -> List[Any]:
    """
    Scrapes the probability table from a given Tennis Abstract tournament URL.
    Targets the table dynamically loaded into the 'forecast' span.

    Args:
        url: The URL of the tournament forecast page.

    Returns:
        A list containing the text/data from the table cells, processed.
        Returns an empty list if scraping fails.
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

        # --- Locate the Table within the forecast span ---
        forecast_span_id = "forecast"
        try:
            # Wait for the span itself first
            wait.until(EC.presence_of_element_located((By.ID, forecast_span_id)))
            # Now wait specifically for a TABLE element *inside* that span
            # More specific wait: wait for a data cell (td) within the table that contains '%'
            cell_locator = (By.CSS_SELECTOR, f"span#{forecast_span_id} table td") # General cell
            # cell_locator_percent = (By.XPATH, f"//span[@id='{forecast_span_id}']//table//td[contains(text(), '%')]") # Cell with %

            print(f"Waiting for table content within span#{forecast_span_id}...")
            # Wait for at least one 'td' element to be present
            wait.until(EC.presence_of_element_located(cell_locator))
            print("Table content appears to be loaded.")

            # Locate the table now
            table_locator = (By.CSS_SELECTOR, f"span#{forecast_span_id} table")
            probability_table = driver.find_element(*table_locator)

        except TimeoutException:
            print(f"Error: Timed out waiting for the probability table content within span#{forecast_span_id} on {url}")
            return []
        except NoSuchElementException:
             print(f"Error: Could not find the probability table within span#{forecast_span_id} on {url}")
             return []

        # --- Extract Data from Table ---
        # Find all rows (tr) within the located table's body
        rows = probability_table.find_elements(By.TAG_NAME, "tr")
        print(f"Found {len(rows)} rows in the table.")

        if not rows:
             print("Warning: No rows found within the located table.")
             return []

        header_pattern = re.compile(r"Player|R16|QF|SF|F|W") # Pattern to identify header rows

        for i, row in enumerate(rows):
            cells = row.find_elements(By.XPATH, ".//td | .//th")
            if not cells: continue # Skip empty rows if any

            # Check if the row looks like a header row to skip it
            first_cell_text = cells[0].text.strip()
            # Check if first cell matches header pattern AND if other cells also look like headers (R16, QF etc or numeric %)
            is_likely_header = False
            if header_pattern.match(first_cell_text) and len(cells) > 1:
                 # Check if subsequent cells look like round names or percentages
                 if all(header_pattern.match(c.text.strip()) or '%' in c.text for c in cells[1:]):
                      is_likely_header = True

            if is_likely_header:
                 print(f"Skipping likely header row {i+1}: {' | '.join([c.text.strip() for c in cells[:3]])}...")
                 continue

            # Extract text from cells in data rows
            for cell in cells:
                cell_text = cell.text.strip().replace('\u00a0', ' ').strip()
                # Append even if empty, let processing handle it? Or skip here? Let's skip empty.
                if cell_text:
                    table_data.append(cell_text)

        print(f"Extracted {len(table_data)} non-empty raw data points from the table (excluding headers).")

        # --- Process Data ---
        # table_data = [x for x in table_data if x != ""] # Ensure no empty strings remain (already handled above)
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

        # Test scraping the first URL found
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

