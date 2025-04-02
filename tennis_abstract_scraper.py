# tennis_abstract_scraper.py (Using Chrome/Chromedriver)

import time
import re
from typing import List, Optional, Any
import os # Added for checking driver path existence

# Selenium imports - UPDATED for Chrome
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException

# Webdriver Manager import - Can still be used for local Chrome testing
try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    print("Warning: webdriver-manager not found. Local execution might require manual chromedriver setup.")
    ChromeDriverManager = None

# --- Constants ---
BASE_URL = "http://www.tennisabstract.com/"
WAIT_TIMEOUT = 15

# --- WebDriver Setup (UPDATED for Chrome) ---
def setup_driver() -> Optional[webdriver.Chrome]:
    """
    Sets up and returns a headless Chrome WebDriver instance.
    Uses chromedriver installed via apt in GitHub Actions.
    Falls back to webdriver-manager for local execution if available.
    """
    print("Setting up Chrome WebDriver...")
    options = ChromeOptions()
    options.add_argument("--headless=new") # Modern headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu") # Often recommended for headless
    options.add_argument("--window-size=1920,1080") # Specify window size

    # Path where chromedriver is typically installed by apt
    chromedriver_path_apt = "/usr/bin/chromedriver"
    # Path where webdriver-manager might install it
    chromedriver_path_wdm = None
    if ChromeDriverManager:
         try:
              # Attempt to get path from webdriver-manager without installing if possible,
              # or just let it install/cache locally. For CI, we rely on apt path.
              # Note: .install() will download if not found locally.
              chromedriver_path_wdm = ChromeDriverManager().install()
              print(f"webdriver-manager path: {chromedriver_path_wdm}")
         except Exception as e:
              print(f"Could not get path from webdriver-manager: {e}")


    driver = None
    try:
        # Prioritize the apt path expected in GitHub Actions
        if os.path.exists(chromedriver_path_apt):
            print(f"Using chromedriver from apt path: {chromedriver_path_apt}")
            service = ChromeService(executable_path=chromedriver_path_apt)
            driver = webdriver.Chrome(service=service, options=options)
        elif chromedriver_path_wdm and os.path.exists(chromedriver_path_wdm):
             # Fallback to webdriver-manager path if apt path doesn't exist (local use)
             print(f"Using chromedriver from webdriver-manager path: {chromedriver_path_wdm}")
             service = ChromeService(executable_path=chromedriver_path_wdm)
             driver = webdriver.Chrome(service=service, options=options)
        else:
             # Last resort: Let Selenium try to find chromedriver in PATH
             print("Chromedriver not found at specific paths, attempting PATH...")
             driver = webdriver.Chrome(options=options) # May fail if not in PATH

        print("Chrome WebDriver setup successful.")
        return driver

    except WebDriverException as e:
        print(f"WebDriver setup failed: {e}")
        if driver:
             driver.quit()
        return None
    except Exception as e:
         print(f"An unexpected error occurred during Chrome WebDriver setup: {e}")
         if driver:
              driver.quit()
         return None


# --- Scraping Functions (No changes needed inside these for Chrome vs Firefox typically) ---

def tourneys_url() -> List[str]:
    """
    Scrapes Tennis Abstract homepage to find URLs for ATP/Challenger tournament
    Results and Forecasts pages, targeting specific table sections.
    """
    print(f"Attempting to find tournament URLs from {BASE_URL}...")
    # Calls the updated setup_driver which now returns a Chrome driver
    driver = setup_driver()
    if driver is None:
        print("Failed to setup WebDriver in tourneys_url. Aborting.")
        return []

    ls_tourneys_urls = []
    try:
        driver.get(BASE_URL)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        current_events_table = wait.until(EC.presence_of_element_located(
            (By.ID, "current-events")
        ))
        print("Found 'current-events' table.")

        mens_tour_cell_xpath = "//th[contains(text(), \"Current Men's Tour\")]/../following-sibling::tr/td[count(//th[contains(text(), \"Current Men's Tour\")]/preceding-sibling::th)+1]"
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
            for cell in target_cells:
                forecast_links = cell.find_elements(By.PARTIAL_LINK_TEXT, "Results and Forecasts")
                for link in forecast_links:
                    href = link.get_attribute("href")
                    if href and href not in ls_tourneys_urls:
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
    """
    print(f"Attempting to scrape probability table from: {url}")
    # Calls the updated setup_driver which now returns a Chrome driver
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
