# tennis_abstract_scraper.py (Improved based on HTML source)

import time
from typing import List, Optional, Any
import re # Import regex for filtering

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
    from webdriver_manager.firefox import GeckoDriverManager
except ImportError:
    print("Error: webdriver-manager not found. Please install it: pip install webdriver-manager")
    GeckoDriverManager = None

# --- Constants ---
BASE_URL = "http://www.tennisabstract.com/"
# Timeout for waiting for elements (in seconds)
WAIT_TIMEOUT = 15 # Increased slightly for potentially slower loads/JS execution

# --- WebDriver Setup ---
def setup_driver() -> Optional[webdriver.Firefox]:
    """Sets up and returns a headless Firefox WebDriver instance."""
    if GeckoDriverManager is None:
         print("Cannot setup driver because webdriver-manager is not available.")
         return None
    try:
        options = FirefoxOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        # Suppress webdriver-manager logs if desired
        # os.environ['WDM_LOG_LEVEL'] = '0'
        service = FirefoxService(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=options)
        print("WebDriver setup successful.")
        return driver
    except (WebDriverException, ValueError) as e: # Catch specific webdriver-manager errors too
        print(f"Error setting up WebDriver: {e}")
        return None
    except Exception as e:
         print(f"An unexpected error occurred during WebDriver setup: {e}")
         return None

# --- Scraping Functions ---

def tourneys_url() -> List[str]:
    """
    Scrapes Tennis Abstract homepage to find URLs for ATP/Challenger tournament
    Results and Forecasts pages, targeting specific table sections.
    """
    print(f"Attempting to find tournament URLs from {BASE_URL}...")
    driver = setup_driver()
    if driver is None:
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
             return []

        # Find links specifically within these cells
        for cell in target_cells:
            # Find links containing "Results and Forecasts" within the current cell
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
        return []

    table_data = []
    try:
        driver.get(url)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)

        # --- Locate the Table within the forecast span ---
        # The content is loaded into <span id="forecast">. We need to wait for the table *inside* it.
        forecast_span_id = "forecast"
        try:
            # Wait for the span itself first (quick check)
            wait.until(EC.presence_of_element_located((By.ID, forecast_span_id)))
            # Now wait specifically for a TABLE element *inside* that span
            # We can also wait for a specific piece of content that indicates loading is complete,
            # like a cell containing '%'.
            table_locator = (By.CSS_SELECTOR, f"span#{forecast_span_id} table")
            # More specific wait: wait for a data cell (td) within the table
            cell_locator = (By.CSS_SELECTOR, f"span#{forecast_span_id} table td")

            print(f"Waiting for table content within span#{forecast_span_id}...")
            # Wait for at least one 'td' element to be present within the table in the span
            wait.until(EC.presence_of_element_located(cell_locator))
            print("Table content appears to be loaded.")

            # Locate the table now that we know it's loaded
            probability_table = driver.find_element(*table_locator) # Use '*' to unpack the locator tuple

        except TimeoutException:
            print(f"Error: Timed out waiting for the probability table content within span#{forecast_span_id} on {url}")
            return []
        except NoSuchElementException:
             print(f"Error: Could not find the probability table within span#{forecast_span_id} on {url}")
             return []

        # --- Extract Data from Table ---
        # Find all rows (tr) within the located table's body (tbody might exist implicitly or explicitly)
        rows = probability_table.find_elements(By.TAG_NAME, "tr")
        print(f"Found {len(rows)} rows in the table.")

        if not rows:
             print("Warning: No rows found within the located table.")
             return []

        header_pattern = re.compile(r"Player|R16|QF|SF|F|W") # Pattern to identify header rows

        for i, row in enumerate(rows):
            # Find all cells (td or th) within each row
            cells = row.find_elements(By.XPATH, ".//td | .//th") # Get both header and data cells

            # Check if the row looks like a header row to skip it
            first_cell_text = cells[0].text.strip() if cells else ""
            if header_pattern.match(first_cell_text) and len(cells) > 1: # Basic check for header row
                 print(f"Skipping potential header row {i+1}: {' | '.join([c.text.strip() for c in cells[:3]])}...")
                 continue

            # Extract text from cells in data rows
            for cell in cells:
                cell_text = cell.text.strip()
                # Handle potential non-breaking spaces if they cause issues
                cell_text = cell_text.replace('\u00a0', ' ').strip()
                if cell_text: # Only append non-empty cell text
                    table_data.append(cell_text)

        print(f"Extracted {len(table_data)} raw data points from the table (excluding headers).")

        # --- Process Data ---
        table_data = [x for x in table_data if x != ""] # Ensure no empty strings remain
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
        print(f"\nFound URLs:\n{tournament_urls[:5]}...") # Print first 5

        # Test scraping the first URL found
        if tournament_urls:
            print(f"\n--- Testing probas_scraper on first URL: {tournament_urls[0]} ---")
            scraped_data = probas_scraper(tournament_urls[0])
            if scraped_data:
                print(f"\nScraped data sample (first 50 elements):\n{scraped_data[:50]}...")
            else:
                print("No data scraped from the first URL.")
    else:
        print("No tournament URLs found.")
