# results_scraper.py (v1 revised)
# Scrapes completed match results from Tennis Abstract tournament pages.
# Reads tournament list from processed_comparison_*.csv and URLs from sackmann_matchups_*.csv.

import pandas as pd
import numpy as np
import time
import traceback
import re
import os
import glob
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

# --- Utility Functions ---
try:
    # Reuse functions from process_data for consistency
    from process_data import create_merge_key, preprocess_player_name, find_latest_csv
except ImportError:
    print("ERROR: Cannot import helper functions from process_data.py. Ensure it's accessible.")
    # Define fallbacks or exit
    def create_merge_key(text: str) -> str: return "" # Placeholder
    def preprocess_player_name(name: str) -> Tuple[str, str]: return name, "" # Placeholder
    def find_latest_csv(directory: str, pattern: str) -> Optional[str]: return None # Placeholder
    # exit()

# --- Selenium/Requests Imports ---
import requests
from bs4 import BeautifulSoup

# --- Constants ---
DATA_DIR = "data_archive"
PROCESSED_CSV_PATTERN = "processed_comparison_*.csv" # Input for relevant tournaments
SACKMANN_CSV_PATTERN = "sackmann_matchups_*.csv"     # Input for URLs
RESULTS_OUTPUT_FILENAME_BASE = "match_results"
DATE_FORMAT = "%Y%m%d"
REQUEST_TIMEOUT = 20
# Updated Regex to better handle seeds/stati and capture winner/loser links
RESULT_REGEX = re.compile(
    r"^(?P<Round>[A-Za-z0-9]+):\s*" # Round indicator
    r"(?:.*?)" # Non-greedy match for optional winner seed/status before link
    r"<a[^>]*href=['\"](?P<WinnerURL>[^'\"]*)['\"][^>]*>(?P<WinnerName>.*?)<\/a>.*?" # Winner Link + Name
    r"\s+d\.\s+" # Defeated indicator
    r"(?:.*?)" # Non-greedy match for optional loser seed/status before link
    r"<a[^>]*href=['\"](?P<LoserURL>[^'\"]*)['\"][^>]*>(?P<LoserName>.*?)<\/a>.*?" # Loser Link + Name
    r"\s+(?P<Score>[\d\-\s\(\)\[\]/]+(?:RET|WO)?)$", # Score, potentially ending in RET or WO
    re.IGNORECASE | re.DOTALL # Use DOTALL to handle potential newlines
)

# --- Helper Functions ---
# find_latest_csv is imported

def get_tournament_urls_from_sources(processed_csv_path: str, sackmann_csv_path: str) -> Dict[str, str]:
    """
    Reads processed and sackmann CSVs, merges them to get URLs for relevant tournaments.
    Returns a dictionary mapping TournamentKey to TournamentURL.
    """
    urls_map = {}
    try:
        df_processed = pd.read_csv(processed_csv_path)
        df_sackmann = pd.read_csv(sackmann_csv_path)

        # Ensure required columns exist
        if 'TournamentName' not in df_processed.columns:
            print("Error: 'TournamentName' not found in processed CSV.")
            return {}
        if 'TournamentName' not in df_sackmann.columns or 'TournamentURL' not in df_sackmann.columns:
            print("Error: 'TournamentName' or 'TournamentURL' not found in Sackmann CSV.")
            return {}

        # Create TournamentKey in both dataframes for merging
        df_processed['TournamentKey'] = df_processed['TournamentName'].astype(str).apply(create_merge_key)
        df_sackmann['TournamentKey'] = df_sackmann['TournamentName'].astype(str).apply(create_merge_key)

        # Keep only relevant columns and drop duplicates for merging
        df_processed_keys = df_processed[['TournamentKey']].drop_duplicates()
        df_sackmann_urls = df_sackmann[['TournamentKey', 'TournamentURL']].drop_duplicates(subset=['TournamentKey'])

        # Merge to get URLs only for tournaments present in the processed file
        df_merged = pd.merge(df_processed_keys, df_sackmann_urls, on='TournamentKey', how='inner')

        # Create the dictionary: Key -> URL
        urls_map = pd.Series(df_merged.TournamentURL.values, index=df_merged.TournamentKey).to_dict()
        print(f"Found {len(urls_map)} unique tournament URLs relevant to processed data.")

    except FileNotFoundError as e:
        print(f"Error: Required CSV file not found: {e}")
    except Exception as e:
        print(f"Error reading or merging tournament URLs from source files: {e}")
        traceback.print_exc(limit=1)
    return urls_map

def parse_completed_matches(html_content: str, tournament_key: str, scrape_date: str) -> List[Dict[str, Any]]:
    """Parses the HTML content of a Tennis Abstract page to extract completed match results."""
    results = []
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        # Find the 'completed' span using BeautifulSoup
        completed_span = soup.find('span', id='completed')
        if not completed_span:
            print(f"  Could not find span with id='completed' for {tournament_key}")
            return []

        # Get the inner HTML of the span as a string
        completed_html = str(completed_span)
        # Split by <br/> tag to get individual lines
        match_lines_html = completed_html.split('<br/>')
        print(f"  Found {len(match_lines_html)} potential result lines in completed span.")

        for line_html in match_lines_html:
            # Clean up HTML fragments and whitespace
            line_cleaned = re.sub(r'<[^>]+>', '', line_html).replace('\n', ' ').strip() # Basic HTML tag removal
            line_cleaned = re.sub(r'\s+', ' ', line_cleaned).strip() # Normalize whitespace

            if not line_cleaned or line_cleaned == '&nbsp;':
                continue

            # Use regex on the raw HTML line to capture links and names
            match = RESULT_REGEX.search(line_html) # Search on the line *with* HTML
            if match:
                data = match.groupdict()
                winner_name_raw = data.get('WinnerName', '').strip()
                loser_name_raw = data.get('LoserName', '').strip()
                round_val = data.get('Round', '').strip()
                score = data.get('Score', '').strip()

                # Standardize names and create keys
                winner_display, winner_key = preprocess_player_name(winner_name_raw)
                loser_display, loser_key = preprocess_player_name(loser_name_raw)

                if winner_key and loser_key: # Ensure keys were generated
                    results.append({
                        'ResultDate': scrape_date, # Date the result was scraped
                        'TournamentKey': tournament_key,
                        'Round': round_val,
                        'WinnerName': winner_display,
                        'LoserName': loser_display,
                        'WinnerNameKey': winner_key,
                        'LoserNameKey': loser_key,
                        'Score': score
                    })
                else:
                    print(f"  Warning: Could not generate keys for result: W='{winner_name_raw}', L='{loser_name_raw}'")
            # else: # Debug non-matches
            #    if len(line_cleaned) > 10 and 'd.' in line_cleaned: # Only print likely candidates
            #        print(f"  Regex non-match line: {line_html[:200]}") # Print HTML fragment

    except Exception as e:
        print(f"Error parsing completed matches for {tournament_key}: {e}")
        traceback.print_exc(limit=1)
    return results

def scrape_results_html(url: str, tournament_key: str) -> str:
    """Scrapes the HTML content from a given URL."""
    print(f"  Requesting URL: {url}")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'}
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        print(f"  Successfully fetched URL (Status: {response.status_code}).")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"  Error fetching URL {url}: {e}")
        return ""

# --- Main Execution Logic ---
if __name__ == "__main__":
    print("="*50); print(" Starting Match Results Scraper..."); print("="*50)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_abs = os.path.join(script_dir, DATA_DIR)

    # 1. Find latest processed and sackmann files
    print("Finding latest input files...")
    latest_processed_file = find_latest_csv(data_dir_abs, PROCESSED_CSV_PATTERN)
    latest_sackmann_file = find_latest_csv(data_dir_abs, SACKMANN_CSV_PATTERN)

    if not latest_processed_file or not latest_sackmann_file:
        print("Error: Missing required input CSV file(s). Cannot proceed.")
        exit()

    # 2. Get map of TournamentKey -> TournamentURL
    tournament_urls_map = get_tournament_urls_from_sources(latest_processed_file, latest_sackmann_file)

    if not tournament_urls_map:
        print("Error: Could not determine tournament URLs to scrape.")
        exit()

    # 3. Scrape results for each tournament URL
    all_results = []
    today_date_str = datetime.now().strftime(DATE_FORMAT)
    scrape_timestamp_iso = datetime.now().isoformat()

    print(f"\nScraping results for {len(tournament_urls_map)} tournaments...")
    i = 0
    for t_key, url in tournament_urls_map.items():
        i += 1
        print(f"\n[{i}/{len(tournament_urls_map)}] Processing: {t_key} ({url})")
        html_content = scrape_results_html(url, t_key)
        if html_content:
            # Pass t_key directly, date for result identification
            tournament_results = parse_completed_matches(html_content, t_key, today_date_str)
            print(f"  Extracted {len(tournament_results)} completed matches.")
            all_results.extend(tournament_results)
        else:
            print(f"  Skipping result parsing due to fetch error.")
        time.sleep(1.5) # Be polite

    # 4. Create DataFrame and Save
    if not all_results:
        print("\nNo completed match results were extracted from any tournament.")
    else:
        print(f"\nSuccessfully extracted {len(all_results)} results in total.")
        results_df = pd.DataFrame(all_results)
        results_df['ScrapeTimestampISO'] = scrape_timestamp_iso # Add overall scrape time

        output_filename = f"{RESULTS_OUTPUT_FILENAME_BASE}_{today_date_str}.csv"
        output_path = os.path.join(data_dir_abs, output_filename)

        try:
            print(f"Saving results data to: {output_path}")
            results_df.to_csv(output_path, index=False, encoding='utf-8')
            print("Successfully saved results.")
        except Exception as e:
            print(f"Error saving results data to CSV: {e}")
            traceback.print_exc()

    print("\nMatch results scraping process finished.")
