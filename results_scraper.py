# results_scraper.py (v2 - Corrected Regex)
# Scrapes completed match results from Tennis Abstract tournament pages.
# Reads tournament list from processed_comparison_*.csv and URLs from sackmann_matchups_*.csv.
# Corrected regex to handle linked 'd.' for defeated.

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
    from process_data import create_merge_key, preprocess_player_name, find_latest_csv
except ImportError:
    print("ERROR: Cannot import helper functions from process_data.py. Ensure it's accessible.")
    def create_merge_key(text: str) -> str: return ""
    def preprocess_player_name(name: str) -> Tuple[str, str]: return name, ""
    def find_latest_csv(directory: str, pattern: str) -> Optional[str]: return None
    # exit()

# --- Selenium/Requests Imports ---
import requests
from bs4 import BeautifulSoup

# --- Constants ---
DATA_DIR = "data_archive"
PROCESSED_CSV_PATTERN = "processed_comparison_*.csv"
SACKMANN_CSV_PATTERN = "sackmann_matchups_*.csv"
RESULTS_OUTPUT_FILENAME_BASE = "match_results"
DATE_FORMAT = "%Y%m%d"
DATE_FORMAT_LOG = "%Y-%m-%d" # For matching result date to log date
REQUEST_TIMEOUT = 20

# --- CORRECTED Regex ---
# Handles 'd.' being plain text OR inside a link <a>d.</a>
# Uses non-greedy matching .*? liberally between captured groups
RESULT_REGEX = re.compile(
    r"^(?P<Round>[A-Za-z0-9]+):\s*" # Round indicator (e.g., F:, R32:)
    r".*?" # Non-greedy match for optional winner seed/status
    r"<a[^>]*href=['\"](?P<WinnerURL>[^'\"]*)['\"][^>]*>(?P<WinnerName>.*?)<\/a>" # Winner Link + Name
    r".*?" # Anything between winner name and the 'd.' link/text (like country code)
    r"(?:<a[^>]*>\s*d\.\s*<\/a>|\sd\.)" # Match EITHER <a...>d.</a> OR space+d.+period (with optional spaces inside link)
    r".*?" # Anything between 'd.' and loser name link
    r"<a[^>]*href=['\"](?P<LoserURL>[^'\"]*)['\"][^>]*>(?P<LoserName>.*?)<\/a>" # Loser Link + Name
    r".*?" # Anything between loser name and score (like country codes)
    r"(?P<Score>[\d\-\s\(\)\[\]/]+(?:RET|WO)?)$", # Score, potentially ending in RET or WO
    re.IGNORECASE | re.DOTALL # Ignore case and allow '.' to match newlines
)
# ----------------------


# --- Helper Functions ---
# (find_latest_csv is imported)

# (get_tournament_urls_from_sources remains the same as v1_revised)
def get_tournament_urls_from_sources(processed_csv_path: str, sackmann_csv_path: str) -> Dict[str, str]:
    """
    Reads processed and sackmann CSVs, merges them to get URLs for relevant tournaments.
    Returns a dictionary mapping TournamentKey to TournamentURL.
    """
    urls_map = {}
    try:
        df_processed = pd.read_csv(processed_csv_path)
        df_sackmann = pd.read_csv(sackmann_csv_path)

        if 'TournamentName' not in df_processed.columns:
            print("Error: 'TournamentName' not found in processed CSV.")
            return {}
        if 'TournamentName' not in df_sackmann.columns or 'TournamentURL' not in df_sackmann.columns:
            print("Error: 'TournamentName' or 'TournamentURL' not found in Sackmann CSV.")
            return {}

        df_processed['TournamentKey'] = df_processed['TournamentName'].astype(str).apply(create_merge_key)
        df_sackmann['TournamentKey'] = df_sackmann['TournamentName'].astype(str).apply(create_merge_key)

        df_processed_keys = df_processed[['TournamentKey']].drop_duplicates()
        # Ensure we take the URL from the *first* occurrence in case of duplicates in sackmann file
        df_sackmann_urls = df_sackmann[['TournamentKey', 'TournamentURL']].drop_duplicates(subset=['TournamentKey'], keep='first')


        df_merged = pd.merge(df_processed_keys, df_sackmann_urls, on='TournamentKey', how='inner')

        urls_map = pd.Series(df_merged.TournamentURL.values, index=df_merged.TournamentKey).to_dict()
        print(f"Found {len(urls_map)} unique tournament URLs relevant to processed data.")

    except FileNotFoundError as e:
        print(f"Error: Required CSV file not found: {e}")
    except Exception as e:
        print(f"Error reading or merging tournament URLs from source files: {e}")
        traceback.print_exc(limit=1)
    return urls_map

# (parse_completed_matches remains the same, uses the updated REGEX constant)
def parse_completed_matches(html_content: str, tournament_key: str, scrape_date: str) -> List[Dict[str, Any]]:
    """Parses the HTML content of a Tennis Abstract page to extract completed match results."""
    results = []
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        completed_span = soup.find('span', id='completed')
        if not completed_span:
            print(f"  Could not find span with id='completed' for {tournament_key}")
            return []

        # Process the inner HTML, splitting by <br/>
        completed_html = str(completed_span)
        match_lines_html = completed_html.split('<br/>')
        print(f"  Found {len(match_lines_html)} potential result lines in completed span.")

        for line_html in match_lines_html:
            # Apply regex directly to the HTML line fragment
            match = RESULT_REGEX.search(line_html)
            if match:
                data = match.groupdict()
                winner_name_raw = data.get('WinnerName', '').strip()
                loser_name_raw = data.get('LoserName', '').strip()
                round_val = data.get('Round', '').strip()
                score = data.get('Score', '').strip()

                # Clean up names extracted from HTML entities if necessary
                winner_name_raw = html.unescape(winner_name_raw)
                loser_name_raw = html.unescape(loser_name_raw)

                # Standardize names and create keys
                winner_display, winner_key = preprocess_player_name(winner_name_raw)
                loser_display, loser_key = preprocess_player_name(loser_name_raw)

                if winner_key and loser_key:
                    results.append({
                        'ResultDate': scrape_date,
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
            else: # Debug non-matches
                 line_cleaned = re.sub(r'<[^>]+>', '', line_html).replace('\n', ' ').strip()
                 line_cleaned = re.sub(r'\s+', ' ', line_cleaned).strip()
                 if len(line_cleaned) > 10 and 'd.' in line_cleaned: # Only print likely candidates
                     print(f"  Regex non-match line fragment: {line_html[:250]}") # Print HTML fragment

    except Exception as e:
        print(f"Error parsing completed matches for {tournament_key}: {e}")
        traceback.print_exc(limit=1)
    return results

# (scrape_results_html remains the same)
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
# (Main logic remains the same)
if __name__ == "__main__":
    print("="*50); print(" Starting Match Results Scraper..."); print("="*50)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_abs = os.path.join(script_dir, DATA_DIR)

    print("Finding latest input files...")
    latest_processed_file = find_latest_csv(data_dir_abs, PROCESSED_CSV_PATTERN)
    latest_sackmann_file = find_latest_csv(data_dir_abs, SACKMANN_CSV_PATTERN)

    if not latest_processed_file or not latest_sackmann_file:
        print("Error: Missing required input CSV file(s). Cannot proceed.")
        exit()

    tournament_urls_map = get_tournament_urls_from_sources(latest_processed_file, latest_sackmann_file)

    if not tournament_urls_map:
        print("Error: Could not determine tournament URLs to scrape.")
        exit()

    all_results = []
    today_date_str = datetime.now().strftime(DATE_FORMAT_LOG) # Use log format for result date consistency
    scrape_timestamp_iso = datetime.now().isoformat()

    print(f"\nScraping results for {len(tournament_urls_map)} tournaments...")
    i = 0
    for t_key, url in tournament_urls_map.items():
        i += 1
        print(f"\n[{i}/{len(tournament_urls_map)}] Processing: {t_key} ({url})")
        html_content = scrape_results_html(url, t_key)
        if html_content:
            tournament_results = parse_completed_matches(html_content, t_key, today_date_str)
            print(f"  Extracted {len(tournament_results)} completed matches.")
            all_results.extend(tournament_results)
        else:
            print(f"  Skipping result parsing due to fetch error.")
        time.sleep(1.5) # Be polite

    if not all_results:
        print("\nNo completed match results were extracted from any tournament.")
    else:
        print(f"\nSuccessfully extracted {len(all_results)} results in total.")
        results_df = pd.DataFrame(all_results)
        results_df['ScrapeTimestampISO'] = scrape_timestamp_iso

        # Use the *actual date* for the filename, not today's date if scraping historical
        # For simplicity now, we assume results are scraped daily for the current day's comparison file date
        output_date_str = datetime.strptime(today_date_str, DATE_FORMAT_LOG).strftime(DATE_FORMAT)
        output_filename = f"{RESULTS_OUTPUT_FILENAME_BASE}_{output_date_str}.csv"
        output_path = os.path.join(data_dir_abs, output_filename)

        try:
            print(f"Saving results data to: {output_path}")
            results_df.to_csv(output_path, index=False, encoding='utf-8')
            print("Successfully saved results.")
        except Exception as e:
            print(f"Error saving results data to CSV: {e}")
            traceback.print_exc()

    print("\nMatch results scraping process finished.")
