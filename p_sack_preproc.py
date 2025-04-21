# p_sack_preproc.py (v2 - Handle Results)
# Processes matchups and aggregates results scraped by tennis_abstract_scraper.

import pandas as pd
import numpy as np
import re
import traceback
from typing import List, Optional, Dict, Any, Tuple # Added Tuple
import sys
import time
import os

# --- Constants ---
MODEL_NAME = "Sackmann"

# --- Import Scraper Functions ---
try:
    # Assuming tennis_abstract_scraper.py is in the same directory
    # Import setup_driver if needed for managing driver instance
    from tennis_abstract_scraper import tourneys_url, probas_scraper, setup_driver
except ImportError as e:
    print(f"Error importing from tennis_abstract_scraper: {e}")
    print("Ensure tennis_abstract_scraper.py is accessible.")
    sys.exit(1)

# --- Helper Functions ---
# (preprocess_player_name, calculate_odds, get_tournament_name_from_url remain the same)
def preprocess_player_name(name: str) -> str:
    """Standardizes a single player name string."""
    if not isinstance(name, str): return ""
    try:
        name = re.sub(r'\s*\([^)]*\)', '', name)
        name = re.sub(r'^\*|\*$', '', name)
        name = name.strip().title()
        return name
    except Exception as e:
        print(f"Warning: Could not preprocess name '{name}': {e}")
        return name

def calculate_odds(probability: Optional[float]) -> Optional[float]:
    """Calculates decimal odds from probability (0-100). Handles 0 probability."""
    if probability is None or not isinstance(probability, (int, float)): return None
    if probability <= 0: return None
    try:
        odds = 100.0 / probability
        return round(odds, 2)
    except ZeroDivisionError: return None
    except Exception as e:
        print(f"Warning: Could not calculate odds for probability '{probability}': {e}")
        return None

def get_tournament_name_from_url(url: str) -> str:
    """Extracts a readable tournament name from the Tennis Abstract URL."""
    try:
        if isinstance(url, str) and '/' in url and len(url.split('/')) > 2:
            name_part = url.split('/')[-1]
            name_part = name_part.replace('.html', '')
            if name_part[:4].isdigit() and len(name_part) > 4: name_part = name_part[4:]
            elif name_part[:2].isdigit() and len(name_part) > 2: name_part = name_part[2:]
            name_part = re.sub(r'[-_]', ' ', name_part)
            name_part = re.sub(r'\((\w+)\)Challenger', r'\1 Challenger', name_part)
            # Simple Title Case might be enough here
            return name_part.title()
        else: return 'Unknown Tournament'
    except Exception as e:
        print(f"Warning: Could not extract tournament name from URL '{url}': {e}")
        return 'Unknown Tournament'

# --- Processing Logic ---

def process_matchup_list(match_list: List[Dict[str, Any]], url: str) -> Optional[pd.DataFrame]:
    """
    Converts a list of scraped upcoming match dictionaries into a processed DataFrame.
    (Logic remains the same as previous version)
    """
    if not match_list:
        # This is normal if a tournament only has completed matches
        # print(f"Received empty matchup list for URL: {url}")
        return None

    try:
        df = pd.DataFrame(match_list)
        # print(f"Created initial matchup DataFrame with shape {df.shape} from {len(match_list)} matchups.")
        required_cols = ['Player1', 'Player2', 'P1_Prob', 'P2_Prob', 'Round']
        if not all(col in df.columns for col in required_cols):
            print(f"Error: Matchup DataFrame missing required columns. Found: {df.columns.tolist()}")
            return None

        df.rename(columns={
            'Player1': 'Player1Name', 'Player2': 'Player2Name',
            'P1_Prob': 'Player1_Match_Prob', 'P2_Prob': 'Player2_Match_Prob'
        }, inplace=True)

        df['Player1Name'] = df['Player1Name'].apply(preprocess_player_name)
        df['Player2Name'] = df['Player2Name'].apply(preprocess_player_name)
        df['Player1_Match_Odds'] = df['Player1_Match_Prob'].apply(calculate_odds)
        df['Player2_Match_Odds'] = df['Player2_Match_Prob'].apply(calculate_odds)
        df['TournamentURL'] = url
        df['TournamentName'] = get_tournament_name_from_url(url)
        df['ModelName'] = MODEL_NAME

        target_columns = [
            'TournamentName', 'TournamentURL', 'Round',
            'Player1Name', 'Player2Name',
            'Player1_Match_Prob', 'Player2_Match_Prob',
            'Player1_Match_Odds', 'Player2_Match_Odds',
            'ModelName'
        ]
        df = df[[col for col in target_columns if col in df.columns]]
        # print(f"Processed matchup DataFrame for {url}. Shape: {df.shape}")
        return df

    except Exception as e:
        print(f"Error processing matchup list for URL {url}: {e}")
        traceback.print_exc()
        return None


# --- MODIFIED: Get All Data (Matchups and Results) ---
def get_all_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Scrapes matchup and results data for all relevant Tennis Abstract URLs
    and processes them into two consolidated DataFrames.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (Consolidated Matchups, Consolidated Results)
                                           Returns empty DataFrames on failure.
    """
    print("Starting to fetch all matchup and results data...")
    urls = tourneys_url()
    print(f"Found {len(urls)} tournament URLs to scrape.")
    all_matchup_dfs = []
    all_results_list = [] # Store results as list of dicts first

    if not urls:
        print("No tournament URLs found.")
        return pd.DataFrame(), pd.DataFrame()

    # Setup main driver instance to reuse
    driver = setup_driver()
    if driver is None:
        print("Failed to setup WebDriver. Aborting scrape.")
        return pd.DataFrame(), pd.DataFrame()

    try:
        for url in urls:
            print("-" * 30)
            print(f"Processing URL: {url}")
            try:
                # Scrape both matchups and results using the single driver instance
                scraped_matchups, scraped_results = probas_scraper(url, driver)

                if scraped_matchups:
                    # print(f"Scraped {len(scraped_matchups)} potential matchups. Processing...")
                    processed_df = process_matchup_list(scraped_matchups, url)
                    if processed_df is not None and not processed_df.empty:
                        all_matchup_dfs.append(processed_df)
                    # else: print(f"No valid matchup DataFrame generated after processing for {url}.")
                # else: print(f"No matchups returned by scraper for {url}.")

                if scraped_results:
                    # print(f"Scraped {len(scraped_results)} completed results.")
                    # Add tournament name derived from URL if needed (depends on results dict structure)
                    t_name = get_tournament_name_from_url(url)
                    for res in scraped_results:
                        res['TournamentName'] = t_name # Ensure name is present
                    all_results_list.extend(scraped_results)
                # else: print(f"No results returned by scraper for {url}.")

            except Exception as e:
                print(f"Critical error during scrape/process loop for {url}: {e}")
                traceback.print_exc()
                print(f"Skipping to next URL due to error.")
                continue
            finally:
                # Optional delay between requests
                time.sleep(1)

    finally:
        # Ensure driver is closed even if errors occur
        if driver:
            print("Closing WebDriver...")
            driver.quit()
            print("WebDriver closed.")

    # --- Final DataFrame Creation ---
    final_matchup_data = pd.DataFrame()
    final_results_data = pd.DataFrame()

    if not all_matchup_dfs:
        print("\nNo matchup data collected from any URL after processing.")
    else:
        try:
            print(f"\nConcatenating {len(all_matchup_dfs)} processed Matchup DataFrames...")
            final_matchup_data = pd.concat(all_matchup_dfs, ignore_index=True)
            final_matchup_data['ScrapeTimestampUTC'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')
            print(f"Final consolidated matchup data shape: {final_matchup_data.shape}")
        except Exception as e:
             print(f"Error during final matchup concatenation or timestamping: {e}")
             traceback.print_exc()

    if not all_results_list:
        print("\nNo results data collected from any URL.")
    else:
        try:
            print(f"\nCreating final Results DataFrame from {len(all_results_list)} records...")
            final_results_data = pd.DataFrame(all_results_list)
            # Add timestamp if desired (already have ResultDate which is scrape date)
            # final_results_data['ScrapeTimestampUTC'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')
            print(f"Final consolidated results data shape: {final_results_data.shape}")
        except Exception as e:
            print(f"Error creating final results DataFrame: {e}")
            traceback.print_exc()

    return final_matchup_data, final_results_data


# --- Example Usage ---
if __name__ == "__main__":
    print("="*50)
    print("Running p_sack_preproc.py directly for testing...")
    print("="*50)
    start_time = pd.Timestamp.now()
    print(f"Start time: {start_time}")

    matchup_data, results_data = get_all_data() # Call the main function

    end_time = pd.Timestamp.now()
    print(f"\nEnd time: {end_time}")
    print(f"Total processing time: {end_time - start_time}")

    if not matchup_data.empty:
        print("\n--- Sample of Processed Matchup Data ---")
        print(matchup_data.head())
    else:
        print("\n--- No matchup data was processed or collected. ---")

    if not results_data.empty:
        print("\n--- Sample of Processed Results Data ---")
        print(results_data.head())
    else:
        print("\n--- No results data was processed or collected. ---")

