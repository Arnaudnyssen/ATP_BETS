# p_sack_preproc.py (Refactored to Process Matchups)

import pandas as pd
import numpy as np
import re
import traceback
from typing import List, Optional, Dict, Any
import sys
import os

# --- Constants ---
MODEL_NAME = "Sackmann" # Identifier for this data source

# --- Import Scraper Functions ---
try:
    # Assuming tennis_abstract_scraper.py is in the same directory
    from tennis_abstract_scraper import tourneys_url, probas_scraper
except ImportError as e:
    print(f"Error importing from tennis_abstract_scraper: {e}")
    # Handle if scripts are in different directories if necessary
    # project_dir = os.path.dirname(os.path.abspath(__file__))
    # if project_dir not in sys.path: sys.path.append(project_dir)
    # try: from tennis_abstract_scraper import tourneys_url, probas_scraper
    # except ImportError: # Handle final import failure
    print("Ensure tennis_abstract_scraper.py is accessible.")
    sys.exit(1)

# --- Helper Functions ---

def preprocess_player_name(name: str) -> str:
    """Standardizes a single player name string."""
    if not isinstance(name, str):
        return "" # Return empty string if input is not a string
    try:
        # Remove content in parentheses (seed, country, WC, Q, etc.)
        name = re.sub(r'\s*\([^)]*\)', '', name)
        # Remove leading/trailing asterisks
        name = re.sub(r'^\*|\*$', '', name)
        # Strip whitespace and convert to lowercase
        name = name.strip().title()
        # Optional: Add more cleaning like removing accents if needed later
        # from unicodedata import normalize
        # name = normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')
        return name
    except Exception as e:
        print(f"Warning: Could not preprocess name '{name}': {e}")
        return name # Return original name on error

def calculate_odds(probability: Optional[float]) -> Optional[float]:
    """Calculates decimal odds from probability (0-100). Handles 0 probability."""
    if probability is None or not isinstance(probability, (int, float)):
        return None
    if probability <= 0:
        return None # Or return np.inf or a very large number? None seems safer.
    try:
        odds = 100.0 / probability
        return round(odds, 2)
    except ZeroDivisionError:
        return None
    except Exception as e:
        print(f"Warning: Could not calculate odds for probability '{probability}': {e}")
        return None

def get_tournament_name_from_url(url: str) -> str:
    """Extracts a readable tournament name from the Tennis Abstract URL."""
    try:
        if isinstance(url, str) and '/' in url and len(url.split('/')) > 2:
            # Assumes format like .../current/YYYYTournamentName.html
            name_part = url.split('/')[-1] # Get filename
            name_part = name_part.replace('.html', '') # Remove extension
            # Remove potential year prefix (handle YYYY or YY)
            if name_part[:4].isdigit() and len(name_part) > 4:
                name_part = name_part[4:]
            elif name_part[:2].isdigit() and len(name_part) > 2:
                 name_part = name_part[2:]
            # Replace hyphens/underscores, title case
            name_part = re.sub(r'[-_]', ' ', name_part)
            # Handle common patterns like (City)Challenger
            name_part = re.sub(r'\((\w+)\)Challenger', r'\1 Challenger', name_part)
            return name_part.title()
        else:
            return 'Unknown Tournament'
    except Exception as e:
        print(f"Warning: Could not extract tournament name from URL '{url}': {e}")
        return 'Unknown Tournament'

# --- New Processing Logic ---

def process_matchup_list(match_list: List[Dict[str, Any]], url: str) -> Optional[pd.DataFrame]:
    """
    Converts a list of scraped match dictionaries into a processed DataFrame.

    Args:
        match_list (List[Dict[str, Any]]): List of dicts from probas_scraper.
                                           Expected keys: 'Player1', 'Player2',
                                           'P1_Prob', 'P2_Prob', 'Round'.
        url (str): The source URL for this tournament.

    Returns:
        Optional[pd.DataFrame]: Processed DataFrame matching the target structure,
                                or None if input is invalid or processing fails.
    """
    if not match_list:
        print(f"Received empty match list for URL: {url}")
        return None # Return None for empty input

    try:
        # 1. Convert list of dictionaries to DataFrame
        df = pd.DataFrame(match_list)
        print(f"Created initial DataFrame with shape {df.shape} from {len(match_list)} matchups.")

        # 2. Basic Validation (check if essential columns exist)
        required_cols = ['Player1', 'Player2', 'P1_Prob', 'P2_Prob', 'Round']
        if not all(col in df.columns for col in required_cols):
            print(f"Error: DataFrame missing required columns. Found: {df.columns.tolist()}")
            return None

        # 3. Rename columns to match target structure
        df.rename(columns={
            'Player1': 'Player1Name',
            'Player2': 'Player2Name',
            'P1_Prob': 'Player1_Match_Prob',
            'P2_Prob': 'Player2_Match_Prob'
            # 'Round' is already correct
        }, inplace=True)
        print("Renamed columns.")

        # 4. Preprocess Player Names
        print("Preprocessing player names...")
        df['Player1Name'] = df['Player1Name'].apply(preprocess_player_name)
        df['Player2Name'] = df['Player2Name'].apply(preprocess_player_name)
        print("Player names preprocessed.")

        # 5. Calculate Odds
        print("Calculating odds...")
        df['Player1_Match_Odds'] = df['Player1_Match_Prob'].apply(calculate_odds)
        df['Player2_Match_Odds'] = df['Player2_Match_Prob'].apply(calculate_odds)
        print("Odds calculated.")

        # 6. Add Metadata
        print("Adding metadata...")
        df['TournamentURL'] = url
        df['TournamentName'] = get_tournament_name_from_url(url)
        df['ModelName'] = MODEL_NAME
        print("Metadata added.")

        # 7. Reorder columns to match target structure
        target_columns = [
            'TournamentName', 'TournamentURL', 'Round',
            'Player1Name', 'Player2Name',
            'Player1_Match_Prob', 'Player2_Match_Prob',
            'Player1_Match_Odds', 'Player2_Match_Odds',
            'ModelName'
            # ScrapeTimestampUTC will be added after concatenation
        ]
        # Ensure only existing columns are selected in the specified order
        df = df[[col for col in target_columns if col in df.columns]]
        print(f"Columns reordered. Final shape for this URL: {df.shape}")

        return df

    except Exception as e:
        print(f"Error processing matchup list for URL {url}: {e}")
        traceback.print_exc()
        return None


def get_all_matchup_data() -> pd.DataFrame:
    """
    Scrapes matchup data for all relevant Tennis Abstract URLs and
    processes it into a single consolidated DataFrame.

    Returns:
        pd.DataFrame: Consolidated DataFrame with matchup data, or empty DataFrame on failure.
    """
    print("Starting to fetch all matchup data...")
    urls = tourneys_url() # Get tournament URLs
    print(f"Found {len(urls)} tournament URLs to scrape.")
    all_matchup_dfs = []

    if not urls:
        print("No tournament URLs found.")
        return pd.DataFrame()

    for url in urls:
        print("-" * 30)
        print(f"Processing URL: {url}")
        try:
            # Scrape matchup dictionaries for the current URL
            scraped_matchups = probas_scraper(url) # Returns List[Dict]

            if scraped_matchups:
                print(f"Scraped {len(scraped_matchups)} potential matchups. Processing...")
                # Process the list of dictionaries into a DataFrame
                processed_df = process_matchup_list(scraped_matchups, url)

                if processed_df is not None and not processed_df.empty:
                    print(f"Successfully processed DataFrame for {url}. Shape: {processed_df.shape}")
                    all_matchup_dfs.append(processed_df)
                else:
                    print(f"No valid DataFrame generated after processing for {url}.")
            else:
                print(f"No matchups returned by scraper for {url}.")

        except Exception as e:
            # Catch errors during the scrape/process loop for a single URL
            print(f"Critical error during scrape/process loop for {url}: {e}")
            traceback.print_exc()
            print(f"Skipping to next URL due to error.")
            continue # Continue with the next URL

    # --- Final Concatenation ---
    if not all_matchup_dfs:
        print("\nNo matchup data collected from any URL after processing.")
        return pd.DataFrame() # Return empty DataFrame

    try:
        print(f"\nConcatenating {len(all_matchup_dfs)} processed DataFrames...")
        final_matchup_data = pd.concat(all_matchup_dfs, ignore_index=True)
        print(f"Concatenated data shape: {final_matchup_data.shape}")

        # Add timestamp after successful concatenation
        final_matchup_data['ScrapeTimestampUTC'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')
        print("Added ScrapeTimestampUTC.")

        # Final column reordering including timestamp
        final_columns_order = [
            'TournamentName', 'TournamentURL', 'Round',
            'Player1Name', 'Player2Name',
            'Player1_Match_Prob', 'Player2_Match_Prob',
            'Player1_Match_Odds', 'Player2_Match_Odds',
            'ModelName', 'ScrapeTimestampUTC'
        ]
        final_matchup_data = final_matchup_data[[col for col in final_columns_order if col in final_matchup_data.columns]]


        print(f"Final consolidated matchup data shape: {final_matchup_data.shape}")
        return final_matchup_data

    except Exception as e:
         print(f"Error during final concatenation or timestamping: {e}")
         traceback.print_exc()
         return pd.DataFrame() # Return empty DataFrame on error


# --- Example Usage ---
if __name__ == "__main__":
    print("="*50)
    print("Running p_sack_preproc.py directly for testing...")
    print("="*50)
    start_time = pd.Timestamp.now()
    print(f"Start time: {start_time}")

    matchup_data = get_all_matchup_data() # Call the main function

    end_time = pd.Timestamp.now()
    print(f"\nEnd time: {end_time}")
    print(f"Total processing time: {end_time - start_time}")

    if not matchup_data.empty:
        print("\n--- Sample of Processed Matchup Data ---")
        print(matchup_data.head())
        print("\n--- Data Info ---")
        matchup_data.info()
        # Optional: Save locally for inspection
        # local_save_path = "debug_matchup_data.csv"
        # print(f"\nSaving debug data locally to: {local_save_path}")
        # matchup_data.to_csv(local_save_path, index=False)
    else:
        print("\n--- No matchup data was processed or collected. ---")

