# calculate_results.py (v1)
# Loads strategy log and match results to calculate Profit/Loss.

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime, timedelta
import traceback
from typing import Optional, List, Dict, Any

# --- Constants ---
DATA_DIR = "data_archive"
STRATEGY_LOG_FILENAME = "strategy_log.csv" # Input log file
RESULTS_CSV_PATTERN = "match_results_*.csv" # Input results files
# Output file can overwrite log or be separate (e.g., performance summary)
# Let's overwrite the log for simplicity for now, adding P/L columns
UPDATED_LOG_FILENAME = "strategy_log.csv"
DATE_FORMAT_LOG = "%Y-%m-%d" # Date format in strategy log
DATE_FORMAT_RESULTS = "%Y%m%d" # Date format in results filename

# --- Helper Functions ---
def find_latest_csv(directory: str, pattern: str) -> Optional[str]:
    """Finds the most recently modified CSV file matching the pattern."""
    # (Same as in other scripts)
    try:
        if not os.path.isabs(directory):
             script_dir = os.path.dirname(os.path.abspath(__file__))
             search_dir = os.path.join(script_dir, directory)
        else:
             search_dir = directory
        search_path = os.path.join(search_dir, pattern); print(f"Searching for pattern: {search_path}")
        list_of_files = glob.glob(search_path)
        if not list_of_files: print(f"  No files found matching pattern."); return None
        list_of_files = [f for f in list_of_files if os.path.isfile(f)]
        if not list_of_files: print(f"  No *files* found matching pattern."); return None
        latest_file = max(list_of_files, key=os.path.getmtime); print(f"Found latest CSV file: {os.path.basename(latest_file)}")
        return latest_file
    except Exception as e: print(f"Error finding latest CSV file in '{directory}' with pattern '{pattern}': {e}"); traceback.print_exc(); return None

# Need key generation functions if results file doesn't have them
try:
    from process_data import create_merge_key, preprocess_player_name
except ImportError:
    print("ERROR: Cannot import helper functions from process_data.py. Ensure it's accessible.")
    def create_merge_key(text: str) -> str: return "" # Placeholder
    def preprocess_player_name(name: str) -> Tuple[str, str]: return name, "" # Placeholder

def load_results_data(data_dir: str, log_dates: pd.Series) -> pd.DataFrame:
    """Loads results CSVs for dates present in the strategy log."""
    results_df_list = []
    required_dates = log_dates.unique()
    print(f"Need results for dates: {required_dates}")

    for date_str in required_dates:
        try:
            # Convert log date string to results filename date string
            date_obj = datetime.strptime(date_str, DATE_FORMAT_LOG)
            results_date_str = date_obj.strftime(DATE_FORMAT_RESULTS)
            results_pattern = f"match_results_{results_date_str}.csv"
            results_file_path = os.path.join(data_dir, results_pattern)

            if os.path.exists(results_file_path):
                print(f"Loading results from: {results_pattern}")
                df_res = pd.read_csv(results_file_path)
                # Ensure keys are present, generate if needed (depends on results_scraper output)
                if 'WinnerNameKey' not in df_res.columns and 'WinnerName' in df_res.columns:
                     df_res['WinnerNameKey'] = df_res['WinnerName'].apply(lambda x: preprocess_player_name(x)[1])
                if 'LoserNameKey' not in df_res.columns and 'LoserName' in df_res.columns:
                     df_res['LoserNameKey'] = df_res['LoserName'].apply(lambda x: preprocess_player_name(x)[1])
                if 'TournamentKey' not in df_res.columns and 'TournamentName' in df_res.columns:
                     df_res['TournamentKey'] = df_res['TournamentName'].apply(create_merge_key)

                results_df_list.append(df_res)
            else:
                print(f"Warning: Results file not found for date {date_str}: {results_pattern}")
        except Exception as e:
            print(f"Error loading results for date {date_str}: {e}")

    if not results_df_list:
        return pd.DataFrame()
    else:
        return pd.concat(results_df_list, ignore_index=True)


# --- Main Execution Logic ---
if __name__ == "__main__":
    print("="*50); print(" Starting Profit/Loss Calculation..."); print("="*50)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_abs = os.path.join(script_dir, DATA_DIR)
    log_file_path = os.path.join(data_dir_abs, STRATEGY_LOG_FILENAME)
    updated_log_path = os.path.join(data_dir_abs, UPDATED_LOG_FILENAME)

    # 1. Load Strategy Log
    print(f"Loading strategy log from: {log_file_path}")
    if not os.path.exists(log_file_path):
        print("Error: Strategy log file not found. Run simulate_strategies.py first.")
        exit()
    try:
        df_log = pd.read_csv(log_file_path)
        # Convert BetDate to consistent format if needed
        df_log['BetDate'] = pd.to_datetime(df_log['BetDate']).dt.strftime(DATE_FORMAT_LOG)
    except Exception as e:
        print(f"Error loading strategy log: {e}")
        traceback.print_exc()
        exit()

    if df_log.empty:
        print("Strategy log is empty. Nothing to calculate.")
        exit()

    # Filter log for entries that haven't been processed yet (ProfitLoss is NaN)
    df_log_unprocessed = df_log[df_log['ProfitLoss'].isna()].copy()
    if df_log_unprocessed.empty:
        print("No unprocessed bets found in the log.")
        exit()
    print(f"Found {len(df_log_unprocessed)} unprocessed bets to calculate.")

    # 2. Load Relevant Results Data
    print("\nLoading relevant match results data...")
    df_results = load_results_data(data_dir_abs, df_log_unprocessed['BetDate'])

    if df_results.empty:
        print("No relevant match results found for the dates in the log. Cannot calculate P/L.")
        exit()

    # 3. Prepare for Merge
    # Ensure keys are present and standardized in both dataframes
    df_log_unprocessed['TournamentKey'] = df_log_unprocessed['Tournament'].apply(create_merge_key)
    df_log_unprocessed['Player1NameKey'] = df_log_unprocessed['Player1'].apply(lambda x: preprocess_player_name(x)[1])
    df_log_unprocessed['Player2NameKey'] = df_log_unprocessed['Player2'].apply(lambda x: preprocess_player_name(x)[1])
    # Results keys should be generated in load_results_data if missing

    # Create unique match keys in both dfs for merging (handle player order)
    # Sort player keys alphabetically to create a consistent key regardless of P1/P2 order
    df_log_unprocessed['MatchKey'] = df_log_unprocessed.apply(
        lambda row: f"{row['BetDate']}_{row['TournamentKey']}_" + "_".join(sorted([row['Player1NameKey'], row['Player2NameKey']])), axis=1
    )
    df_results['MatchKey'] = df_results.apply(
        lambda row: f"{datetime.strptime(str(row['ResultDate']), DATE_FORMAT_RESULTS).strftime(DATE_FORMAT_LOG)}_{row['TournamentKey']}_" + "_".join(sorted([row['WinnerNameKey'], row['LoserNameKey']])), axis=1
    )

    # Select only necessary columns from results
    df_results_slim = df_results[['MatchKey', 'WinnerNameKey']].drop_duplicates(subset=['MatchKey'])

    # 4. Merge Log with Results
    print("\nMerging bets with results...")
    df_merged = pd.merge(df_log_unprocessed, df_results_slim, on='MatchKey', how='left')
    print(f"Merge complete. Shape: {df_merged.shape}")

    # 5. Calculate Profit/Loss
    print("Calculating Profit/Loss...")
    df_merged['MatchResult'] = 'Unknown' # Default
    df_merged['ProfitLoss'] = np.nan # Default

    for index, row in df_merged.iterrows():
        if pd.isna(row['WinnerNameKey']): # Result not found
            df_merged.loc[index, 'MatchResult'] = 'Result Missing'
            continue

        bet_on_p1 = (row['BetOnPlayer'] == 'P1')
        p1_won = (row['WinnerNameKey'] == row['Player1NameKey'])
        p2_won = (row['WinnerNameKey'] == row['Player2NameKey']) # Should be the opposite of p1_won if names match

        if bet_on_p1:
            if p1_won:
                df_merged.loc[index, 'MatchResult'] = 'P1_Win'
                df_merged.loc[index, 'ProfitLoss'] = row['BetAmount'] * (row['BetOdds'] - 1)
            elif p2_won:
                df_merged.loc[index, 'MatchResult'] = 'P2_Win'
                df_merged.loc[index, 'ProfitLoss'] = -row['BetAmount']
            else: # Should not happen if merge worked and names are consistent
                 df_merged.loc[index, 'MatchResult'] = 'Result Name Mismatch'
        else: # Bet on P2
            if p2_won:
                df_merged.loc[index, 'MatchResult'] = 'P2_Win'
                df_merged.loc[index, 'ProfitLoss'] = row['BetAmount'] * (row['BetOdds'] - 1)
            elif p1_won:
                df_merged.loc[index, 'MatchResult'] = 'P1_Win'
                df_merged.loc[index, 'ProfitLoss'] = -row['BetAmount']
            else:
                 df_merged.loc[index, 'MatchResult'] = 'Result Name Mismatch'

    print("Profit/Loss calculation complete.")

    # 6. Update the original log DataFrame
    print("\nUpdating original strategy log...")
    # Use the index from df_log_unprocessed to update df_log
    update_cols = ['MatchResult', 'ProfitLoss']
    # Set index on both to align rows correctly
    df_log.set_index(df_log_unprocessed.index, inplace=True)
    df_merged.set_index(df_log_unprocessed.index, inplace=True) # df_merged has same index as unprocessed
    df_log.update(df_merged[update_cols])
    df_log.reset_index(drop=True, inplace=True) # Reset index back to default

    # 7. Save Updated Log File
    try:
        print(f"Saving updated strategy log to: {updated_log_path}")
        # Overwrite the original log file with the updated data
        df_log.to_csv(updated_log_path, index=False, encoding='utf-8', float_format='%.4f')
        print("Successfully saved updated log.")
    except Exception as e:
        print(f"Error writing updated strategy log file '{updated_log_path}': {e}")
        traceback.print_exc()

    # --- Optional: Generate Daily Summary ---
    try:
        print("\nGenerating daily performance summary...")
        # Ensure ProfitLoss is numeric for aggregation
        df_log['ProfitLoss'] = pd.to_numeric(df_log['ProfitLoss'], errors='coerce')
        # Group by Date and Strategy, calculate total P/L and number of bets
        daily_summary = df_log.groupby(['BetDate', 'Strategy'])['ProfitLoss'].agg(['sum', 'count']).reset_index()
        daily_summary.rename(columns={'sum': 'DailyPL', 'count': 'NumBets'}, inplace=True)
        # Calculate cumulative P/L per strategy
        daily_summary['CumulativePL'] = daily_summary.sort_values(by='BetDate').groupby('Strategy')['DailyPL'].cumsum()

        summary_filename = "daily_results_summary.csv"
        summary_path = os.path.join(data_dir_abs, summary_filename)
        print(f"Saving daily summary to: {summary_path}")
        daily_summary.to_csv(summary_path, index=False, encoding='utf-8', float_format='%.2f')
        print("Successfully saved daily summary.")
    except Exception as e:
        print(f"Error generating or saving daily summary: {e}")

    print("\nResults calculation finished.")
