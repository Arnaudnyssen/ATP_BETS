# calculate_results.py (v1 - with Debug Prints)
# Loads strategy log and match results to calculate Profit/Loss.
# Includes print statements to help debug the merge process.

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime, timedelta # Ensure datetime is imported
import traceback
from typing import Optional, List, Dict, Any, Tuple # Added Tuple

# --- Constants ---
DATA_DIR = "data_archive"
STRATEGY_LOG_FILENAME = "strategy_log.csv" # Input log file
RESULTS_CSV_PATTERN = "match_results_*.csv" # Input results files (Base pattern)
# Output file can overwrite log or be separate
UPDATED_LOG_FILENAME = "strategy_log.csv" # Overwrite the log
DATE_FORMAT_LOG = "%Y-%m-%d" # Date format in strategy log BetDate column
DATE_FORMAT_RESULTS = "%Y%m%d" # Date format used in results FILENAMES

# --- Helper Functions ---
# Import key generation functions - crucial for consistent keys
try:
    from process_data import create_merge_key, preprocess_player_name
    print("Successfully imported key helpers from process_data.")
except ImportError:
    print("ERROR: Cannot import helper functions from process_data.py. Ensure it's accessible.")
    # Define dummy functions if import fails, calculations will likely fail.
    def create_merge_key(text: str) -> str:
        print("Warning: Using dummy 'create_merge_key'. Merge may fail.")
        return re.sub(r'\W+', '', text).lower() if isinstance(text, str) else ""
    def preprocess_player_name(name: str) -> Tuple[str, str]:
        print("Warning: Using dummy 'preprocess_player_name'. Merge may fail.")
        key = re.sub(r'\W+', '', name).lower() if isinstance(name, str) else ""
        return name, key

def find_latest_csv(directory: str, pattern: str) -> Optional[str]:
    """Finds the most recently modified CSV file matching the pattern."""
    # (Assuming this function works as intended based on previous usage)
    try:
        if not os.path.isabs(directory):
             script_dir = os.path.dirname(os.path.abspath(__file__))
             search_dir = os.path.join(script_dir, directory)
        else:
             search_dir = directory
        search_path = os.path.join(search_dir, pattern); # Removed print for brevity
        list_of_files = glob.glob(search_path)
        if not list_of_files: return None
        list_of_files = [f for f in list_of_files if os.path.isfile(f)]
        if not list_of_files: return None
        latest_file = max(list_of_files, key=os.path.getmtime); # Removed print for brevity
        return latest_file
    except Exception as e: print(f"Error finding latest CSV file in '{directory}' with pattern '{pattern}': {e}"); traceback.print_exc(); return None


def load_results_data(data_dir: str, log_dates: pd.Series) -> pd.DataFrame:
    """
    Loads results CSVs for specific dates present in the strategy log's
    unprocessed entries.
    """
    results_df_list = []
    # Get unique dates from the log that need processing
    required_dates = log_dates.unique()
    print(f"Need results for dates: {required_dates}")

    for date_str_log_format in required_dates: # e.g., '2025-04-15'
        try:
            # Convert log date string ('YYYY-MM-DD') to results filename date string ('YYYYMMDD')
            date_obj = datetime.strptime(date_str_log_format, DATE_FORMAT_LOG)
            results_date_str_filename = date_obj.strftime(DATE_FORMAT_RESULTS) # e.g., '20250415'
            # Construct the specific filename pattern for this date
            results_filename = f"match_results_{results_date_str_filename}.csv"
            results_file_path = os.path.join(data_dir, results_filename)

            if os.path.exists(results_file_path):
                print(f"Loading results from: {results_filename}")
                df_res = pd.read_csv(results_file_path)
                # --- Crucial: Ensure results file has necessary keys ---
                # Generate keys if they are missing (best practice is for scraper to add them)
                if 'WinnerNameKey' not in df_res.columns and 'WinnerName' in df_res.columns:
                     print(f"  Generating 'WinnerNameKey' for {results_filename}")
                     df_res['WinnerNameKey'] = df_res['WinnerName'].apply(lambda x: preprocess_player_name(x)[1])
                if 'LoserNameKey' not in df_res.columns and 'LoserName' in df_res.columns:
                     print(f"  Generating 'LoserNameKey' for {results_filename}")
                     df_res['LoserNameKey'] = df_res['LoserName'].apply(lambda x: preprocess_player_name(x)[1])
                # TournamentKey is essential for the merge key
                if 'TournamentKey' not in df_res.columns:
                     # Attempt to generate from TournamentName if present
                     if 'TournamentName' in df_res.columns:
                         print(f"  Generating 'TournamentKey' from 'TournamentName' for {results_filename}")
                         df_res['TournamentKey'] = df_res['TournamentName'].apply(create_merge_key)
                     else:
                         # If no TournamentKey or TournamentName, this file can't be used for merging
                         print(f"  ERROR: Cannot generate 'TournamentKey' for {results_filename}. Skipping this file.")
                         continue # Skip to the next date

                # Add the ResultDate in the log format for easier key comparison later
                df_res['ResultDateLogFmt'] = date_str_log_format
                results_df_list.append(df_res)
            else:
                # This is the warning currently being triggered
                print(f"Warning: Results file not found for date {date_str_log_format}: {results_filename}")
        except Exception as e:
            print(f"Error loading or processing results for date {date_str_log_format}: {e}")
            traceback.print_exc()

    if not results_df_list:
        print("No results dataframes were loaded.")
        return pd.DataFrame() # Return empty DataFrame if no files found/loaded
    else:
        print(f"Successfully loaded {len(results_df_list)} results file(s). Concatenating...")
        # Concatenate all loaded results DataFrames
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
        print(f"Error: Strategy log file not found at {log_file_path}. Run simulate_strategies.py first.")
        exit()
    try:
        df_log = pd.read_csv(log_file_path)
        # Standardize BetDate format just in case
        df_log['BetDate'] = pd.to_datetime(df_log['BetDate']).dt.strftime(DATE_FORMAT_LOG)
        print(f"Strategy log loaded. Shape: {df_log.shape}")
        # Check if ProfitLoss column exists, add if not
        if 'ProfitLoss' not in df_log.columns:
            print("ProfitLoss column not found in log, adding it with NaN values.")
            df_log['ProfitLoss'] = np.nan
        if 'MatchResult' not in df_log.columns:
             print("MatchResult column not found in log, adding it.")
             df_log['MatchResult'] = 'Pending' # Or 'Unknown' or pd.NA

    except Exception as e:
        print(f"Error loading strategy log: {e}")
        traceback.print_exc()
        exit()

    if df_log.empty:
        print("Strategy log is empty. Nothing to calculate.")
        exit()

    # Filter log for entries that haven't been processed yet (ProfitLoss is NaN or null)
    # Use pd.isna() to handle potential None/NaN values robustly
    unprocessed_mask = df_log['ProfitLoss'].isna()
    df_log_unprocessed = df_log[unprocessed_mask].copy()

    if df_log_unprocessed.empty:
        print("No unprocessed bets found in the log (ProfitLoss column is not NaN).")
        # Optional: Check if any MatchResult is 'Pending' or 'Unknown'
        pending_results = df_log[df_log['MatchResult'].isin(['Pending', 'Unknown', 'Result Missing'])].shape[0]
        if pending_results > 0:
             print(f"Note: Found {pending_results} bets with MatchResult as Pending/Unknown/Missing, but ProfitLoss is already filled.")
        exit()
    print(f"Found {len(df_log_unprocessed)} unprocessed bets to calculate.")

    # 2. Load Relevant Results Data
    print("\nLoading relevant match results data...")
    # Pass only the dates from the *unprocessed* bets
    df_results = load_results_data(data_dir_abs, df_log_unprocessed['BetDate'])

    # --- EXIT if no results data could be loaded ---
    # This is the current exit point based on the logs provided
    if df_results.empty:
        print("No relevant match results data was loaded (files might be missing or empty). Cannot calculate P/L.")
        exit() # Stop execution if no results are available

    print(f"Results data loaded successfully. Shape: {df_results.shape}")

    # 3. Prepare for Merge
    print("\nPreparing keys for merging...")
    # Ensure keys are present and standardized in both dataframes
    # Use the imported helper functions consistently
    print("Generating keys in unprocessed log data...")
    df_log_unprocessed['TournamentKey'] = df_log_unprocessed['Tournament'].apply(create_merge_key)
    df_log_unprocessed['Player1NameKey'] = df_log_unprocessed['Player1'].apply(lambda x: preprocess_player_name(x)[1])
    df_log_unprocessed['Player2NameKey'] = df_log_unprocessed['Player2'].apply(lambda x: preprocess_player_name(x)[1])

    # Results keys should have been generated in load_results_data if missing
    # Verify required keys exist in results df before creating MatchKey
    required_results_keys = ['TournamentKey', 'WinnerNameKey', 'LoserNameKey', 'ResultDateLogFmt']
    if not all(key in df_results.columns for key in required_results_keys):
         print("ERROR: Results DataFrame is missing required keys for merge after loading. Cannot proceed.")
         missing_keys = [key for key in required_results_keys if key not in df_results.columns]
         print(f"Missing keys in results: {missing_keys}")
         exit()

    print("Generating MatchKey in log data...")
    # Create unique match keys in log df (Date_TournamentKey_SortedPlayerKeys)
    df_log_unprocessed['MatchKey'] = df_log_unprocessed.apply(
        lambda row: f"{row['BetDate']}_{row['TournamentKey']}_" + "_".join(sorted([row['Player1NameKey'], row['Player2NameKey']])), axis=1
    )

    print("Generating MatchKey in results data...")
     # Create unique match keys in results df (Date_TournamentKey_SortedPlayerKeys)
    df_results['MatchKey'] = df_results.apply(
        lambda row: f"{row['ResultDateLogFmt']}_{row['TournamentKey']}_" + "_".join(sorted([row['WinnerNameKey'], row['LoserNameKey']])), axis=1
    )

    # Select only necessary columns from results for the merge
    # Keep 'MatchKey' and the column needed to determine the winner ('WinnerNameKey')
    df_results_slim = df_results[['MatchKey', 'WinnerNameKey', 'Score']].drop_duplicates(subset=['MatchKey'], keep='first')
    print(f"Prepared slim results data for merge. Shape: {df_results_slim.shape}")


    # --- DEBUGGING PRINTS START ---
    print("\n--- Debugging Merge Keys ---")
    if not df_log_unprocessed.empty:
        print("\n--- Log Keys Sample (Unprocessed Bets) ---")
        log_key_cols = ['BetDate', 'TournamentKey', 'Player1NameKey', 'Player2NameKey', 'MatchKey']
        log_key_cols_present = [col for col in log_key_cols if col in df_log_unprocessed.columns]
        if len(log_key_cols_present) == len(log_key_cols):
             print(df_log_unprocessed[log_key_cols_present].head())
        else:
             print(f"Warning: Missing one or more key columns in log: {log_key_cols}")
             print(df_log_unprocessed.head())
    else:
        print("Log DataFrame (unprocessed) is empty.")

    if not df_results_slim.empty:
        print("\n--- Results Keys Sample (Slimmed) ---")
        results_key_cols = ['MatchKey', 'WinnerNameKey', 'Score']
        results_key_cols_present = [col for col in results_key_cols if col in df_results_slim.columns]
        if len(results_key_cols_present) == len(results_key_cols):
            print(df_results_slim[results_key_cols_present].head())
        else:
            print(f"Warning: Missing one or more key columns in results slim: {results_key_cols}")
            print(df_results_slim.head())
    else:
        print("Results DataFrame (slimmed) is empty.")
    # --- DEBUGGING PRINTS END ---


    # 4. Merge Log with Results
    print("\nMerging unprocessed bets with results...")
    # Merge the unprocessed log entries with the slim results based on the MatchKey
    # Keep all log entries ('how=left')
    df_merged = pd.merge(
        df_log_unprocessed,
        df_results_slim,
        on='MatchKey',
        how='left',
        suffixes=('', '_res') # Add suffix to results cols to avoid name clashes if any overlap besides key
    )
    print(f"Merge complete. Shape after merge: {df_merged.shape}")

    # --- DEBUGGING PRINTS START ---
    print("\n--- Merged Data Sample (Showing Match Success) ---")
    # Check if the WinnerNameKey column (from results) is present and non-null after merge
    winner_key_col_merged = 'WinnerNameKey_res' if 'WinnerNameKey_res' in df_merged.columns else 'WinnerNameKey'
    if winner_key_col_merged in df_merged.columns:
        print(df_merged[['MatchKey', 'BetOnPlayer', 'Player1NameKey', 'Player2NameKey', winner_key_col_merged]].head())
        null_winner_keys = df_merged[winner_key_col_merged].isna().sum()
        print(f"\nNumber of rows where '{winner_key_col_merged}' is NaN (merge failed): {null_winner_keys} out of {len(df_merged)}")
    else:
         print(f"Could not find winner key column ('{winner_key_col_merged}') post-merge?")
         print(df_merged.head())
    print("-" * 30) # Separator
     # --- DEBUGGING PRINTS END ---


    # 5. Calculate Profit/Loss
    print("Calculating Profit/Loss for merged rows...")
    # Initialize columns in the merged dataframe
    df_merged['MatchResult'] = 'Pending' # Default
    df_merged['ProfitLoss'] = np.nan # Default

    # Iterate through the merged dataframe to calculate results
    for index, row in df_merged.iterrows():
        # Check if the merge was successful for this row (WinnerNameKey_res is not NaN)
        winner_key_val = row.get(winner_key_col_merged) # Use the potentially suffixed column name
        if pd.isna(winner_key_val):
            # Merge failed for this bet, result unknown
            df_merged.loc[index, 'MatchResult'] = 'Result Missing'
            # ProfitLoss remains NaN
            continue # Skip to next row

        # Merge was successful, determine outcome
        bet_on_p1 = (row['BetOnPlayer'] == 'P1')
        # Check if the winner from results matches P1 or P2 from the log
        p1_won = (winner_key_val == row['Player1NameKey'])
        p2_won = (winner_key_val == row['Player2NameKey'])
        score = row.get('Score_res', row.get('Score', '')) # Get score if available

        if bet_on_p1:
            if p1_won:
                df_merged.loc[index, 'MatchResult'] = f'P1_Win ({score})'
                # Profit = Stake * (Odds - 1)
                df_merged.loc[index, 'ProfitLoss'] = row['BetAmount'] * (row['BetOdds'] - 1)
            elif p2_won:
                df_merged.loc[index, 'MatchResult'] = f'P2_Win ({score})'
                # Loss = -Stake
                df_merged.loc[index, 'ProfitLoss'] = -row['BetAmount']
            else:
                 # Should not happen if keys match correctly
                 df_merged.loc[index, 'MatchResult'] = 'Result Name Mismatch'
                 print(f"Warning: Result Name Mismatch for MatchKey {row['MatchKey']} - WinnerKey: {winner_key_val}, P1Key: {row['Player1NameKey']}, P2Key: {row['Player2NameKey']}")
        else: # Bet on P2
            if p2_won:
                df_merged.loc[index, 'MatchResult'] = f'P2_Win ({score})'
                df_merged.loc[index, 'ProfitLoss'] = row['BetAmount'] * (row['BetOdds'] - 1)
            elif p1_won:
                df_merged.loc[index, 'MatchResult'] = f'P1_Win ({score})'
                df_merged.loc[index, 'ProfitLoss'] = -row['BetAmount']
            else:
                 df_merged.loc[index, 'MatchResult'] = 'Result Name Mismatch'
                 print(f"Warning: Result Name Mismatch for MatchKey {row['MatchKey']} - WinnerKey: {winner_key_val}, P1Key: {row['Player1NameKey']}, P2Key: {row['Player2NameKey']}")

    print("Profit/Loss calculation complete for processed rows.")
    # Display summary of results calculated
    results_calculated_count = df_merged['ProfitLoss'].notna().sum()
    print(f"Successfully calculated P/L for {results_calculated_count} bets.")


    # 6. Update the original log DataFrame
    print("\nUpdating original strategy log with calculated results...")
    # Use the index from df_log_unprocessed (which matches df_merged) to update df_log
    update_cols = ['MatchResult', 'ProfitLoss']
    # Check if columns exist in df_merged before updating
    update_cols_present = [col for col in update_cols if col in df_merged.columns]
    if update_cols_present:
        # Set index on df_log using the index of the unprocessed rows
        # This ensures we update the correct rows in the original log
        df_log.set_index(df_log_unprocessed.index, inplace=True)
        # Update df_log with the calculated values from df_merged using the common index
        df_log.update(df_merged[update_cols_present])
        # Reset index back to default integer index
        df_log.reset_index(drop=True, inplace=True)
        print("Original log DataFrame updated.")
    else:
        print("Warning: Update columns ('MatchResult', 'ProfitLoss') not found in merged data. Log not updated.")


    # 7. Save Updated Log File
    try:
        print(f"Saving updated strategy log to: {updated_log_path}")
        # Overwrite the original log file with the updated data
        # Use float_format to control precision of P/L values
        df_log.to_csv(updated_log_path, index=False, encoding='utf-8', float_format='%.4f')
        print("Successfully saved updated log.")
    except Exception as e:
        print(f"Error writing updated strategy log file '{updated_log_path}': {e}")
        traceback.print_exc()

    # --- Optional: Generate Daily Summary ---
    try:
        print("\nGenerating daily performance summary...")
        # Ensure ProfitLoss is numeric for aggregation, coercing errors
        df_log['ProfitLoss'] = pd.to_numeric(df_log['ProfitLoss'], errors='coerce')
        # Filter out rows where ProfitLoss is still NaN before grouping
        df_summary_input = df_log.dropna(subset=['ProfitLoss'])

        if not df_summary_input.empty:
            # Group by Date and Strategy, calculate total P/L and number of bets
            daily_summary = df_summary_input.groupby(['BetDate', 'Strategy'])['ProfitLoss'].agg(['sum', 'count']).reset_index()
            daily_summary.rename(columns={'sum': 'DailyPL', 'count': 'NumBets'}, inplace=True)
            # Calculate cumulative P/L per strategy
            daily_summary['CumulativePL'] = daily_summary.sort_values(by='BetDate').groupby('Strategy')['DailyPL'].cumsum()

            summary_filename = "daily_results_summary.csv"
            summary_path = os.path.join(data_dir_abs, summary_filename)
            print(f"Saving daily summary to: {summary_path}")
            daily_summary.to_csv(summary_path, index=False, encoding='utf-8', float_format='%.2f')
            print("Successfully saved daily summary.")
        else:
            print("No data with calculated ProfitLoss found to generate summary.")
    except Exception as e:
        print(f"Error generating or saving daily summary: {e}")
        traceback.print_exc()

    print("\nResults calculation finished.")
