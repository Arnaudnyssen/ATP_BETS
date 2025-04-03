# p_sack_preproc.py (Use Headers Provided by Scraper)

import pandas as pd
import numpy as np
import re
import traceback
from typing import List, Optional, Dict, Any

# --- Scraper imports remain inside the function that uses them ---


# --- REMOVED find_headers_cols function ---
# This function is no longer needed as headers are provided by the scraper.


# --- Helper Functions ---
# (table_data_to_df, clean_df, get_active_round_and_columns,
#  preprocess_player_names remain largely the same, but table_data_to_df
#  no longer needs complex logic to find start of data)

# MODIFIED table_data_to_df to directly use provided headers
def table_data_to_df(headers: List[str], table_data: List[Any], url: str) -> Optional[pd.DataFrame]:
    """
    Converts the raw list of scraped data into a pandas DataFrame using the provided headers.
    Adds the source URL as a column. Returns None if DataFrame creation fails.
    Assumes table_data is a flat list corresponding to the headers.
    """
    if not headers:
        print(f"Error: No headers provided for URL {url}. Cannot create DataFrame.")
        return None

    num_headers = len(headers)
    if num_headers == 0:
         print(f"Error: Zero headers provided for URL {url}. Cannot create DataFrame.")
         return None
    print(f"--- Debug: Creating DataFrame with {num_headers} provided headers: {headers} ---")

    if not table_data:
        print(f"Error: Empty table_data received for URL {url}. Cannot create DataFrame.")
        return None

    # Calculate expected number of data points
    num_data_points = len(table_data)
    if num_data_points % num_headers != 0:
        print(f"--- Debug: Warning - Number of data points ({num_data_points}) is not a multiple of the number of headers ({num_headers}) for URL {url}. Table might be incomplete or parsing error occurred. ---")
        # Decide how to handle: truncate or return None? Truncating might hide issues. Let's try to proceed but log clearly.
        num_rows = num_data_points // num_headers
        print(f"--- Debug: Attempting to create DataFrame with {num_rows} full rows. ---")
        if num_rows == 0:
            print("Error: Not enough data points to form even one full row.")
            return None
        # Truncate data to the largest multiple of num_headers
        table_data = table_data[:num_rows * num_headers]
    else:
        num_rows = num_data_points // num_headers
        print(f"--- Debug: Reshaping {num_data_points} data points into {num_rows} rows of {num_headers} columns. ---")

    # Reshape the flat list into a list of lists (rows)
    try:
        data_rows = [table_data[i:i + num_headers] for i in range(0, len(table_data), num_headers)]
    except Exception as e:
        print(f"Error reshaping data list into rows for URL {url}: {e}")
        traceback.print_exc()
        return None

    if not data_rows:
         print(f"Error: No data rows could be formed after reshaping for URL {url}.")
         return None

    print(f"--- Debug: Reshaped into {len(data_rows)} potential data rows. Creating DataFrame... ---")
    try:
        # Create DataFrame directly from reshaped data and provided headers
        df = pd.DataFrame(data_rows, columns=headers)

        # Basic validation: Check if 'Player' column exists (it should if headers were captured correctly)
        if "Player" not in df.columns:
             print(f"--- Debug: Warning - 'Player' column not found in provided headers {df.columns}. Check header capturing in scraper. Attempting to use first column. ---")
             if df.shape[1] > 0:
                  df.rename(columns={df.columns[0]: "Player"}, inplace=True) # Assume first column is Player
             else:
                  print("Error: DataFrame created with no columns.")
                  return None

        # Set index and add URL
        df.set_index("Player", inplace=True)
        df['Tournament_URL'] = url
        print(f"--- Debug: DataFrame created successfully with shape {df.shape} for URL {url}. ---")
        return df
    except ValueError as ve:
         print(f"Error creating DataFrame for URL {url}: {ve}")
         print(f"--- Debug: Mismatch likely between number of headers ({num_headers}) and reshaped data structure. ---")
         print(f"--- Debug: First few reshaped data rows: {data_rows[:3]} ---")
         return None
    except Exception as e:
        print(f"Unexpected error creating DataFrame for URL {url}: {e}")
        traceback.print_exc()
        return None

# --- clean_df, get_active_round_and_columns, preprocess_player_names remain the same as previous debug version ---
# (No changes needed here as they operate on the DataFrame after creation)
def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the DataFrame by removing irrelevant rows/columns and handling NaNs."""
    print("--- Debug: Entering clean_df ---") # DEBUG LOG
    print(f"--- Debug: Initial shape: {df.shape} ---") # DEBUG LOG
    potential_numeric_cols = [col for col in df.columns if col not in ['Tournament_URL']]
    print(f"--- Debug: Potential numeric cols: {potential_numeric_cols} ---") # DEBUG LOG
    for col in potential_numeric_cols:
         if col not in df.columns: continue # Skip if column doesn't exist
         if pd.api.types.is_string_dtype(df[col]) or df[col].dtype == 'object':
              if df[col].astype(str).str.contains('%').any():
                   print(f"--- Debug: Removing '%' from column '{col}' ---") # DEBUG LOG
                   df[col] = df[col].astype(str).str.replace('%', '', regex=False)
         print(f"--- Debug: Converting column '{col}' to numeric ---") # DEBUG LOG
         df[col] = pd.to_numeric(df[col], errors='coerce')

    rows_before = df.shape[0]
    cols_before = df.shape[1]
    df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
    rows_after = df.shape[0]
    cols_after = df.shape[1]
    print(f"--- Debug: Dropped {rows_before - rows_after} all-NaN rows. ---") # DEBUG LOG
    print(f"--- Debug: Dropped {cols_before - cols_after} all-NaN columns. ---") # DEBUG LOG
    print(f"--- Debug: Final shape after cleaning: {df.shape} ---") # DEBUG LOG
    print("Cleaning complete.")
    return df

def get_active_round_and_columns(df: pd.DataFrame) -> Optional[str]:
    """
    Identifies the current active round based on probabilities not being 0 or 100.
    Returns the column name of the active round, or None if no active round found.
    Assumes numeric conversion and cleaning have happened.
    """
    print("--- Debug: Entering get_active_round_and_columns ---") # DEBUG LOG
    round_col_names = ["R128", "R64", "R32", "R16", "QF", "SF", "F", "W"]
    potential_round_cols = [col for col in df.columns if col in round_col_names]
    ordered_round_cols = [col for col in round_col_names if col in potential_round_cols]

    if not ordered_round_cols:
        print("--- Debug: Warning - Could not identify potential round columns based on typical names (R128, R64...). ---") # DEBUG LOG
        return None

    print(f"--- Debug: Potential round columns identified and ordered: {ordered_round_cols} ---") # DEBUG LOG

    for round_col in ordered_round_cols:
        if round_col not in df.columns: continue

        print(f"--- Debug: Checking round column '{round_col}' for active status... ---") # DEBUG LOG
        if pd.api.types.is_numeric_dtype(df[round_col]):
            percentages = df[round_col].dropna()
            if percentages.empty:
                 print(f"--- Debug: Round column '{round_col}' contains only NaN values. Skipping. ---") # DEBUG LOG
                 continue
            is_active = (percentages > 0.01) & (percentages < 99.99)
            if is_active.any():
                print(f"--- Debug: Active round identified: {round_col} (found values between 0.01 and 99.99) ---") # DEBUG LOG
                return round_col
            else:
                 finished_check = percentages.isin([0, 100])
                 if finished_check.all():
                     print(f"--- Debug: Round '{round_col}' appears finished (only 0, 100, or NaN values). ---") # DEBUG LOG
                 else:
                     print(f"--- Debug: Warning - Round '{round_col}' has unexpected numeric values not in [0, 100]: {percentages[~finished_check].unique()} ---") # DEBUG LOG
        else:
             print(f"--- Debug: Warning - Column '{round_col}' is not numeric after cleaning, cannot check for active status. Type: {df[round_col].dtype} ---") # DEBUG LOG

    print("--- Debug: No active round found (all rounds might be finished, empty, or data is missing/unparseable). ---") # DEBUG LOG
    return None

def preprocess_player_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes player names in the DataFrame index or 'Player' column."""
    print("--- Debug: Entering preprocess_player_names ---") # DEBUG LOG
    if isinstance(df.index, pd.MultiIndex):
         print("--- Debug: Warning - MultiIndex found, cannot preprocess player names in index. ---") # DEBUG LOG
         return df

    target_column_data = None
    is_index = False
    original_target_name = None

    if df.index.name == 'Player':
        print("--- Debug: Processing 'Player' index. ---") # DEBUG LOG
        target_column_data = df.index
        original_target_name = df.index.name
        is_index = True
    elif 'Player' in df.columns:
        print("--- Debug: Processing 'Player' column. ---") # DEBUG LOG
        target_column_data = df['Player']
        original_target_name = 'Player'
        is_index = False
    else:
         print("--- Debug: Warning - Neither 'Player' index nor 'Player' column found for preprocessing. ---") # DEBUG LOG
         return df

    if target_column_data is None:
         print("--- Debug: Error - Target column data is None. Skipping preprocessing. ---") # DEBUG LOG
         return df

    original_target_copy = target_column_data.copy()

    try:
        processed_target = target_column_data.astype(str)
        processed_target = processed_target.str.replace(r'\s*\(\d+\)', '', regex=True)
        processed_target = processed_target.str.replace(r'\s*\([A-Z]{3}\)', '', regex=True)
        processed_target = processed_target.str.replace(r'\s*\(WC\)', '', regex=True)
        processed_target = processed_target.str.replace(r'\s*\(Q\)', '', regex=True)
        processed_target = processed_target.str.replace(r'\s*\(LL\)', '', regex=True)
        processed_target = processed_target.str.replace(r'\s*\(SE\)', '', regex=True)
        processed_target = processed_target.str.replace(r'\s*\(PR\)', '', regex=True)
        processed_target = processed_target.str.replace(r'^\*|\*$', '', regex=True)
        processed_target = processed_target.str.strip().str.lower()

        if is_index:
            df.index = processed_target
            df.index.name = original_target_name
        else:
            df['Player'] = processed_target
        print("--- Debug: Player name preprocessing applied. ---") # DEBUG LOG
    except AttributeError as e:
        print(f"--- Debug: Could not preprocess player names, likely not string type or regex error: {e} ---") # DEBUG LOG
        if is_index: df.index = original_target_copy; df.index.name = original_target_name
        else: df['Player'] = original_target_copy
    except Exception as e:
        print(f"--- Debug: Unexpected error during player name preprocessing: {e} ---"); traceback.print_exc()
        try:
            if is_index: df.index = original_target_copy; df.index.name = original_target_name
            else: df['Player'] = original_target_copy
        except: pass

    return df


# --- Main Processing Logic ---

# MODIFIED process_sackmann_table to accept headers
def process_sackmann_table(headers: List[str], table_data: List[Any], url: str) -> Optional[pd.DataFrame]:
    """
    Processes scraped data for a single Tennis Abstract tournament URL using provided headers.
    Returns a DataFrame with Player, Round, Probability (%), and Decimal Odds,
    or None if processing fails.
    """
    print(f"\n--- Debug: Entering process_sackmann_table for URL: {url} ---")
    # Basic validation of inputs
    if not table_data:
        print("--- Debug: Error - Received empty table_data list. Cannot process. ---")
        return None
    if not headers:
        print("--- Debug: Error - Received empty headers list. Cannot process. ---")
        # This case should ideally be handled before calling this function
        return None

    try:
        # --- DataFrame Creation (using provided headers) ---
        print(f"--- Debug: Attempting DataFrame creation with {len(headers)} provided headers... ---")
        df_raw = table_data_to_df(headers, table_data, url) # Call the modified function

        if df_raw is None:
            print("--- Debug: Error - Failed to create DataFrame (table_data_to_df returned None). ---")
            return None
        print(f"--- Debug: Raw DataFrame created with shape: {df_raw.shape} ---")

        # --- Preprocessing & Cleaning ---
        df_processed = preprocess_player_names(df_raw.copy())
        df_cleaned = clean_df(df_processed)
        print(f"--- Debug: Cleaned DataFrame shape: {df_cleaned.shape} ---")

        if df_cleaned.empty:
            print("--- Debug: DataFrame is empty after cleaning. No data to process further. ---")
            # Return None because even if headers exist, there's no data
            return None

        # --- Active Round Identification ---
        active_round = get_active_round_and_columns(df_cleaned)
        if active_round is None:
            print("--- Debug: Warning - Could not determine the active round for odds calculation. Cannot create final output. ---")
            return None # Return None as no odds can be calculated

        # This check might be redundant if get_active_round ensures column exists, but safe to keep
        if active_round not in df_cleaned.columns:
             print(f"--- Debug: Error - Active round column '{active_round}' not found after cleaning. Columns: {df_cleaned.columns} ---")
             return None

        # --- Final DataFrame Assembly ---
        print(f"--- Debug: Assembling final DataFrame using active round: '{active_round}' ---")
        df_final = pd.DataFrame(index=df_cleaned.index) # Start with player index
        df_final['Probability (%)'] = df_cleaned[active_round]

        # Calculate Decimal Odds
        if pd.api.types.is_numeric_dtype(df_final['Probability (%)']):
             valid_probs = df_final['Probability (%)'].dropna() / 100.0
             valid_probs = valid_probs[valid_probs > 0]
             if not valid_probs.empty:
                  df_final['Decimal_Odds'] = (1 / valid_probs).round(2)
             else:
                  df_final['Decimal_Odds'] = np.nan
             df_final['Decimal_Odds'] = df_final['Decimal_Odds'].fillna(np.nan) # Ensure NaNs remain NaN
             print(f"--- Debug: Calculated Decimal_Odds. ---")
        else:
             print(f"--- Debug: Warning - Probability column '{active_round}' is not numeric. Cannot calculate odds. Setting Decimal_Odds to NaN. ---")
             df_final['Decimal_Odds'] = np.nan

        df_final['Round'] = active_round
        df_final['Tournament_URL'] = url

        df_final.reset_index(inplace=True) # Make 'Player' a column again

        final_cols = ['Tournament_URL', 'Round', 'Player', 'Probability (%)', 'Decimal_Odds']
        existing_final_cols = [col for col in final_cols if col in df_final.columns]
        df_final = df_final[existing_final_cols]

        print(f"--- Debug: Successfully processed. Final DataFrame shape: {df_final.shape} ---")
        return df_final

    # No longer need specific ValueError catch for find_headers_cols
    except Exception as e:
        print(f"--- Debug: An unexpected error occurred during processing for {url}: {e} ---")
        traceback.print_exc()
        return None


# MODIFIED get_all_sackmann_data to handle tuple return and pass headers
def get_all_sackmann_data() -> pd.DataFrame:
    """
    Scrapes and processes Sackmann data for all relevant Tennis Abstract URLs.
    Returns a consolidated DataFrame with data from all tournaments.
    """
    # --- Scraper Import ---
    try:
        from tennis_abstract_scraper import tourneys_url, probas_scraper
        print("Successfully imported from tennis_abstract_scraper inside function.")
    except ImportError as e:
        print(f"Error importing from tennis_abstract_scraper inside function: {e}")
        return pd.DataFrame()
    # --- End Import ---

    urls = tourneys_url()
    print(f"Found {len(urls)} tournament URLs to scrape.")
    all_data_dfs = []

    if not urls:
        print("No tournament URLs found. Returning empty DataFrame.")
        return pd.DataFrame()

    for url in urls:
        print(f"\n--- Processing URL: {url} ---")
        try:
            # Step 1: Scrape raw data AND HEADERS
            print("--- Debug: Calling probas_scraper... ---")
            # **** UNPACK RETURNED TUPLE ****
            headers, table_data = probas_scraper(url)

            # **** VALIDATE SCRAPED RESULTS ****
            if not headers:
                print(f"--- Debug: Warning - No headers identified by scraper for {url}. Skipping processing for this URL. ---")
                continue # Skip if headers couldn't be found
            if not table_data:
                 print(f"--- Debug: No data scraped from {url}. Skipping. ---")
                 continue
            print(f"--- Debug: Scraped {len(headers)} headers and {len(table_data)} raw data points from {url}. ---")

            # Step 2: Process scraped data using the scraped headers
            print("--- Debug: Calling process_sackmann_table... ---")
            # **** PASS HEADERS TO PROCESSING FUNCTION ****
            processed_df = process_sackmann_table(headers, table_data, url)

            # Step 3: Check result and append
            if processed_df is not None and not processed_df.empty:
                print(f"--- Debug: Successfully processed data for {url}. Shape: {processed_df.shape} ---")
                all_data_dfs.append(processed_df)
            elif processed_df is None:
                 print(f"--- Debug: Processing failed for {url} (returned None). ---")
            else: # processed_df is an empty DataFrame
                 print(f"--- Debug: Processing resulted in an empty DataFrame for {url}. ---")

        except Exception as e:
            print(f"--- Debug: Critical error during scrape/process loop for {url}: {e} ---")
            traceback.print_exc()
            print(f"--- Debug: Skipping to next URL due to error. ---")
            continue

    # --- Final Concatenation ---
    if not all_data_dfs:
        print("\n--- Debug: No data collected from any URL after processing. Returning empty DataFrame. ---")
        return pd.DataFrame()

    try:
        print(f"\n--- Debug: Concatenating {len(all_data_dfs)} processed DataFrames... ---")
        final_sackmann_data = pd.concat(all_data_dfs, ignore_index=True)
        # Add a timestamp column right before returning
        final_sackmann_data['ScrapeTimestampUTC'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S %Z')
        print(f"\n--- Debug: Consolidated Sackmann data shape: {final_sackmann_data.shape} ---")
        return final_sackmann_data
    except Exception as e:
         print(f"--- Debug: Error during final concatenation: {e} ---")
         traceback.print_exc()
         return pd.DataFrame()


# Example usage (remains the same)
if __name__ == "__main__":
    print("Fetching and processing Sackmann data...")
    start_time = pd.Timestamp.now()
    print(f"Start time: {start_time}")

    sackmann_data = get_all_sackmann_data()

    end_time = pd.Timestamp.now()
    print(f"\nEnd time: {end_time}")
    print(f"Total processing time: {end_time - start_time}")

    if not sackmann_data.empty:
        print("\n--- Sample of Processed Sackmann Data ---")
        print(sackmann_data.head())
        print("\n--- Data Info ---")
        sackmann_data.info()
    else:
        print("\n--- No Sackmann data was processed or collected. ---")

