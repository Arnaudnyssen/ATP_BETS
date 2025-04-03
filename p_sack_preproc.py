# p_sack_preproc.py (with Debug Logging for Header Issue)

import pandas as pd
import numpy as np
import re
import traceback # Import traceback
from typing import List, Optional, Dict, Any

# --- Move scraper imports inside the function that uses them ---
# (Keep this structure)
# try:
#     from tennis_abstract_scraper import tourneys_url, probas_scraper
# except ImportError as e:
#     # ... (error handling)


# --- Helper Functions ---

# MODIFIED find_headers_cols with more logging
def find_headers_cols(table_data: List[Any]) -> List[str]:
    """
    Identifies header columns from the raw scraped table data.
    Assumes headers are strings appearing before the first player name containing '('.
    """
    print("--- Debug: Entering find_headers_cols ---") # DEBUG LOG
    headers = []
    player_found = False
    # Define the common headers expected
    common_headers = ["Player", "Seed", "Rank", "R128", "R64", "R32", "R16", "QF", "SF", "F", "W", "Pts"]
    potential_headers = []

    print(f"--- Debug: First 20 elements of input table_data: {table_data[:20]} ---") # DEBUG LOG

    # Iterate through the data to find potential headers before the first player
    for i, element in enumerate(table_data):
        element_str = str(element).strip()
        if not element_str: continue

        # Player detection logic (check for parentheses and letters)
        is_player_name = isinstance(element, str) and '(' in element_str and ')' in element_str and any(c.isalpha() for c in element_str)

        if is_player_name:
            print(f"--- Debug: Player name found at index {i}: '{element_str}'. Stopping header search. ---") # DEBUG LOG
            player_found = True
            break # Stop searching for headers once a player name is encountered

        # If player not found yet, consider this element as a potential header
        if not player_found:
             # Basic check: is it likely data (number or percentage)?
             is_number = element_str.replace('.', '', 1).isdigit()
             is_percentage = '%' in element_str or (isinstance(element, (float, int)) and 0 <= element <= 100) # Check numeric too

             # If it's not obviously data, add to potential headers
             if not is_number and not is_percentage:
                  potential_headers.append(element_str)
             else:
                 # If we encounter data before finding a player, something is wrong
                 print(f"--- Debug: Encountered potential data '{element_str}' at index {i} before finding a player name. Header structure might be unexpected. ---") # DEBUG LOG


    print(f"--- Debug: Potential headers found before first player: {potential_headers} ---") # DEBUG LOG

    # Filter potential headers against the known common headers
    headers = [h for h in potential_headers if h in common_headers]
    print(f"--- Debug: Headers matched against common_headers: {headers} ---") # DEBUG LOG

    # Fallback heuristic if no common headers were matched
    if not headers and potential_headers:
         print("--- Debug: Common headers not found, using heuristic (first non-numeric/non-% strings)... ---") # DEBUG LOG
         # Take the initial sequence of potential headers found
         headers = potential_headers
         print(f"--- Debug: Headers based on heuristic: {headers} ---") # DEBUG LOG

    # Ensure 'Player' is the first header if found
    if "Player" in headers:
        if headers[0] != "Player":
            print("--- Debug: Moving 'Player' header to the first position. ---") # DEBUG LOG
            headers.remove("Player")
            headers.insert(0, "Player")
    elif headers:
        # If headers were found but 'Player' isn't one, assume the first one is Player
        # This might be risky if the first column isn't actually Player
        print("--- Debug: 'Player' header not explicitly found, assuming first identified header is Player. ---") # DEBUG LOG
        # Let's not forcibly insert 'Player' if it wasn't found, rely on table_data_to_df to handle it.
        pass
    else:
        # If absolutely no headers could be identified by any method
        print("--- Debug: No headers identified by common list or heuristic. Raising ValueError. ---") # DEBUG LOG
        raise ValueError("Could not determine headers for the table data.")

    print(f"--- Debug: Final identified headers: {headers} ---") # DEBUG LOG
    return headers


# --- table_data_to_df, clean_df, get_active_round_and_columns, preprocess_player_names remain the same ---
# (No changes needed in these functions for this specific debug step)
def table_data_to_df(headers: List[str], table_data: List[Any], url: str) -> Optional[pd.DataFrame]:
    """
    Converts the raw list of scraped data into a pandas DataFrame using the identified headers.
    Adds the source URL as a column. Returns None if DataFrame creation fails.
    Handles tables with interspersed header rows.
    """
    if not headers:
        print(f"Error: No headers provided for URL {url}. Cannot create DataFrame.")
        return None

    num_headers = len(headers)
    if num_headers == 0:
         print(f"Error: Zero headers identified for URL {url}. Cannot create DataFrame.")
         return None
    print(f"--- Debug: Creating DataFrame with {num_headers} headers: {headers} ---") # DEBUG LOG

    data_rows = []
    current_row = []
    # Simple pattern to identify potential repeated headers within the data rows
    header_pattern = re.compile(r"Player|R16|QF|SF|F|W") # Keep this simple

    # Attempt to find the actual start of the data rows more reliably
    # Find the first occurrence of what looks like a player name
    data_start_index = -1
    for i, item in enumerate(table_data):
        item_str = str(item).strip()
        if isinstance(item, str) and '(' in item_str and ')' in item_str and any(c.isalpha() for c in item_str):
            data_start_index = i
            print(f"--- Debug: Found first likely player name '{item_str}' at index {i}. Starting data row processing from here. ---") # DEBUG LOG
            break

    if data_start_index == -1:
        print("--- Debug: Warning - Could not find a likely player name using '(COUNTRY)' pattern. Attempting to process from beginning of table_data. This might include headers. ---") # DEBUG LOG
        data_start_index = 0 # Process from the start if no player found

    relevant_data = table_data[data_start_index:]
    print(f"--- Debug: Processing {len(relevant_data)} elements for data rows. ---") # DEBUG LOG

    row_buffer = []
    for i, item in enumerate(relevant_data):
        item_str = str(item).strip()

        # Add item to buffer
        row_buffer.append(item)

        # Check if buffer is full
        if len(row_buffer) == num_headers:
            # Check if this looks like a repeated header row before adding
            first_cell_text = str(row_buffer[0]).strip()
            is_likely_header_row = False
            if header_pattern.match(first_cell_text):
                 # Check if most other cells are headers or contain '%' (similar to scraper logic)
                 header_like_count = sum(1 for c in row_buffer if isinstance(c,str) and (header_pattern.match(c.strip()) or '%' in c))
                 if header_like_count >= num_headers // 2:
                      is_likely_header_row = True

            if is_likely_header_row:
                 print(f"--- Debug: Skipping likely repeated header row found in data: {row_buffer[:5]}... ---") # DEBUG LOG
            else:
                 # Add the completed row to data_rows
                 data_rows.append(list(row_buffer)) # Add a copy
            # Clear buffer for next row
            row_buffer = []

    # Handle any remaining items in the buffer (incomplete row)
    if row_buffer:
        print(f"--- Debug: Warning - Discarding incomplete trailing row ({len(row_buffer)}/{num_headers} items): {row_buffer} ---") # DEBUG LOG

    if not data_rows:
         print(f"Error: No valid data rows extracted for URL {url} after processing.")
         return None

    print(f"--- Debug: Extracted {len(data_rows)} potential data rows. Creating DataFrame... ---") # DEBUG LOG
    try:
        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=headers)

        # Check if 'Player' column exists, if not, assume first column is Player
        if "Player" not in df.columns:
             if df.shape[1] > 0:
                  print(f"--- Debug: Warning - 'Player' column not found in headers {df.columns}. Renaming first column '{df.columns[0]}' to 'Player'. ---") # DEBUG LOG
                  df.rename(columns={df.columns[0]: "Player"}, inplace=True)
             else:
                  print("Error: DataFrame created with no columns.")
                  return None # Cannot proceed if no columns

        # Validate Player column format (optional, for debugging)
        if 'Player' in df.columns and not df['Player'].astype(str).str.contains(r'\(', regex=True).any():
             print("--- Debug: Warning - Player names might not be in the expected 'Name (COUNTRY/SEED)' format after DataFrame creation. Check scraping/parsing. ---") # DEBUG LOG

        # Set index and add URL
        df.set_index("Player", inplace=True)
        df['Tournament_URL'] = url
        print(f"--- Debug: DataFrame created successfully with shape {df.shape} for URL {url}. ---") # DEBUG LOG
        return df
    except ValueError as ve:
         print(f"Error creating DataFrame for URL {url}: {ve}")
         print(f"--- Debug: Mismatch likely between number of headers ({num_headers}) and data rows structure. ---")
         print(f"--- Debug: First few data rows passed to DataFrame: {data_rows[:3]} ---")
         return None
    except Exception as e:
        print(f"Unexpected error creating DataFrame for URL {url}: {e}")
        traceback.print_exc()
        return None

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the DataFrame by removing irrelevant rows/columns and handling NaNs."""
    print("--- Debug: Entering clean_df ---") # DEBUG LOG
    print(f"--- Debug: Initial shape: {df.shape} ---") # DEBUG LOG
    potential_numeric_cols = [col for col in df.columns if col not in ['Tournament_URL']]
    print(f"--- Debug: Potential numeric cols: {potential_numeric_cols} ---") # DEBUG LOG
    for col in potential_numeric_cols:
         if col not in df.columns: continue # Skip if column doesn't exist
         # Ensure column is string type before using .str accessor
         if pd.api.types.is_string_dtype(df[col]) or df[col].dtype == 'object':
              if df[col].astype(str).str.contains('%').any():
                   print(f"--- Debug: Removing '%' from column '{col}' ---") # DEBUG LOG
                   df[col] = df[col].astype(str).str.replace('%', '', regex=False)
         # Convert to numeric, coercing errors
         print(f"--- Debug: Converting column '{col}' to numeric ---") # DEBUG LOG
         df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop rows/columns that are entirely NaN
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
    # Order them correctly based on the predefined list
    ordered_round_cols = [col for col in round_col_names if col in potential_round_cols]

    if not ordered_round_cols:
        print("--- Debug: Warning - Could not identify potential round columns based on typical names (R128, R64...). ---") # DEBUG LOG
        return None

    print(f"--- Debug: Potential round columns identified and ordered: {ordered_round_cols} ---") # DEBUG LOG

    for round_col in ordered_round_cols:
        if round_col not in df.columns: continue # Should not happen if ordered_round_cols is derived from df.columns, but safe check

        print(f"--- Debug: Checking round column '{round_col}' for active status... ---") # DEBUG LOG
        if pd.api.types.is_numeric_dtype(df[round_col]):
            percentages = df[round_col].dropna() # Drop NaNs before checking conditions
            if percentages.empty:
                 print(f"--- Debug: Round column '{round_col}' contains only NaN values. Skipping. ---") # DEBUG LOG
                 continue

            # Check if any value is strictly between 0 and 100 (excluding NaN)
            # Use a small epsilon to handle potential floating point inaccuracies if needed, but direct comparison should be fine here.
            is_active = (percentages > 0.01) & (percentages < 99.99)

            if is_active.any():
                print(f"--- Debug: Active round identified: {round_col} (found values between 0.01 and 99.99) ---") # DEBUG LOG
                return round_col
            else:
                 # Check if all values are 0 or 100 (or were NaN initially)
                 finished_check = percentages.isin([0, 100])
                 if finished_check.all():
                     print(f"--- Debug: Round '{round_col}' appears finished (only 0, 100, or NaN values). ---") # DEBUG LOG
                 else:
                     # This case means values exist but are outside (0, 100) and not exactly 0 or 100. Should be rare.
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

    original_target_copy = target_column_data.copy() # Keep a copy for potential rollback

    try:
        # Ensure data is treated as string for replacement operations
        processed_target = target_column_data.astype(str)
        # Chain replacements for various patterns (seed, country code, qualifiers etc.)
        processed_target = processed_target.str.replace(r'\s*\(\d+\)', '', regex=True) # Seed: (1), (12)
        processed_target = processed_target.str.replace(r'\s*\([A-Z]{3}\)', '', regex=True) # Country: (USA), (FRA)
        processed_target = processed_target.str.replace(r'\s*\(WC\)', '', regex=True) # Wild Card
        processed_target = processed_target.str.replace(r'\s*\(Q\)', '', regex=True) # Qualifier
        processed_target = processed_target.str.replace(r'\s*\(LL\)', '', regex=True) # Lucky Loser
        processed_target = processed_target.str.replace(r'\s*\(SE\)', '', regex=True) # Special Exempt
        processed_target = processed_target.str.replace(r'\s*\(PR\)', '', regex=True) # Protected Ranking
        # Remove leading/trailing asterisks sometimes used for notes
        processed_target = processed_target.str.replace(r'^\*|\*$', '', regex=True)
        # Final strip and lowercase
        processed_target = processed_target.str.strip().str.lower()

        # Apply the processed names back to the DataFrame
        if is_index:
            df.index = processed_target
            df.index.name = original_target_name # Preserve original index name ('Player')
        else:
            df['Player'] = processed_target

        print("--- Debug: Player name preprocessing applied. ---") # DEBUG LOG

    except AttributeError as e:
        print(f"--- Debug: Could not preprocess player names, likely not string type or regex error: {e} ---") # DEBUG LOG
        # Rollback to original names if processing failed
        if is_index:
            df.index = original_target_copy
            df.index.name = original_target_name
        else:
            df['Player'] = original_target_copy
    except Exception as e:
        print(f"--- Debug: Unexpected error during player name preprocessing: {e} ---") # DEBUG LOG
        traceback.print_exc()
        # Attempt rollback
        try:
            if is_index:
                df.index = original_target_copy
                df.index.name = original_target_name
            else:
                df['Player'] = original_target_copy
        except: pass # Ignore rollback errors

    return df


# --- Main Processing Logic ---

# MODIFIED process_sackmann_table with more logging around header finding
def process_sackmann_table(table_data: List[Any], url: str) -> Optional[pd.DataFrame]:
    """
    Processes scraped data for a single Tennis Abstract tournament URL.
    Returns a DataFrame with Player, Round, Probability (%), and Decimal Odds,
    or None if processing fails.
    """
    print(f"\n--- Debug: Entering process_sackmann_table for URL: {url} ---") # DEBUG LOG
    if not table_data:
        print("--- Debug: Error - Received empty table_data list. Cannot process. ---") # DEBUG LOG
        return None

    try:
        # --- Header Finding ---
        print("--- Debug: Attempting to find headers... ---") # DEBUG LOG
        headers = find_headers_cols(table_data) # Call the modified function
        # If find_headers_cols raises ValueError, it will be caught by the outer except block

        print(f"--- Debug: Headers successfully found: {headers}. Proceeding to DataFrame creation. ---") # DEBUG LOG

        # --- DataFrame Creation ---
        df_raw = table_data_to_df(headers, table_data, url)
        if df_raw is None:
            print("--- Debug: Error - Failed to create DataFrame (table_data_to_df returned None). ---") # DEBUG LOG
            return None
        print(f"--- Debug: Raw DataFrame created with shape: {df_raw.shape} ---") # DEBUG LOG
        # print(f"--- Debug: Raw DataFrame head:\n{df_raw.head()} ---") # Optional: Log head

        # --- Preprocessing & Cleaning ---
        df_processed = preprocess_player_names(df_raw.copy())
        df_cleaned = clean_df(df_processed)
        print(f"--- Debug: Cleaned DataFrame shape: {df_cleaned.shape} ---") # DEBUG LOG
        # print(f"--- Debug: Cleaned DataFrame head:\n{df_cleaned.head()} ---") # Optional: Log head

        if df_cleaned.empty:
            print("--- Debug: DataFrame is empty after cleaning. No data to process further. ---") # DEBUG LOG
            return None

        # --- Active Round Identification ---
        active_round = get_active_round_and_columns(df_cleaned)
        if active_round is None:
            print("--- Debug: Warning - Could not determine the active round for odds calculation. Cannot create final output. ---") # DEBUG LOG
            # Return None or an empty DataFrame depending on desired behavior
            return None # Returning None as no odds can be calculated

        if active_round not in df_cleaned.columns:
             print(f"--- Debug: Error - Active round column '{active_round}' not found after cleaning. Columns: {df_cleaned.columns} ---") # DEBUG LOG
             return None

        # --- Final DataFrame Assembly ---
        print(f"--- Debug: Assembling final DataFrame using active round: '{active_round}' ---") # DEBUG LOG
        # Create final DataFrame starting with the index (Player names)
        df_final = pd.DataFrame(index=df_cleaned.index)
        df_final['Probability (%)'] = df_cleaned[active_round]

        # Calculate Decimal Odds
        if pd.api.types.is_numeric_dtype(df_final['Probability (%)']):
             # Calculate odds only for valid probabilities > 0
             valid_probs = df_final['Probability (%)'].dropna() / 100.0
             valid_probs = valid_probs[valid_probs > 0] # Filter out 0% probabilities
             if not valid_probs.empty:
                  # Apply calculation only to the valid subset
                  df_final['Decimal_Odds'] = (1 / valid_probs).round(2)
             else:
                  # If no valid probabilities > 0, fill with NaN
                  df_final['Decimal_Odds'] = np.nan
             # Fill NaN for rows that had NaN or 0 probability initially
             df_final['Decimal_Odds'] = df_final['Decimal_Odds'].fillna(np.nan)
             print(f"--- Debug: Calculated Decimal_Odds. ---") # DEBUG LOG
        else:
             print(f"--- Debug: Warning - Probability column '{active_round}' is not numeric. Cannot calculate odds. Setting Decimal_Odds to NaN. ---") # DEBUG LOG
             df_final['Decimal_Odds'] = np.nan

        # Add Round and Tournament URL columns
        df_final['Round'] = active_round
        df_final['Tournament_URL'] = url # Use the original full URL

        # Reset index to make 'Player' a column
        df_final.reset_index(inplace=True)

        # Select and order final columns
        final_cols = ['Tournament_URL', 'Round', 'Player', 'Probability (%)', 'Decimal_Odds']
        # Ensure only existing columns are selected
        existing_final_cols = [col for col in final_cols if col in df_final.columns]
        df_final = df_final[existing_final_cols]

        print(f"--- Debug: Successfully processed. Final DataFrame shape: {df_final.shape} ---") # DEBUG LOG
        return df_final

    except ValueError as ve: # Catch the specific error from find_headers_cols
        print(f"--- Debug: Error processing {url} - Failed during header finding: {ve} ---") # DEBUG LOG
        # traceback.print_exc() # Optional: print traceback for ValueError too
        return None
    except Exception as e:
        print(f"--- Debug: An unexpected error occurred during processing for {url}: {e} ---") # DEBUG LOG
        traceback.print_exc() # Print full traceback for unexpected errors
        return None


# MODIFIED get_all_sackmann_data to handle potential None returns
def get_all_sackmann_data() -> pd.DataFrame:
    """
    Scrapes and processes Sackmann data for all relevant Tennis Abstract URLs.
    Returns a consolidated DataFrame with data from all tournaments.
    """
    # --- MOVED IMPORTS HERE ---
    try:
        from tennis_abstract_scraper import tourneys_url, probas_scraper
        print("Successfully imported from tennis_abstract_scraper inside function.")
    except ImportError as e:
        print(f"Error importing from tennis_abstract_scraper inside function: {e}")
        return pd.DataFrame() # Return empty DataFrame if import fails
    # --- END MOVED IMPORTS ---

    urls = tourneys_url()
    print(f"Found {len(urls)} tournament URLs to scrape.")
    all_data_dfs = []

    if not urls:
        print("No tournament URLs found. Returning empty DataFrame.")
        return pd.DataFrame()

    for url in urls:
        print(f"\n--- Processing URL: {url} ---") # Log which URL is being processed
        try:
            # Step 1: Scrape raw data
            print("--- Debug: Calling probas_scraper... ---") # DEBUG LOG
            table_data = probas_scraper(url)
            if not table_data:
                 print(f"--- Debug: No data scraped from {url}. Skipping. ---") # DEBUG LOG
                 continue
            print(f"--- Debug: Scraped {len(table_data)} raw data points from {url}. ---") # DEBUG LOG

            # Step 2: Process scraped data
            print("--- Debug: Calling process_sackmann_table... ---") # DEBUG LOG
            processed_df = process_sackmann_table(table_data, url) # Call the main processing function

            # Step 3: Check result and append
            if processed_df is not None and not processed_df.empty:
                print(f"--- Debug: Successfully processed data for {url}. Shape: {processed_df.shape} ---") # DEBUG LOG
                all_data_dfs.append(processed_df)
            elif processed_df is None:
                 print(f"--- Debug: Processing failed for {url} (returned None). ---") # DEBUG LOG
            else: # processed_df is an empty DataFrame
                 print(f"--- Debug: Processing resulted in an empty DataFrame for {url}. ---") # DEBUG LOG

        except Exception as e:
            # Catch errors during the scrape/process loop for a single URL
            print(f"--- Debug: Critical error during scrape/process loop for {url}: {e} ---") # DEBUG LOG
            traceback.print_exc()
            print(f"--- Debug: Skipping to next URL due to error. ---") # DEBUG LOG
            continue # Continue to the next URL

    # --- Final Concatenation ---
    if not all_data_dfs:
        print("\n--- Debug: No data collected from any URL after processing. Returning empty DataFrame. ---") # DEBUG LOG
        return pd.DataFrame()

    try:
        print(f"\n--- Debug: Concatenating {len(all_data_dfs)} processed DataFrames... ---") # DEBUG LOG
        final_sackmann_data = pd.concat(all_data_dfs, ignore_index=True)
        print(f"\n--- Debug: Consolidated Sackmann data shape: {final_sackmann_data.shape} ---") # DEBUG LOG
        return final_sackmann_data
    except Exception as e:
         print(f"--- Debug: Error during final concatenation: {e} ---") # DEBUG LOG
         traceback.print_exc()
         return pd.DataFrame() # Return empty DataFrame on concatenation error


# Example usage
if __name__ == "__main__":
    print("Fetching and processing Sackmann data...")
    # Add timestamp for local testing clarity
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
        # Optional: Save locally for inspection
        # local_save_path = "debug_sackmann_data.csv"
        # print(f"\nSaving debug data locally to: {local_save_path}")
        # sackmann_data.to_csv(local_save_path, index=False)
    else:
        print("\n--- No Sackmann data was processed or collected. ---")
