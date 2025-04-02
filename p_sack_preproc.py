# p_sack_preproc.py (Cleaned - No Markdown Formatting)

import pandas as pd
import numpy as np
# Assuming tennis_abstract_scraper provides these functions
try:
    # Use the latest reviewed version of the scraper
    from tennis_abstract_scraper import tourneys_url, probas_scraper
except ImportError as e:
    print(f"Error importing from tennis_abstract_scraper: {e}")
    print("Ensure tennis_abstract_scraper.py is in the same directory or Python path.")
    # Depending on execution context, might need sys.path manipulation if files are structured differently
    import sys
    sys.exit(1) # Exit if essential import fails

from typing import List, Optional, Dict, Any # Added typing

# --- Helper Functions (Minor changes/Additions) ---

def find_headers_cols(table_data: List[Any]) -> List[str]:
    """
    Identifies header columns from the raw scraped table data.
    Assumes headers are strings appearing before the first player name containing '('.
    """
    headers = []
    player_found = False
    # Increased robustness: Check for common header keywords explicitly first
    common_headers = ["Player", "Seed", "Rank", "R128", "R64", "R32", "R16", "QF", "SF", "F", "W", "Pts"]
    potential_headers = []

    # First pass: Collect potential headers before first likely player row
    for element in table_data:
        element_str = str(element).strip() # Work with string representation
        if not element_str: continue # Skip empty

        # Check for player pattern (e.g., Name (USA) or Name (1))
        if isinstance(element, str) and '(' in element_str and ')' in element_str and any(c.isalpha() for c in element_str):
            player_found = True
            break # Stop collecting headers once a player is likely found

        # Collect potential headers if no player found yet
        if not player_found:
             # Avoid adding pure numbers or percentages as headers initially
             is_number = element_str.replace('.', '', 1).isdigit()
             is_percentage = '%' in element_str
             if not is_number and not is_percentage:
                  potential_headers.append(element_str)

    # Second pass: Filter potential headers against common ones and refine
    headers = [h for h in potential_headers if h in common_headers or h == "Player"] # Prioritize known headers

    # If common headers aren't found, use a simpler heuristic (less reliable)
    if not headers and potential_headers:
         print("Warning: Common headers not found, using heuristic based on first non-numeric strings.")
         headers = [h for h in potential_headers if not h.replace('.', '', 1).isdigit() and '%' not in h][:len(common_headers)] # Limit length

    # Ensure 'Player' is the first column if present
    if "Player" in headers:
        if headers[0] != "Player":
            headers.remove("Player")
            headers.insert(0, "Player")
    elif headers: # If headers exist but 'Player' is missing, assume first column is Player
        print("Warning: 'Player' header not explicitly found, assuming first column.")
        # Check if first potential header IS player-like, otherwise insert 'Player'
        if '(' not in headers[0]: # Simple check if first header doesn't look like a player name
             headers.insert(0, "Player")
    else:
        # Cannot proceed without any headers
        raise ValueError("Could not determine headers for the table data.")

    print(f"Identified headers: {headers}")
    return headers


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

    data_rows = []
    current_row = []
    header_pattern = re.compile(r"Player|R16|QF|SF|F|W") # Pattern from scraper

    # Iterate through the flat list, grouping into rows based on num_headers, skipping headers
    row_start_heuristic = headers[0] # Use first header as a marker

    temp_data_list = list(table_data) # Work on a copy

    # Try to find the start of the actual data rows after the first header instance
    try:
        first_header_index = temp_data_list.index(row_start_heuristic)
        # Look for the first player-like entry after the first header row ends
        data_start_index = -1
        for i in range(first_header_index + num_headers, len(temp_data_list)):
             element_str = str(temp_data_list[i]).strip()
             # Check for player pattern (e.g., Name (USA) or Name (1))
             if isinstance(temp_data_list[i], str) and '(' in element_str and ')' in element_str and any(c.isalpha() for c in element_str):
                  data_start_index = i
                  break
        if data_start_index == -1:
             print("Warning: Could not reliably find start of data rows. Attempting from beginning.")
             data_start_index = 0 # Fallback
        else:
             print(f"Data rows likely start around index {data_start_index}")

        relevant_data = temp_data_list[data_start_index:]

    except ValueError:
         print(f"Warning: Could not find first header '{row_start_heuristic}' in data. Processing full list.")
         relevant_data = temp_data_list # Process everything if header isn't found


    row_buffer = []
    for item in relevant_data:
        item_str = str(item).strip()
        # Skip items that look like part of a repeated header row
        if header_pattern.match(item_str) and len(row_buffer) == 0: # If it looks like a header and we are at start of a potential row
             print(f"Skipping likely header item: {item_str}")
             # Potentially skip next num_headers-1 items too if a full header row is detected
             continue # Skip this item

        row_buffer.append(item)

        if len(row_buffer) == num_headers:
            # Check if this completed row looks like a header row
            first_cell_text = str(row_buffer[0]).strip()
            if header_pattern.match(first_cell_text):
                 print(f"Skipping likely full header row: {row_buffer}")
            else:
                 data_rows.append(row_buffer)
            row_buffer = [] # Reset buffer

    # Handle any remaining items in the buffer (if data length wasn't multiple of num_headers)
    if row_buffer:
        print(f"Warning: Trailing data found ({len(row_buffer)} items), potentially incomplete row. Discarding: {row_buffer}")


    if not data_rows:
         print(f"Error: No valid data rows extracted for URL {url}.")
         return None

    try:
        df = pd.DataFrame(data_rows, columns=headers)
        if "Player" not in df.columns:
             print(f"Error: 'Player' column missing after DataFrame creation for URL {url}.")
             # Attempt to assign first column as Player if logic failed
             if df.shape[1] > 0:
                  print("Attempting to rename first column to 'Player'.")
                  df.rename(columns={df.columns[0]: "Player"}, inplace=True)
             else:
                  return None # Cannot proceed

        # Basic check for player name format before setting index
        if not df['Player'].astype(str).str.contains(r'\(', regex=True).any():
             print("Warning: Player names might not be in the expected 'Name (COUNTRY/SEED)' format.")

        df.set_index("Player", inplace=True)
        df['Tournament_URL'] = url # Add the source URL
        return df
    except Exception as e:
        print(f"Error creating DataFrame for URL {url}: {e}")
        return None


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the DataFrame by removing irrelevant rows/columns and handling NaNs."""
    print("Cleaning DataFrame...")
    # Remove rows that seem like repeated headers (e.g., containing 'W' or 'Player' in data columns)
    # This check is less reliable now that header rows are skipped earlier
    # if 'W' in df.columns:
    #     df = df[df["W"] != "W"]

    # Convert relevant columns to numeric, coercing errors
    potential_numeric_cols = [col for col in df.columns if col not in ['Tournament_URL']] # Exclude URL
    for col in potential_numeric_cols:
         # Try converting percentage strings first
         if df[col].astype(str).str.contains('%').any():
              df[col] = df[col].astype(str).str.replace('%', '', regex=False)
         # Attempt numeric conversion
         df[col] = pd.to_numeric(df[col], errors='coerce')


    # Replace specific non-numeric markers if needed (original code had replace for "" and "-")
    # df.replace(["", "-"], np.nan, inplace=True) # This might be redundant after coerce

    # Drop rows/columns that are entirely NaN (after numeric conversion)
    df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
    print("Cleaning complete.")
    return df

def get_active_round_and_columns(df: pd.DataFrame) -> Optional[str]:
    """
    Identifies the current active round based on probabilities not being 0 or 100.
    Returns the column name of the active round, or None if no active round found.
    Assumes numeric conversion and cleaning have happened.
    """
    # Potential round columns are typically between 'Seed'/'Rank' and 'W'/'Pts', and contain numbers/percentages
    # Let's identify them based on typical names
    round_col_names = ["R128", "R64", "R32", "R16", "QF", "SF", "F", "W"] # Ordered typical rounds
    potential_round_cols = [col for col in df.columns if col in round_col_names]

    # Reorder found columns according to typical progression
    ordered_round_cols = [col for col in round_col_names if col in potential_round_cols]


    if not ordered_round_cols:
        print("Warning: Could not identify potential round columns based on typical names (R128, R64...).")
        # Fallback: Consider any numeric column except Seed/Rank/Pts as potential round
        # This is less reliable
        # excluded_cols = ['Seed', 'Rank', 'Pts', 'Tournament_URL']
        # potential_round_cols = [col for col in df.select_dtypes(include=np.number).columns if col not in excluded_cols]
        # ordered_round_cols = potential_round_cols # No guaranteed order here
        return None # Safer to return None if standard round names aren't found

    print(f"Potential round columns identified and ordered: {ordered_round_cols}")

    # Check rounds from left to right (assuming earlier rounds finish first)
    for round_col in ordered_round_cols:
        if round_col not in df.columns: continue # Skip if column was dropped during cleaning

        # Check if column contains values strictly between 0 and 100
        # Ensure column is numeric first
        if pd.api.types.is_numeric_dtype(df[round_col]):
            percentages = df[round_col]
            # Check if any percentage is strictly between 0 and 100 (floating point safe)
            is_active = (percentages > 0.01) & (percentages < 99.99)
            if is_active.any():
                print(f"Active round identified: {round_col}")
                return round_col
            else:
                # Check if round appears finished (only 0, 100, or NaN)
                 finished_check = percentages.isin([0, 100]) | percentages.isna()
                 if finished_check.all():
                     print(f"Round '{round_col}' appears finished or empty.")
                 else:
                     print(f"Warning: Round '{round_col}' has unexpected numeric values: {percentages[~finished_check].unique()}")
        else:
             print(f"Warning: Column '{round_col}' is not numeric, cannot check for active status.")


    print("No active round found (all rounds might be finished or data is missing/unparseable).")
    return None


def preprocess_player_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes player names in the DataFrame index or 'Player' column."""
    print("Preprocessing player names...")
    if isinstance(df.index, pd.MultiIndex):
         print("Warning: MultiIndex found, cannot preprocess player names in index.")
         return df

    if df.index.name == 'Player':
        # Preprocess index
        original_index = df.index
        try:
             df.index = df.index.str.replace(r'\s*\(\d+\)', '', regex=True) # Remove seed (e.g., "(1)")
             df.index = df.index.str.replace(r'\s*\([A-Z]{3}\)', '', regex=True) # Remove country code (e.g., "(USA)")
             df.index = df.index.str.replace(r'\s*\(WC\)', '', regex=True) # Remove wildcard (WC)
             df.index = df.index.str.replace(r'\s*\(Q\)', '', regex=True) # Remove qualifier (Q)
             df.index = df.index.str.replace(r'\s*\(LL\)', '', regex=True) # Remove lucky loser (LL)
             df.index = df.index.str.replace(r'\s*\(SE\)', '', regex=True) # Remove special exempt (SE)
             df.index = df.index.str.replace(r'\s*\(PR\)', '', regex=True) # Remove protected ranking (PR)
             df.index = df.index.str.replace(r'^\*|\*$', '', regex=True) # Remove leading/trailing asterisk if present
             df.index = df.index.str.strip().str.lower()
        except AttributeError as e:
             print(f"Could not preprocess index names, likely not string type: {e}")
             df.index = original_index # Restore original index if processing fails
    elif 'Player' in df.columns:
         # Preprocess 'Player' column
         original_column = df['Player'].copy()
         try:
             df['Player'] = df['Player'].str.replace(r'\s*\(\d+\)', '', regex=True)
             df['Player'] = df['Player'].str.replace(r'\s*\([A-Z]{3}\)', '', regex=True)
             df['Player'] = df['Player'].str.replace(r'\s*\(WC\)', '', regex=True)
             df['Player'] = df['Player'].str.replace(r'\s*\(Q\)', '', regex=True)
             df['Player'] = df['Player'].str.replace(r'\s*\(LL\)', '', regex=True)
             df['Player'] = df['Player'].str.replace(r'\s*\(SE\)', '', regex=True)
             df['Player'] = df['Player'].str.replace(r'\s*\(PR\)', '', regex=True)
             df['Player'] = df['Player'].str.replace(r'^\*|\*$', '', regex=True)
             df['Player'] = df['Player'].str.strip().str.lower()
         except AttributeError as e:
              print(f"Could not preprocess 'Player' column, likely not string type: {e}")
              df['Player'] = original_column # Restore original column if processing fails
    else:
         print("Warning: Neither 'Player' index nor 'Player' column found for preprocessing.")

    print("Player name preprocessing complete.")
    return df

# --- Main Processing Logic ---

def process_sackmann_table(table_data: List[Any], url: str) -> Optional[pd.DataFrame]:
    """
    Processes scraped data for a single Tennis Abstract tournament URL.
    Returns a DataFrame with Player, Round, Probability (%), and Decimal Odds,
    or None if processing fails.
    """
    print(f"\nProcessing URL: {url}")
    try:
        headers = find_headers_cols(table_data)
        if not headers:
             print("Error: Failed to find headers.")
             return None

        df_raw = table_data_to_df(headers, table_data, url)
        if df_raw is None:
            print("Failed to create DataFrame.")
            return None
        print(f"Raw DataFrame created with shape: {df_raw.shape}")

        # Preprocess player names before cleaning (might help with identifying rows)
        df_processed = preprocess_player_names(df_raw.copy()) # Work on a copy

        df_cleaned = clean_df(df_processed) # Clean after preprocessing names and converting types
        print(f"Cleaned DataFrame shape: {df_cleaned.shape}")
        if df_cleaned.empty:
            print("DataFrame is empty after cleaning.")
            return None

        active_round = get_active_round_and_columns(df_cleaned)
        if active_round is None:
            print("Could not determine the active round for odds calculation.")
            # Option: Return data for the last available round if no active one?
            # For now, return None if no active round identified.
            return None

        # Prepare the final DataFrame structure
        # Ensure the active_round column exists after cleaning
        if active_round not in df_cleaned.columns:
             print(f"Error: Active round column '{active_round}' not found after cleaning.")
             return None

        df_final = pd.DataFrame(index=df_cleaned.index) # Keep player index
        df_final['Probability (%)'] = df_cleaned[active_round]

        # Calculate Decimal Odds
        # Ensure probability column is numeric
        if pd.api.types.is_numeric_dtype(df_final['Probability (%)']):
             valid_probs = df_final['Probability (%)'].dropna() / 100.0
             # Handle potential zero probabilities explicitly before division
             valid_probs = valid_probs[valid_probs > 0]
             if not valid_probs.empty:
                  df_final['Decimal_Odds'] = (1 / valid_probs).round(2)
             else:
                  df_final['Decimal_Odds'] = np.nan
        else:
             print(f"Warning: Probability column '{active_round}' is not numeric. Cannot calculate odds.")
             df_final['Decimal_Odds'] = np.nan


        df_final['Round'] = active_round
        df_final['Tournament_URL'] = url # Add URL back if needed, or get from df_cleaned

        # Reset index to make 'Player' a column
        df_final.reset_index(inplace=True)

        # Reorder columns for clarity
        final_cols = ['Tournament_URL', 'Round', 'Player', 'Probability (%)', 'Decimal_Odds']
        # Ensure all desired columns exist before reordering
        existing_final_cols = [col for col in final_cols if col in df_final.columns]
        df_final = df_final[existing_final_cols]

        print(f"Successfully processed. Final DataFrame shape: {df_final.shape}")
        return df_final

    except Exception as e:
        print(f"An unexpected error occurred during processing for {url}: {e}")
        import traceback
        traceback.print_exc()
        return None


def get_all_sackmann_data() -> pd.DataFrame:
    """
    Scrapes and processes Sackmann data for all relevant Tennis Abstract URLs.
    Returns a consolidated DataFrame with data from all tournaments.
    """
    urls = tourneys_url()
    print(f"Found {len(urls)} tournament URLs to scrape.")
    all_data_dfs = []

    # Limit number of URLs for testing if needed
    # urls = urls[:2]
    # print(f"Processing first {len(urls)} URLs for testing...")

    for url in urls:
        try:
            # Scrape data for the current URL
            table_data = probas_scraper(url)
            if not table_data:
                 print(f"No data scraped from {url}.")
                 continue

            # Process the scraped data
            processed_df = process_sackmann_table(table_data, url)

            if processed_df is not None and not processed_df.empty:
                all_data_dfs.append(processed_df)
            else:
                 print(f"No valid data processed for {url}.")

        except Exception as e:
            print(f"Failed to scrape or process {url}: {e}")
            import traceback
            traceback.print_exc() # Print full traceback for failures
            continue # Continue with the next URL

    if not all_data_dfs:
        print("No data collected from any URL.")
        return pd.DataFrame() # Return empty DataFrame

    # Concatenate all individual DataFrames
    try:
        final_sackmann_data = pd.concat(all_data_dfs, ignore_index=True)
        print(f"\nConsolidated Sackmann data shape: {final_sackmann_data.shape}")
        return final_sackmann_data
    except Exception as e:
         print(f"Error during final concatenation: {e}")
         return pd.DataFrame()


# Example usage (optional, can be run directly for testing)
if __name__ == "__main__":
    import re # Import regex here if needed for helper functions
    print("Fetching and processing Sackmann data...")
    sackmann_data = get_all_sackmann_data()

    if not sackmann_data.empty:
        print("\n--- Sample of Processed Sackmann Data ---")
        print(sackmann_data.head())
        print("\n--- Data Info ---")
        sackmann_data.info()
        # Example: Save to CSV directly from here if needed for testing
        # try:
        #     output_filename = "sackmann_data_test_direct.csv"
        #     sackmann_data.to_csv(output_filename, index=False)
        #     print(f"\nTest data saved to {output_filename}")
        # except Exception as e:
        #     print(f"\nError saving test data to CSV: {e}")
    else:
        print("No Sackmann data was processed.")
