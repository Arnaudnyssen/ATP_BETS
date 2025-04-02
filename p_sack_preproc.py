# p_sack_preproc.py (Import re Added)

import pandas as pd
import numpy as np
import re # <--- ADDED IMPORT STATEMENT
# Assuming tennis_abstract_scraper provides these functions
try:
    # Use the latest reviewed version of the scraper
    from tennis_abstract_scraper import tourneys_url, probas_scraper
except ImportError as e:
    print(f"Error importing from tennis_abstract_scraper: {e}")
    print("Ensure tennis_abstract_scraper.py is in the same directory or Python path.")
    import sys
    sys.exit(1)

from typing import List, Optional, Dict, Any

# --- Helper Functions ---
# (Functions find_headers_cols, table_data_to_df, clean_df,
#  get_active_round_and_columns, preprocess_player_names remain the same
#  as in p_sack_preproc_py_cleaned_01 - including the use of 're')
def find_headers_cols(table_data: List[Any]) -> List[str]:
    """
    Identifies header columns from the raw scraped table data.
    Assumes headers are strings appearing before the first player name containing '('.
    """
    headers = []
    player_found = False
    common_headers = ["Player", "Seed", "Rank", "R128", "R64", "R32", "R16", "QF", "SF", "F", "W", "Pts"]
    potential_headers = []

    for element in table_data:
        element_str = str(element).strip()
        if not element_str: continue

        if isinstance(element, str) and '(' in element_str and ')' in element_str and any(c.isalpha() for c in element_str):
            player_found = True
            break

        if not player_found:
             is_number = element_str.replace('.', '', 1).isdigit()
             is_percentage = '%' in element_str
             if not is_number and not is_percentage:
                  potential_headers.append(element_str)

    headers = [h for h in potential_headers if h in common_headers or h == "Player"]

    if not headers and potential_headers:
         print("Warning: Common headers not found, using heuristic based on first non-numeric strings.")
         headers = [h for h in potential_headers if not h.replace('.', '', 1).isdigit() and '%' not in h][:len(common_headers)]

    if "Player" in headers:
        if headers[0] != "Player":
            headers.remove("Player")
            headers.insert(0, "Player")
    elif headers:
        print("Warning: 'Player' header not explicitly found, assuming first column.")
        if '(' not in headers[0]:
             headers.insert(0, "Player")
    else:
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
    # Use the 're' module which is now imported
    header_pattern = re.compile(r"Player|R16|QF|SF|F|W") # Pattern from scraper

    row_start_heuristic = headers[0]
    temp_data_list = list(table_data)

    try:
        first_header_index = temp_data_list.index(row_start_heuristic)
        data_start_index = -1
        for i in range(first_header_index + num_headers, len(temp_data_list)):
             element_str = str(temp_data_list[i]).strip()
             if isinstance(temp_data_list[i], str) and '(' in element_str and ')' in element_str and any(c.isalpha() for c in element_str):
                  data_start_index = i
                  break
        if data_start_index == -1:
             print("Warning: Could not reliably find start of data rows. Attempting from beginning.")
             data_start_index = 0
        else:
             print(f"Data rows likely start around index {data_start_index}")
        relevant_data = temp_data_list[data_start_index:]
    except ValueError:
         print(f"Warning: Could not find first header '{row_start_heuristic}' in data. Processing full list.")
         relevant_data = temp_data_list

    row_buffer = []
    for item in relevant_data:
        item_str = str(item).strip()
        if header_pattern.match(item_str) and len(row_buffer) == 0:
             print(f"Skipping likely header item: {item_str}")
             continue

        row_buffer.append(item)

        if len(row_buffer) == num_headers:
            first_cell_text = str(row_buffer[0]).strip()
            if header_pattern.match(first_cell_text):
                 print(f"Skipping likely full header row: {row_buffer}")
            else:
                 data_rows.append(row_buffer)
            row_buffer = []

    if row_buffer:
        print(f"Warning: Trailing data found ({len(row_buffer)} items), potentially incomplete row. Discarding: {row_buffer}")

    if not data_rows:
         print(f"Error: No valid data rows extracted for URL {url}.")
         return None

    try:
        df = pd.DataFrame(data_rows, columns=headers)
        if "Player" not in df.columns:
             print(f"Error: 'Player' column missing after DataFrame creation for URL {url}.")
             if df.shape[1] > 0:
                  print("Attempting to rename first column to 'Player'.")
                  df.rename(columns={df.columns[0]: "Player"}, inplace=True)
             else: return None

        if not df['Player'].astype(str).str.contains(r'\(', regex=True).any():
             print("Warning: Player names might not be in the expected 'Name (COUNTRY/SEED)' format.")

        df.set_index("Player", inplace=True)
        df['Tournament_URL'] = url
        return df
    except Exception as e:
        print(f"Error creating DataFrame for URL {url}: {e}")
        return None


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the DataFrame by removing irrelevant rows/columns and handling NaNs."""
    print("Cleaning DataFrame...")
    potential_numeric_cols = [col for col in df.columns if col not in ['Tournament_URL']]
    for col in potential_numeric_cols:
         if df[col].astype(str).str.contains('%').any():
              df[col] = df[col].astype(str).str.replace('%', '', regex=False)
         df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
    print("Cleaning complete.")
    return df

def get_active_round_and_columns(df: pd.DataFrame) -> Optional[str]:
    """
    Identifies the current active round based on probabilities not being 0 or 100.
    Returns the column name of the active round, or None if no active round found.
    Assumes numeric conversion and cleaning have happened.
    """
    round_col_names = ["R128", "R64", "R32", "R16", "QF", "SF", "F", "W"]
    potential_round_cols = [col for col in df.columns if col in round_col_names]
    ordered_round_cols = [col for col in round_col_names if col in potential_round_cols]

    if not ordered_round_cols:
        print("Warning: Could not identify potential round columns based on typical names (R128, R64...).")
        return None

    print(f"Potential round columns identified and ordered: {ordered_round_cols}")

    for round_col in ordered_round_cols:
        if round_col not in df.columns: continue

        if pd.api.types.is_numeric_dtype(df[round_col]):
            percentages = df[round_col]
            is_active = (percentages > 0.01) & (percentages < 99.99)
            if is_active.any():
                print(f"Active round identified: {round_col}")
                return round_col
            else:
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

    target_column = None
    if df.index.name == 'Player':
        target_column = df.index
        is_index = True
    elif 'Player' in df.columns:
        target_column = df['Player']
        is_index = False
    else:
         print("Warning: Neither 'Player' index nor 'Player' column found for preprocessing.")
         return df

    original_target = target_column.copy()
    try:
        # Ensure working with string type
        processed_target = target_column.astype(str)
        # Apply replacements using 're' module which is now imported
        processed_target = processed_target.str.replace(r'\s*\(\d+\)', '', regex=True) # Seed
        processed_target = processed_target.str.replace(r'\s*\([A-Z]{3}\)', '', regex=True) # Country
        processed_target = processed_target.str.replace(r'\s*\(WC\)', '', regex=True) # Wildcard
        processed_target = processed_target.str.replace(r'\s*\(Q\)', '', regex=True) # Qualifier
        processed_target = processed_target.str.replace(r'\s*\(LL\)', '', regex=True) # Lucky Loser
        processed_target = processed_target.str.replace(r'\s*\(SE\)', '', regex=True) # Special Exempt
        processed_target = processed_target.str.replace(r'\s*\(PR\)', '', regex=True) # Protected Ranking
        processed_target = processed_target.str.replace(r'^\*|\*$', '', regex=True) # Asterisks
        processed_target = processed_target.str.strip().str.lower()

        if is_index:
            df.index = processed_target
        else:
            df['Player'] = processed_target

    except AttributeError as e:
        print(f"Could not preprocess player names, likely not string type or regex error: {e}")
        # Restore original if processing fails
        if is_index:
            df.index = original_target
        else:
            df['Player'] = original_target

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

        # Use the 're' module which is now imported in table_data_to_df
        df_raw = table_data_to_df(headers, table_data, url)
        if df_raw is None:
            print("Failed to create DataFrame.")
            return None
        print(f"Raw DataFrame created with shape: {df_raw.shape}")

        df_processed = preprocess_player_names(df_raw.copy())
        df_cleaned = clean_df(df_processed)
        print(f"Cleaned DataFrame shape: {df_cleaned.shape}")
        if df_cleaned.empty:
            print("DataFrame is empty after cleaning.")
            return None

        active_round = get_active_round_and_columns(df_cleaned)
        if active_round is None:
            print("Could not determine the active round for odds calculation.")
            return None

        if active_round not in df_cleaned.columns:
             print(f"Error: Active round column '{active_round}' not found after cleaning.")
             return None

        df_final = pd.DataFrame(index=df_cleaned.index)
        df_final['Probability (%)'] = df_cleaned[active_round]

        if pd.api.types.is_numeric_dtype(df_final['Probability (%)']):
             valid_probs = df_final['Probability (%)'].dropna() / 100.0
             valid_probs = valid_probs[valid_probs > 0]
             if not valid_probs.empty:
                  df_final['Decimal_Odds'] = (1 / valid_probs).round(2)
             else:
                  df_final['Decimal_Odds'] = np.nan
        else:
             print(f"Warning: Probability column '{active_round}' is not numeric. Cannot calculate odds.")
             df_final['Decimal_Odds'] = np.nan

        df_final['Round'] = active_round
        df_final['Tournament_URL'] = url

        df_final.reset_index(inplace=True)

        final_cols = ['Tournament_URL', 'Round', 'Player', 'Probability (%)', 'Decimal_Odds']
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

    for url in urls:
        try:
            table_data = probas_scraper(url)
            if not table_data:
                 print(f"No data scraped from {url}.")
                 continue

            processed_df = process_sackmann_table(table_data, url)

            if processed_df is not None and not processed_df.empty:
                all_data_dfs.append(processed_df)
            else:
                 print(f"No valid data processed for {url}.")

        except Exception as e:
            print(f"Failed to scrape or process {url}: {e}")
            import traceback
            traceback.print_exc()
            continue

    if not all_data_dfs:
        print("No data collected from any URL.")
        return pd.DataFrame()

    try:
        final_sackmann_data = pd.concat(all_data_dfs, ignore_index=True)
        print(f"\nConsolidated Sackmann data shape: {final_sackmann_data.shape}")
        return final_sackmann_data
    except Exception as e:
         print(f"Error during final concatenation: {e}")
         return pd.DataFrame()


# Example usage
if __name__ == "__main__":
    # import re # No longer needed here as it's imported at top level
    print("Fetching and processing Sackmann data...")
    sackmann_data = get_all_sackmann_data()

    if not sackmann_data.empty:
        print("\n--- Sample of Processed Sackmann Data ---")
        print(sackmann_data.head())
        print("\n--- Data Info ---")
        sackmann_data.info()
    else:
        print("No Sackmann data was processed.")

