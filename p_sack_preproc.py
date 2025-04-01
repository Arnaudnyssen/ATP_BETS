# p_sack_preproc.py (Modified)

import pandas as pd
import numpy as np
# Assuming tennis_abstract_scraper provides these functions
from tennis_abstract_scraper import tourneys_url, probas_scraper
from typing import List, Optional, Dict, Any # Added typing

# --- Helper Functions (Minor changes/Additions) ---

def find_headers_cols(table_data: List[Any]) -> List[str]:
    """
    Identifies header columns from the raw scraped table data.
    Assumes headers are strings appearing before the first player name containing '('.
    """
    headers = []
    player_found = False
    for element in table_data:
        if isinstance(element, str):
            # Simple check for player names often having ranking/seed in parentheses
            if "(" in element and ")" in element:
                player_found = True
            # Collect potential headers before the first player is found
            if not player_found and element: # Ensure non-empty strings
                 # Basic check to avoid adding stray numbers/percentages as headers
                if not '%' in element and not element.isdigit():
                    headers.append(element)
        # Stop adding headers once a player entry is likely found
        elif player_found:
            break # Optimization: stop once headers are likely done
    # Filter out potential empty strings or non-header items collected early
    headers = [h for h in headers if h and not h.isdigit() and '%' not in h]
    # Ensure 'Player' is always the first column if found
    if "Player" in headers and headers[0] != "Player":
        headers.remove("Player")
        headers.insert(0, "Player")
    elif "Player" not in headers:
        # Handle case where 'Player' header might be missing or formatted differently
        print("Warning: 'Player' header not explicitly found using '(' pattern.")
        # Attempt to infer based on common table structures (may need adjustment)
        if headers:
            headers.insert(0, "Player") # Assume first column is Player if others exist
        else:
            # Cannot proceed without headers
             raise ValueError("Could not determine headers for the table data.")

    return headers


def table_data_to_df(headers: List[str], table_data: List[Any], url: str) -> Optional[pd.DataFrame]:
    """
    Converts the raw list of scraped data into a pandas DataFrame using the identified headers.
    Adds the source URL as a column. Returns None if DataFrame creation fails.
    """
    if not headers:
        print(f"Error: No headers provided for URL {url}. Cannot create DataFrame.")
        return None

    num_headers = len(headers)
    if num_headers == 0:
         print(f"Error: Zero headers identified for URL {url}. Cannot create DataFrame.")
         return None

    # Estimate starting point of actual data after headers
    # This assumes headers appear contiguously at the start
    data_start_index = -1
    temp_headers_check = list(headers) # Copy headers to check against
    for i, element in enumerate(table_data):
         if isinstance(element, str) and element in temp_headers_check:
             temp_headers_check.remove(element)
             if not temp_headers_check: # Found all headers
                 # Data likely starts after this point, look for first player name pattern
                 for j in range(i + 1, len(table_data)):
                     if isinstance(table_data[j], str) and "(" in table_data[j]:
                         data_start_index = j
                         break
                 if data_start_index != -1:
                     break
         # Fallback if exact header match fails, look for first player name
         elif isinstance(element, str) and "(" in element and data_start_index == -1:
             data_start_index = i
             break

    if data_start_index == -1:
        print(f"Error: Could not reliably determine the start of table data for URL {url}.")
        # Fallback: Assume data starts immediately after the number of headers found
        # This is less reliable
        data_start_index = num_headers
        print(f"Warning: Assuming data starts at index {data_start_index} for URL {url}.")
        #return None # Or try a less reliable approach


    relevant_data = table_data[data_start_index:]

    # Basic validation: Check if data length is a multiple of header count
    if len(relevant_data) % num_headers != 0:
        print(f"Warning: Data length ({len(relevant_data)}) is not a multiple of header count ({num_headers}) for URL {url}. DataFrame might be misaligned.")
        # Attempt to trim or pad - this is risky and might indicate scraping errors
        # For now, we'll proceed but flag the potential issue. A better approach
        # might involve more sophisticated parsing or erroring out.

    data_dict: Dict[str, List] = {header: [] for header in headers}
    current_col_index = 0
    for item in relevant_data:
        header_name = headers[current_col_index % num_headers]
        data_dict[header_name].append(item)
        current_col_index += 1

    # Ensure all lists have the same length (pad if necessary due to earlier warning)
    max_len = 0
    if data_dict:
        max_len = max(len(lst) for lst in data_dict.values())

    for header in data_dict:
        while len(data_dict[header]) < max_len:
            data_dict[header].append(np.nan) # Pad with NaN

    try:
        df = pd.DataFrame(data_dict)
        if "Player" not in df.columns:
             print(f"Error: 'Player' column missing after DataFrame creation for URL {url}.")
             return None
        df.set_index("Player", inplace=True)
        df['Tournament_URL'] = url # Add the source URL
        return df
    except Exception as e: # Catch specific pandas errors if possible
        print(f"Error creating DataFrame for URL {url}: {e}")
        return None


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the DataFrame by removing irrelevant rows/columns and handling NaNs."""
    # Remove rows that seem like repeated headers (e.g., containing 'W')
    if 'W' in df.columns:
        df = df[df["W"] != "W"]
    df.replace(["", "-"], np.nan, inplace=True) # Replace empty strings and hyphens with NaN
    # Drop rows/columns that are entirely NaN
    df = df.dropna(how="all", axis=0).dropna(how="all", axis=1)
    return df

def get_active_round_and_columns(df: pd.DataFrame) -> Optional[str]:
    """
    Identifies the current active round based on probabilities not being 0 or 100.
    Returns the column name of the active round, or None if no active round found.
    """
    # Potential round columns are typically between 'Seed'/'Rank' and 'W'/'Pts', and contain numbers/percentages
    potential_round_cols = []
    column_names = df.columns.tolist()

    # Try to identify typical columns bordering the rounds
    start_col = 'Seed' if 'Seed' in column_names else ('Rank' if 'Rank' in column_names else None)
    end_col = 'W' if 'W' in column_names else ('Pts' if 'Pts' in column_names else None)

    start_index = column_names.index(start_col) + 1 if start_col else 0
    end_index = column_names.index(end_col) if end_col else len(column_names)

    # Filter columns in the potential range that likely contain percentages
    for col in column_names[start_index:end_index]:
        # Check if column contains numeric data (or NaN) after potential cleaning
        try:
            numeric_col = pd.to_numeric(df[col].astype(str).str.replace('%', '', regex=False), errors='coerce')
            if not numeric_col.isna().all(): # If at least one value is numeric
                 potential_round_cols.append(col)
        except Exception:
            continue # Ignore columns that cause errors during conversion

    if not potential_round_cols:
        print("Warning: Could not identify potential round columns.")
        return None

    print(f"Potential round columns identified: {potential_round_cols}")

    # Check rounds from left to right (assuming earlier rounds finish first)
    for round_col in potential_round_cols:
        is_active = False
        try:
            # Convert column to numeric, coercing errors (like player names) to NaN
            percentages = pd.to_numeric(df[round_col], errors='coerce')
            # Check if any percentage is strictly between 0 and 100
            if percentages.between(0.01, 99.99, inclusive='neither').any():
                 is_active = True

            if is_active:
                print(f"Active round identified: {round_col}")
                return round_col
            else:
                # Check if round appears finished (only 0, 100, or NaN)
                 finished_check = percentages.isin([0, 100]) | percentages.isna()
                 if finished_check.all():
                     print(f"Round '{round_col}' appears finished or empty.")
                 else:
                     # This case might indicate unexpected values
                     print(f"Warning: Round '{round_col}' has values other than 0, 100, NaN, or active percentages.")

        except Exception as e:
            print(f"Error processing column '{round_col}' for active status: {e}")
            continue # Skip problematic columns

    print("No active round found (all rounds might be finished or data is missing/unparseable).")
    return None


def preprocess_player_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes player names in the DataFrame index."""
    df.index = df.index.str.replace(r"\(.*?\)", "", regex=True).str.strip().str.lower()
     # Optional: Add more specific cleaning if needed (e.g., handling accents, initials)
    # df.index = df.index.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
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
        print(f"Headers found: {headers}")
        if not headers or "Player" not in headers:
             print("Error: Essential headers ('Player') not found.")
             return None

        df_raw = table_data_to_df(headers, table_data, url)
        if df_raw is None:
            print("Failed to create DataFrame.")
            return None
        print(f"Raw DataFrame created with shape: {df_raw.shape}")

        df_cleaned = clean_df(df_raw.copy()) # Work on a copy
        print(f"Cleaned DataFrame shape: {df_cleaned.shape}")
        if df_cleaned.empty:
            print("DataFrame is empty after cleaning.")
            return None

        active_round = get_active_round_and_columns(df_cleaned)
        if active_round is None:
            print("Could not determine the active round for odds calculation.")
            return None # Or decide to return probabilities for all rounds?

        # Prepare the final DataFrame
        df_final = df_cleaned[[active_round]].copy() # Select only the active round column
        df_final.rename(columns={active_round: 'Probability (%)'}, inplace=True)

        # Convert probabilities to numeric, coercing errors
        df_final['Probability (%)'] = pd.to_numeric(df_final['Probability (%)'], errors='coerce')

        # Calculate Decimal Odds
        # Avoid division by zero or using NaN/invalid values
        valid_probs = df_final['Probability (%)'].dropna() / 100.0
        valid_probs = valid_probs[valid_probs > 0] # Ensure probability > 0 for odds calculation
        if not valid_probs.empty:
             df_final['Decimal_Odds'] = (1 / valid_probs).round(2)
        else:
             df_final['Decimal_Odds'] = np.nan # Assign NaN if no valid probabilities

        # Add round information and preprocess names
        df_final['Round'] = active_round
        df_final = preprocess_player_names(df_final)
        df_final.reset_index(inplace=True) # Move 'Player' from index to column

        # Add back the Tournament URL
        df_final['Tournament_URL'] = url

        # Reorder columns for clarity
        df_final = df_final[['Tournament_URL', 'Round', 'Player', 'Probability (%)', 'Decimal_Odds']]

        print(f"Successfully processed. Final DataFrame shape: {df_final.shape}")
        return df_final

    except Exception as e:
        print(f"An unexpected error occurred during processing for {url}: {e}")
        import traceback
        traceback.print_exc() # Print detailed traceback for debugging
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
            # It's more efficient to initialize the driver once if possible,
            # but probas_scraper currently handles its own driver lifecycle.
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
            continue # Continue with the next URL

    if not all_data_dfs:
        print("No data collected from any URL.")
        return pd.DataFrame() # Return empty DataFrame

    # Concatenate all individual DataFrames
    final_sackmann_data = pd.concat(all_data_dfs, ignore_index=True)
    print(f"\nConsolidated Sackmann data shape: {final_sackmann_data.shape}")
    return final_sackmann_data


# Example usage (optional, can be run directly)
if __name__ == "__main__":
    print("Fetching and processing Sackmann data...")
    sackmann_data = get_all_sackmann_data()

    if not sackmann_data.empty:
        print("\n--- Sample of Processed Sackmann Data ---")
        print(sackmann_data.head())
        print("\n--- Data Info ---")
        sackmann_data.info()
        # Example: Save to CSV directly from here if needed for testing
        # try:
        #     output_filename = "sackmann_data_test.csv"
        #     sackmann_data.to_csv(output_filename, index=False)
        #     print(f"\nTest data saved to {output_filename}")
        # except Exception as e:
        #     print(f"\nError saving test data to CSV: {e}")
    else:
        print("No Sackmann data was processed.")

```python
# save_sackmann_data.py (New File)

import pandas as pd
from datetime import datetime
import os
import sys

# Ensure the main project directory is in the Python path
# Adjust the path ('..') if this script is placed in a subdirectory
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_dir not in sys.path:
    sys.path.append(project_dir)

# Import the function from the modified p_sack_preproc module
try:
    from p_sack_preproc import get_all_sackmann_data
except ImportError as e:
    print(f"Error importing 'get_all_sackmann_data': {e}")
    print("Ensure 'p_sack_preproc.py' is accessible and the project structure is correct.")
    sys.exit(1) # Exit if import fails

def save_data_to_csv(data: pd.DataFrame, base_filename: str = "sackmann_data", output_dir: str = "data_output") -> None:
    """
    Saves the provided DataFrame to a CSV file in the specified directory.
    The filename includes the current date. Creates the output directory if it doesn't exist.

    Args:
        data (pd.DataFrame): The DataFrame to save.
        base_filename (str): The base name for the output CSV file.
        output_dir (str): The directory where the CSV file will be saved.
    """
    if data.empty:
        print("No data to save.")
        return

    # Create the output directory if it doesn't exist
    try:
        os.makedirs(output_dir, exist_ok=True)
        print(f"Output directory '{output_dir}' ensured.")
    except OSError as e:
        print(f"Error creating output directory '{output_dir}': {e}")
        return # Cannot proceed without output directory

    # Generate filename with timestamp
    today_date = datetime.now().strftime("%Y%m%d") # Format: YYYYMMDD
    output_filename = f"{base_filename}_{today_date}.csv"
    output_path = os.path.join(output_dir, output_filename)

    # Save the DataFrame to CSV
    try:
        data.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Successfully saved Sackmann data to: {output_path}")
    except Exception as e:
        print(f"Error saving data to CSV file '{output_path}': {e}")

def main():
    """
    Main function to fetch Sackmann data and save it to a CSV file.
    """
    print("Starting the process to fetch and save Sackmann data...")

    try:
        # Fetch the processed data using the function from p_sack_preproc
        sackmann_data = get_all_sackmann_data()

        # Save the collected data
        save_data_to_csv(sackmann_data)

    except Exception as e:
        print(f"An error occurred during the main process: {e}")
        import traceback
        traceback.print_exc() # Print detailed traceback for debugging

    print("Process finished.")

if __name__ == "__main__":
    main()