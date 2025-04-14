# save_sackmann_data.py (v2 - Save Matchups & Results)
# Imports and saves matchup and results data from p_sack_preproc.

import pandas as pd
from datetime import datetime
import os
import sys
import traceback
from typing import Optional, Tuple # Added Tuple

# --- Constants ---
OUTPUT_DIRECTORY = "data_archive"
MATCHUPS_FILENAME_BASE = "sackmann_matchups" # Filename for matchups
RESULTS_FILENAME_BASE = "match_results"     # Filename for results
DATE_FORMAT = "%Y%m%d"

# --- Import Preprocessing Function ---
try:
    # Import the function that now returns two dataframes
    from p_sack_preproc import get_all_data
except ImportError as e:
    print(f"Error importing 'get_all_data': {e}")
    project_dir = os.path.dirname(os.path.abspath(__file__))
    if project_dir not in sys.path:
        sys.path.append(project_dir)
        print(f"Added {project_dir} to sys.path")
        try:
            from p_sack_preproc import get_all_data
            print("Import successful after path adjustment.")
        except ImportError as e2:
            print(f"Still cannot import 'get_all_data' after path adjustment: {e2}")
            print("Ensure 'p_sack_preproc.py' exists and contains the 'get_all_data' function.")
            sys.exit(1)
    else:
        print("Ensure 'p_sack_preproc.py' exists and contains the 'get_all_data' function.")
        sys.exit(1)


# --- Saving Function (No changes needed here) ---
def save_data_to_dated_csv(data: pd.DataFrame, base_filename: str, output_dir: str) -> Optional[str]:
    """
    Saves the provided DataFrame to a CSV file with today's date in the filename,
    inside the specified output directory. Creates the directory if it doesn't exist.
    """
    if data is None or data.empty:
        print(f"No data provided for '{base_filename}' or DataFrame is empty. Nothing to save.")
        return None
    # Ensure output dir exists
    script_dir = os.path.dirname(os.path.abspath(__file__))
    absolute_output_dir = os.path.join(script_dir, output_dir)
    try:
        os.makedirs(absolute_output_dir, exist_ok=True)
        # print(f"Ensured output directory exists: '{absolute_output_dir}'") # Less verbose
    except OSError as e:
        print(f"Error creating output directory '{absolute_output_dir}': {e}")
        return None

    today_date_str = datetime.now().strftime(DATE_FORMAT)
    filename = f"{base_filename}_{today_date_str}.csv"
    output_path = os.path.join(absolute_output_dir, filename)
    print(f"Attempting to save data to: {output_path}")

    try:
        data.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Successfully saved data to: {output_path}")
        return output_path
    except Exception as e:
        print(f"Error saving data to CSV file '{output_path}': {e}")
        traceback.print_exc()
        return None


# --- Main Execution ---
def main():
    """
    Main function to fetch matchup and results data and save them to dated CSV files.
    """
    print("Starting the process to fetch and save Sackmann MATCHUP and RESULTS data...")

    try:
        # Step 1: Fetch the processed matchup and results data
        print("Fetching processed data using get_all_data()...")
        matchup_data, results_data = get_all_data() # Expects two dataframes

        # Step 2: Save the matchup data
        if matchup_data is not None and not matchup_data.empty:
            print(f"\nMatchup data fetched successfully. Shape: {matchup_data.shape}")
            saved_matchups_path = save_data_to_dated_csv(
                data=matchup_data,
                base_filename=MATCHUPS_FILENAME_BASE,
                output_dir=OUTPUT_DIRECTORY
            )
            if not saved_matchups_path: print("Matchup data saving process failed.")
        else:
             print("\nNo matchup data was fetched or it was empty.")

        # Step 3: Save the results data
        if results_data is not None and not results_data.empty:
            print(f"\nResults data fetched successfully. Shape: {results_data.shape}")
            saved_results_path = save_data_to_dated_csv(
                data=results_data,
                base_filename=RESULTS_FILENAME_BASE,
                output_dir=OUTPUT_DIRECTORY
            )
            if not saved_results_path: print("Results data saving process failed.")
        else:
             print("\nNo results data was fetched or it was empty.")

    except Exception as e:
        print(f"An critical error occurred during the main process in save_sackmann_data.py: {e}")
        traceback.print_exc()

    print("\nSave process finished.")

if __name__ == "__main__":
    main()
