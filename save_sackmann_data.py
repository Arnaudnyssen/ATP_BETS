# save_sackmann_data.py (Imports and saves matchup data)

import pandas as pd
from datetime import datetime
import os
import sys
import traceback
from typing import Optional # Keep Optional for type hints if needed elsewhere

# --- Constants ---
OUTPUT_DIRECTORY = "data_archive"
BASE_FILENAME = "sackmann_matchups" # Changed base name slightly for clarity
DATE_FORMAT = "%Y%m%d"

# --- Import Preprocessing Function ---
try:
    # **** CORRECTED IMPORT: Use the new function name ****
    from p_sack_preproc import get_all_matchup_data
except ImportError as e:
    print(f"Error importing 'get_all_matchup_data': {e}")
    # Add path adjustment logic if needed, similar to p_sack_preproc
    project_dir = os.path.dirname(os.path.abspath(__file__))
    if project_dir not in sys.path:
        sys.path.append(project_dir)
        print(f"Added {project_dir} to sys.path")
        try:
            # Retry import after path adjustment
            from p_sack_preproc import get_all_matchup_data
            print("Import successful after path adjustment.")
        except ImportError as e2:
            print(f"Still cannot import 'get_all_matchup_data' after path adjustment: {e2}")
            print("Ensure 'p_sack_preproc.py' exists and contains the 'get_all_matchup_data' function.")
            print(f"Current sys.path: {sys.path}")
            sys.exit(1) # Exit if import fails
    else:
        # If already in path but failed, likely function name or file issue
        print("Ensure 'p_sack_preproc.py' exists and contains the 'get_all_matchup_data' function.")
        print(f"Current sys.path: {sys.path}")
        sys.exit(1)


# --- Saving Function (No changes needed here) ---
def save_data_to_dated_csv(data: pd.DataFrame, base_filename: str, output_dir: str) -> Optional[str]:
    """
    Saves the provided DataFrame to a CSV file with today's date in the filename,
    inside the specified output directory. Creates the directory if it doesn't exist.
    """
    if data is None or data.empty:
        print("No data provided or DataFrame is empty. Nothing to save.")
        return None
    try:
        os.makedirs(output_dir, exist_ok=True)
        print(f"Ensured output directory exists: '{output_dir}'")
    except OSError as e:
        print(f"Error creating output directory '{output_dir}': {e}")
        return None

    today_date_str = datetime.now().strftime(DATE_FORMAT)
    filename = f"{base_filename}_{today_date_str}.csv"
    output_path = os.path.join(output_dir, filename)
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
    Main function to fetch matchup data and save it to a dated CSV file.
    """
    print("Starting the process to fetch and save Sackmann MATCHUP data...")

    try:
        # Step 1: Fetch the processed matchup data
        print("Fetching processed matchup data using get_all_matchup_data()...")
        # **** CORRECTED FUNCTION CALL and variable name ****
        matchup_data = get_all_matchup_data()

        # Step 2: Save the collected data to the archive directory
        if matchup_data is not None and not matchup_data.empty:
            print(f"Matchup data fetched successfully. Shape: {matchup_data.shape}")
            saved_filepath = save_data_to_dated_csv(
                data=matchup_data, # Pass the correct DataFrame
                base_filename=BASE_FILENAME,
                output_dir=OUTPUT_DIRECTORY
            )
            if saved_filepath:
                print(f"Data saving process completed successfully. File: {saved_filepath}")
            else:
                print("Data saving process failed.")
        elif matchup_data is None:
             print("Fetching matchup data failed (returned None). No data to save.")
        else: # matchup_data is empty DataFrame
             print("Fetched matchup data is empty. No data to save.")

    except Exception as e:
        print(f"An critical error occurred during the main process in save_sackmann_data.py: {e}")
        traceback.print_exc()

    print("Save process finished.")

if __name__ == "__main__":
    main()
