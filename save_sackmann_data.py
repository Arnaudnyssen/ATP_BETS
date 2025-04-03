# save_sackmann_data.py (Save to data_archive)

import pandas as pd
from datetime import datetime
import os
import sys
import traceback # Import traceback

# --- Constants ---
# Define the target directory and filename pattern
# Ensure this matches what subsequent steps expect (like generate_page.py)
OUTPUT_DIRECTORY = "data_archive"
BASE_FILENAME = "sackmann_data" # Base name without date
DATE_FORMAT = "%Y%m%d"

# --- Import Preprocessing Function ---
# Ensure the main project directory is in the Python path
try:
    # Assumes save_sackmann_data.py and p_sack_preproc.py are in the same directory (e.g., project root)
    from p_sack_preproc import get_all_sackmann_data
except ImportError:
    # Handle cases where scripts might be in subdirectories if needed
    # This basic structure assumes they are siblings
    project_dir = os.path.dirname(os.path.abspath(__file__))
    if project_dir not in sys.path:
        sys.path.append(project_dir)
    try:
        from p_sack_preproc import get_all_sackmann_data
    except ImportError as e:
         print(f"Error importing 'get_all_sackmann_data': {e}")
         print("Ensure 'p_sack_preproc.py' is accessible (e.g., in the same directory).")
         print(f"Current sys.path: {sys.path}")
         sys.exit(1) # Exit if import fails


# --- Saving Function ---
def save_data_to_dated_csv(data: pd.DataFrame, base_filename: str, output_dir: str) -> Optional[str]:
    """
    Saves the provided DataFrame to a CSV file with today's date in the filename,
    inside the specified output directory. Creates the directory if it doesn't exist.

    Args:
        data (pd.DataFrame): The DataFrame to save.
        base_filename (str): The base name for the CSV file (e.g., "sackmann_data").
        output_dir (str): The directory where the CSV file will be saved (e.g., "data_archive").

    Returns:
        Optional[str]: The full path to the saved file, or None if saving failed.
    """
    if data is None or data.empty:
        print("No data provided or DataFrame is empty. Nothing to save.")
        return None

    # --- Create Output Directory ---
    try:
        # exist_ok=True prevents an error if the directory already exists
        os.makedirs(output_dir, exist_ok=True)
        print(f"Ensured output directory exists: '{output_dir}'")
    except OSError as e:
        print(f"Error creating output directory '{output_dir}': {e}")
        return None # Cannot proceed without output directory

    # --- Construct Filename ---
    today_date_str = datetime.now().strftime(DATE_FORMAT)
    filename = f"{base_filename}_{today_date_str}.csv"
    output_path = os.path.join(output_dir, filename)
    print(f"Attempting to save data to: {output_path}")

    # --- Save DataFrame to CSV ---
    try:
        data.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Successfully saved data to: {output_path}")
        return output_path # Return the path of the saved file
    except Exception as e:
        print(f"Error saving data to CSV file '{output_path}': {e}")
        traceback.print_exc()
        return None


# --- Main Execution ---
def main():
    """
    Main function to fetch Sackmann data and save it to a dated CSV file
    in the specified archive directory.
    """
    print("Starting the process to fetch and save Sackmann data...")

    try:
        # Step 1: Fetch the processed data
        print("Fetching processed data using get_all_sackmann_data()...")
        sackmann_data = get_all_sackmann_data() # This now returns the consolidated DataFrame

        # Step 2: Save the collected data to the archive directory
        if sackmann_data is not None and not sackmann_data.empty:
            print(f"Data fetched successfully. Shape: {sackmann_data.shape}")
            # **** SAVE TO THE CORRECT DIRECTORY ****
            saved_filepath = save_data_to_dated_csv(
                data=sackmann_data,
                base_filename=BASE_FILENAME,
                output_dir=OUTPUT_DIRECTORY # Use the constant defined above
            )
            if saved_filepath:
                print(f"Data saving process completed successfully. File: {saved_filepath}")
            else:
                print("Data saving process failed.")
        elif sackmann_data is None:
             print("Fetching data failed (returned None). No data to save.")
        else: # sackmann_data is empty DataFrame
             print("Fetched data is empty. No data to save.")


    except Exception as e:
        print(f"An critical error occurred during the main process in save_sackmann_data.py: {e}")
        traceback.print_exc() # Print detailed traceback for debugging

    print("Save process finished.")

if __name__ == "__main__":
    main()
