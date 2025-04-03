# save_sackmann_data.py (Correct Version - Saving Dated Files)

import pandas as pd
from datetime import datetime, timezone # Ensure timezone is imported
import os
import sys
from typing import Optional # Ensure Optional is imported

# Ensure the main project directory is in the Python path (adjust if needed)
try:
    from p_sack_preproc import get_all_sackmann_data
except ImportError:
    project_dir = os.path.dirname(os.path.abspath(__file__))
    if project_dir not in sys.path:
        sys.path.append(project_dir)
    try:
        from p_sack_preproc import get_all_sackmann_data
    except ImportError as e:
         print(f"Error importing 'get_all_sackmann_data': {e}")
         print("Ensure 'p_sack_preproc.py' is accessible and the project structure is correct.")
         print(f"Current sys.path: {sys.path}")
         sys.exit(1)

def save_data_to_dated_csv(data: pd.DataFrame, base_filename: str = "sackmann_data", output_dir: str = "data_archive") -> Optional[str]:
    """
    Saves the provided DataFrame to a CSV file named with the current date
    in the specified output directory. Creates the directory if it doesn't exist.

    Args:
        data (pd.DataFrame): The DataFrame to save.
        base_filename (str): The base name for the output CSV file.
        output_dir (str): The directory where the CSV file will be saved.

    Returns:
        str: The full path to the saved file, or None if saving failed.
    """
    if data is None or data.empty: # Check if data is None or empty
        print("No data provided to save.")
        return None

    # Create the output directory if it doesn't exist
    try:
        # Use os.path.abspath to handle relative paths robustly
        abs_output_dir = os.path.abspath(output_dir)
        os.makedirs(abs_output_dir, exist_ok=True)
        print(f"Output directory '{abs_output_dir}' ensured.")
    except OSError as e:
        print(f"Error creating output directory '{abs_output_dir}': {e}")
        return None # Cannot proceed without output directory

    # Generate filename with date
    today_date = datetime.now().strftime("%Y%m%d") # Format: YYYYMMDD
    output_filename = f"{base_filename}_{today_date}.csv"
    # Use the absolute path for saving
    output_path = os.path.join(abs_output_dir, output_filename)

    # Save the DataFrame to CSV
    try:
        # Add a timestamp column to the data before saving
        data_to_save = data.copy()
        # Use timezone.utc for consistency
        data_to_save['ScrapeTimestampUTC'] = datetime.now(timezone.utc).isoformat()

        data_to_save.to_csv(output_path, index=False, encoding='utf-8')
        # Print the absolute path where it was saved
        print(f"Successfully saved Sackmann data to: {output_path}")
        return output_path
    except Exception as e:
        print(f"Error saving data to CSV file '{output_path}': {e}")
        return None

def main():
    """
    Main function to fetch Sackmann data and save it to a dated CSV file.
    """
    print("Starting the process to fetch and save Sackmann data...")
    output_directory = "data_archive" # Define where to save historical CSVs

    try:
        # Fetch the processed data
        sackmann_data = get_all_sackmann_data()

        # Save the collected data to a dated file
        # Pass the relative directory name; the function handles making it absolute
        saved_filepath = save_data_to_dated_csv(sackmann_data, output_dir=output_directory)

        if saved_filepath:
            print(f"Data saving process completed for {saved_filepath}")
        else:
            # Make sure this case is handled (e.g., maybe the workflow should fail)
            print("Failed to save data (likely no data was scraped).")

    except Exception as e:
        print(f"An error occurred during the main process: {e}")
        import traceback
        traceback.print_exc()

    print("Process finished.")

if __name__ == "__main__":
    main()
