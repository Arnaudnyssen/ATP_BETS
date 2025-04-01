# save_sackmann_data.py (Modified)

import pandas as pd
from datetime import datetime
import os
import sys

# Ensure the main project directory is in the Python path
# Adjust the path ('..') if this script is placed in a subdirectory
# Assumes this script is in the root or a known location relative to p_sack_preproc.py
try:
    # If save_sackmann_data.py is in the root, p_sack_preproc should be importable
    from p_sack_preproc import get_all_sackmann_data
except ImportError:
    # If it's in a subdirectory, adjust the path
    project_dir = os.path.dirname(os.path.abspath(__file__)) # Or os.path.dirname(os.path.dirname(...))
    if project_dir not in sys.path:
        sys.path.append(project_dir)
    try:
        from p_sack_preproc import get_all_sackmann_data
    except ImportError as e:
         print(f"Error importing 'get_all_sackmann_data': {e}")
         print("Ensure 'p_sack_preproc.py' is accessible and the project structure is correct.")
         print(f"Current sys.path: {sys.path}")
         sys.exit(1) # Exit if import fails


def save_data_to_csv(data: pd.DataFrame, filename: str = "sackmann_data.csv", output_dir: str = ".") -> None:
    """
    Saves the provided DataFrame to a CSV file with a fixed name in the specified directory.
    Creates the output directory if it doesn't exist.

    Args:
        data (pd.DataFrame): The DataFrame to save.
        filename (str): The fixed name for the output CSV file.
        output_dir (str): The directory where the CSV file will be saved. Defaults to current dir.
    """
    if data.empty:
        print("No data to save.")
        return

    # Create the output directory if it doesn't exist and is not the current directory
    if output_dir != ".":
        try:
            os.makedirs(output_dir, exist_ok=True)
            print(f"Output directory '{output_dir}' ensured.")
        except OSError as e:
            print(f"Error creating output directory '{output_dir}': {e}")
            return # Cannot proceed without output directory

    output_path = os.path.join(output_dir, filename)

    # Save the DataFrame to CSV, overwriting if it exists
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

        # Save the collected data using the modified function
        # Save it in the root directory (output_dir=".") for easier access by GitHub Actions steps
        save_data_to_csv(sackmann_data, filename="sackmann_data.csv", output_dir=".")

    except Exception as e:
        print(f"An error occurred during the main process: {e}")
        import traceback
        traceback.print_exc() # Print detailed traceback for debugging

    print("Process finished.")

if __name__ == "__main__":
    main()
