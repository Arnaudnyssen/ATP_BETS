# generate_page.py (Corrected Replace Calls using Placeholders)

import pandas as pd
from datetime import datetime
import os
import glob
import pytz
import traceback
from typing import Optional

# --- Constants for Placeholders ---
TABLE_PLACEHOLDER = ""
TIMESTAMP_PLACEHOLDER = ""
ERROR_MESSAGE_CLASS = "error-message" # CSS class for styling errors

# --- find_latest_csv function remains the same ---
def find_latest_csv(directory: str, pattern: str = "sackmann_data_*.csv") -> Optional[str]:
    """Finds the most recently created CSV file matching the pattern in a directory."""
    try:
        search_path = os.path.join(directory, pattern)
        list_of_files = glob.glob(search_path)
        if not list_of_files:
            print(f"No files found matching pattern '{pattern}' in directory '{directory}'.")
            return None
        # Use modification time as a fallback if creation time is not available/reliable
        try:
             latest_file = max(list_of_files, key=os.path.getctime)
        except AttributeError: # Handle systems where getctime might not be available
             latest_file = max(list_of_files, key=os.path.getmtime)
        except ValueError: # Handle case where glob returns empty list after all checks
             print("Error finding latest file: No files match pattern after filtering.")
             return None
        print(f"Found latest CSV file: {latest_file}")
        return latest_file
    except Exception as e:
        print(f"Error finding latest CSV file in '{directory}': {e}")
        traceback.print_exc()
        return None

# --- generate_html_table function (minor update to wrap errors in styled div) ---
def generate_html_table(csv_filepath: str) -> str:
    """
    Reads the CSV and generates an HTML table string or a styled error message.
    """
    # Helper function to format error messages
    def format_error(message: str) -> str:
        print(f"Error generating table: {message}") # Log the error
        # Wrap the user-facing message in a div with the error class
        return f'<div class="{ERROR_MESSAGE_CLASS}">{message} Check workflow logs for details.</div>'

    if csv_filepath is None or not os.path.exists(csv_filepath):
         return format_error("Data file not found.")

    try:
        # Check if file is empty *before* reading
        if os.path.getsize(csv_filepath) == 0:
            return format_error("No match data available (data file is empty).")

        # Read the CSV
        df = pd.read_csv(csv_filepath)

        # Check if DataFrame is empty after reading
        if df.empty:
            print(f"Warning: DataFrame is empty after reading {csv_filepath}.")
            return format_error("No match data available (DataFrame is empty after reading).")

        print("Preparing data for HTML table...")
        # Drop timestamp if it exists
        if 'ScrapeTimestampUTC' in df.columns:
             df_display = df.drop(columns=['ScrapeTimestampUTC']).copy()
        else:
             df_display = df.copy()

        # Define desired columns and find which ones actually exist
        columns_to_display = ['Player', 'Probability (%)', 'Decimal_Odds', 'Round', 'Tournament_URL']
        existing_columns = [col for col in columns_to_display if col in df_display.columns]

        if not existing_columns:
             return format_error("No relevant columns found in the data file.")
        if 'Player' not in existing_columns:
             return format_error("'Player' column is missing from the data file.")

        df_display = df_display[existing_columns].copy()
        print(f"Selected columns for display: {existing_columns}")

        # Rename columns for better readability
        rename_map = {'Probability (%)': 'Probability','Decimal_Odds': 'Odds','Tournament_URL': 'Tournament'}
        rename_map_existing = {k: v for k, v in rename_map.items() if k in df_display.columns}
        df_display.rename(columns=rename_map_existing, inplace=True)
        print(f"Renamed columns: {rename_map_existing}")

        # Process Tournament URL to get a readable name
        if 'Tournament' in df_display.columns:
            try:
                 # Handle potential non-string values or URLs without slashes
                 df_display['Tournament'] = df_display['Tournament'].apply(
                     lambda url: url.split('/')[-2].replace('-', ' ').title()
                     if isinstance(url, str) and '/' in url and len(url.split('/')) > 2
                     else 'Unknown Tournament'
                 )
            except Exception as e:
                 print(f"Warning: Could not process Tournament URLs cleanly - {e}. Setting to 'Unknown'.")
                 df_display['Tournament'] = 'Unknown Tournament' # Ensure column exists even on error

        # Format Probability column
        if 'Probability' in df_display.columns:
            try:
                # Convert to numeric, coercing errors to NaN
                df_display['Probability'] = pd.to_numeric(df_display['Probability'], errors='coerce')
                # Sort only if the column is numeric and has non-NaN values
                if pd.api.types.is_numeric_dtype(df_display['Probability']) and not df_display['Probability'].isna().all():
                     df_display.sort_values(by='Probability', ascending=False, inplace=True, na_position='last')
                # Format as percentage string, ignoring NaNs
                df_display['Probability'] = df_display['Probability'].map('{:.1f}%'.format, na_action='ignore')
            except Exception as e:
                 print(f"Warning: Could not sort/format Probability column - {e}")

        # Format Odds column
        if 'Odds' in df_display.columns:
             try:
                # Convert to numeric, coercing errors to NaN
                df_display['Odds'] = pd.to_numeric(df_display['Odds'], errors='coerce')
                # Format as decimal string, ignoring NaNs
                df_display['Odds'] = df_display['Odds'].map('{:.2f}'.format, na_action='ignore')
             except Exception as e:
                 print(f"Warning: Could not format Odds column - {e}")

        print("Generating HTML string from DataFrame...")
        try:
            # Generate the HTML table using pandas
            html_table = df_display.to_html(
                index=False,        # Don't include DataFrame index
                border=0,           # No border attribute on table tag
                classes='dataframe',# Add 'dataframe' class for styling
                escape=True,        # Escape HTML characters in data
                na_rep='-'          # Representation for NaN values
            )
            # Basic check to ensure a table was generated
            if "<table" not in html_table or "</table>" not in html_table:
                 print("Warning: pandas to_html did not return expected table tags.")
                 return format_error("Could not format data into a table.")

            print("HTML table generated successfully.")
            return html_table
        except Exception as e_html:
             print(f"Error during pandas to_html conversion: {e_html}")
             traceback.print_exc()
             return format_error(f"Failed during HTML conversion ({type(e_html).__name__}).")

    except pd.errors.EmptyDataError:
        # This might occur if the CSV exists but pandas can't read any data (e.g., only headers)
        return format_error("No match data available (file is empty or unreadable by pandas).")
    except FileNotFoundError:
         # Should be caught earlier, but added for robustness
         return format_error("Data file not found (should not happen here).")
    except Exception as e:
        # Catch-all for other unexpected errors during processing
        print(f"Error processing CSV or generating HTML table: {e}")
        traceback.print_exc()
        return format_error(f"An unexpected error ({type(e).__name__}) occurred during table generation.")


# --- update_index_html function MODIFIED to use placeholders ---
def update_index_html(template_path: str, output_path: str, table_html_content: str):
    """
    Reads the template, injects the table/error message and timestamp using placeholders,
    and writes the output HTML.
    """
    print(f"Updating {output_path} using template {template_path}...")
    try:
        # Read the template content
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()

        # Check if placeholders exist in the template
        if TABLE_PLACEHOLDER not in template_content:
            print(f"ERROR: Table placeholder '{TABLE_PLACEHOLDER}' not found in template '{template_path}'!")
            # Optionally, write the original template to output to avoid breaking the page completely
            # with open(output_path, 'w', encoding='utf-8') as f:
            #     f.write(template_content)
            return # Stop processing if placeholder is missing
        if TIMESTAMP_PLACEHOLDER not in template_content:
            print(f"ERROR: Timestamp placeholder '{TIMESTAMP_PLACEHOLDER}' not found in template '{template_path}'!")
            # Optionally handle as above
            return # Stop processing if placeholder is missing

        # Get current UTC time
        update_time = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
        last_updated_text = f"Last updated: {update_time}"

        # *** CORRECTED .replace() CALLS using defined placeholders ***
        content_with_table = template_content.replace(TABLE_PLACEHOLDER, table_html_content)
        final_content = content_with_table.replace(TIMESTAMP_PLACEHOLDER, last_updated_text)
        # *** END CORRECTION ***

        # Verify replacements (optional but good for debugging)
        if TABLE_PLACEHOLDER in final_content:
            print("WARNING: Table placeholder WAS NOT REPLACED in final content! Check logic.")
        else:
            print("Table placeholder replaced successfully.")

        if TIMESTAMP_PLACEHOLDER in final_content:
             print("WARNING: Last updated placeholder WAS NOT REPLACED in final content! Check logic.")
        else:
             print("Last updated placeholder replaced successfully.")

        # Write the final content to the output file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(final_content)
        print(f"Successfully wrote updated content to {output_path}")

    except FileNotFoundError:
        print(f"Error: Template file not found at {template_path}")
    except Exception as e:
        print(f"Error updating index.html: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    # Define paths relative to the script location or use absolute paths
    # Assuming script runs from the root of the project directory
    DATA_DIR = "data_archive"
    TEMPLATE_FILE = "index.html"      # Source template
    OUTPUT_HTML_FILE = "index.html"   # Destination file (overwrites template in this case)

    print("Starting page generation process...")
    print(f"Looking for latest CSV in: {os.path.abspath(DATA_DIR)}")
    print(f"Using template: {os.path.abspath(TEMPLATE_FILE)}")
    print(f"Outputting to: {os.path.abspath(OUTPUT_HTML_FILE)}")

    print("\nFinding latest data file...")
    latest_csv = find_latest_csv(DATA_DIR)

    table_html_content = "" # Initialize variable

    if latest_csv:
        print(f"\nGenerating HTML table from: {latest_csv}")
        table_html_content = generate_html_table(latest_csv)
    else:
        # If no CSV found, create the standard error message
        print(f"\nNo CSV file found in {DATA_DIR}. Generating error message for HTML content.")
        # Use the helper function from generate_html_table for consistency
        table_html_content = f'<div class="{ERROR_MESSAGE_CLASS}">Error: Could not find the latest data file to generate the table. Check workflow logs.</div>'


    # Ensure table_html_content is always a string before updating
    if not isinstance(table_html_content, str):
        print("ERROR: table_html_content is not a string! Defaulting to an error message.")
        table_html_content = f'<div class="{ERROR_MESSAGE_CLASS}">Internal Error: Failed to generate valid HTML content.</div>'


    print(f"\nUpdating {OUTPUT_HTML_FILE}...")
    # Pass the generated table HTML (or error message) to the update function
    update_index_html(TEMPLATE_FILE, OUTPUT_HTML_FILE, table_html_content)

    print("\nPage generation process complete.")
