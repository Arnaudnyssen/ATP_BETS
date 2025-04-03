# generate_page.py (Using Comment Placeholders)

import pandas as pd
from datetime import datetime
import os
import glob
import pytz
import traceback
from typing import Optional

# --- Constants for Placeholders ---
# *** THESE MUST EXACTLY MATCH THE COMMENTS IN index.html ***
TABLE_PLACEHOLDER = ""
TIMESTAMP_PLACEHOLDER = ""
ERROR_MESSAGE_CLASS = "error-message" # CSS class for styling errors

# --- find_latest_csv function ---
def find_latest_csv(directory: str, pattern: str = "sackmann_data_*.csv") -> Optional[str]:
    """Finds the most recently created CSV file matching the pattern in a directory."""
    try:
        search_path = os.path.join(directory, pattern)
        # Use absolute path for clarity in logs
        abs_search_path = os.path.abspath(search_path)
        print(f"Searching for pattern: {abs_search_path}")
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
        print(f"Found latest CSV file: {latest_file} (Full path: {os.path.abspath(latest_file)})")
        return latest_file
    except Exception as e:
        print(f"Error finding latest CSV file in '{directory}': {e}")
        traceback.print_exc()
        return None

# --- generate_html_table function ---
def generate_html_table(csv_filepath: str) -> str:
    """ Reads the CSV and generates an HTML table string or a styled error message. """
    def format_error(message: str) -> str:
        print(f"Error generating table: {message}")
        return f'<div class="{ERROR_MESSAGE_CLASS}">{message} Check workflow logs for details.</div>'

    abs_csv_filepath = os.path.abspath(csv_filepath)
    print(f"Attempting to read CSV: {abs_csv_filepath}")

    if csv_filepath is None or not os.path.exists(csv_filepath):
         return format_error(f"Data file not found at {abs_csv_filepath}.")

    try:
        if os.path.getsize(csv_filepath) == 0:
            return format_error(f"No match data available (data file is empty: {abs_csv_filepath}).")

        df = pd.read_csv(csv_filepath)

        if df.empty:
            print(f"Warning: DataFrame is empty after reading {csv_filepath}.")
            return format_error("No match data available (DataFrame is empty after reading).")

        print("Preparing data for HTML table...")
        # Drop timestamp if it exists, handle potential absence gracefully
        if 'ScrapeTimestampUTC' in df.columns:
             df_display = df.drop(columns=['ScrapeTimestampUTC']).copy()
             print("Dropped 'ScrapeTimestampUTC' column.")
        else:
             df_display = df.copy()
             print("Note: 'ScrapeTimestampUTC' column not found in CSV.")


        columns_to_display = ['Player', 'Probability (%)', 'Decimal_Odds', 'Round', 'Tournament_URL']
        print(f"Desired columns: {columns_to_display}")
        print(f"Available columns in CSV: {df_display.columns.tolist()}")
        existing_columns = [col for col in columns_to_display if col in df_display.columns]

        if not existing_columns:
             return format_error(f"No relevant columns ({', '.join(columns_to_display)}) found in the data file.")
        if 'Player' not in existing_columns:
             # This case should be rare if scraper worked, but good check
             return format_error("'Player' column is missing from the data file.")

        df_display = df_display[existing_columns].copy()
        print(f"Selected columns for display: {existing_columns}")

        rename_map = {'Probability (%)': 'Probability','Decimal_Odds': 'Odds','Tournament_URL': 'Tournament'}
        rename_map_existing = {k: v for k, v in rename_map.items() if k in df_display.columns}
        if rename_map_existing:
            df_display.rename(columns=rename_map_existing, inplace=True)
            print(f"Renamed columns: {rename_map_existing}")

        if 'Tournament' in df_display.columns:
            print("Processing 'Tournament' column...")
            try:
                 df_display['Tournament'] = df_display['Tournament'].apply(
                     lambda url: url.split('/')[-2].replace('-', ' ').title()
                     if isinstance(url, str) and '/' in url and len(url.split('/')) > 2
                     else 'Unknown Tournament' # Handle malformed URLs or non-strings
                 )
                 print("Processed 'Tournament' column successfully.")
            except Exception as e:
                 print(f"Warning: Could not process Tournament URLs cleanly - {e}. Setting to 'Unknown'.")
                 df_display['Tournament'] = 'Unknown Tournament'

        if 'Probability' in df_display.columns:
            print("Processing 'Probability' column...")
            try:
                df_display['Probability'] = pd.to_numeric(df_display['Probability'], errors='coerce')
                if pd.api.types.is_numeric_dtype(df_display['Probability']) and not df_display['Probability'].isna().all():
                    df_display.sort_values(by='Probability', ascending=False, inplace=True, na_position='last')
                    print("Sorted by 'Probability'.")
                df_display['Probability'] = df_display['Probability'].map('{:.1f}%'.format, na_action='ignore')
                print("Formatted 'Probability' column.")
            except Exception as e: print(f"Warning: Could not sort/format Probability column - {e}")

        if 'Odds' in df_display.columns:
             print("Processing 'Odds' column...")
             try:
                df_display['Odds'] = pd.to_numeric(df_display['Odds'], errors='coerce')
                df_display['Odds'] = df_display['Odds'].map('{:.2f}'.format, na_action='ignore')
                print("Formatted 'Odds' column.")
             except Exception as e: print(f"Warning: Could not format Odds column - {e}")

        print("Generating HTML string from DataFrame...")
        try:
            html_table = df_display.to_html(index=False, border=0, classes='dataframe', escape=True, na_rep='-')
            if "<table" not in html_table or "</table>" not in html_table:
                 print("Warning: pandas to_html did not return expected table tags.")
                 return format_error("Could not format data into a table.")
            print(f"HTML table string generated successfully (Length: {len(html_table)} chars).")
            return html_table
        except Exception as e_html:
             print(f"Error during pandas to_html conversion: {e_html}")
             traceback.print_exc()
             return format_error(f"Failed during HTML conversion ({type(e_html).__name__}).")

    except pd.errors.EmptyDataError:
        return format_error(f"No match data available (file is empty or unreadable by pandas: {abs_csv_filepath}).")
    except FileNotFoundError:
         # Should be caught earlier, but added for robustness
         return format_error(f"Data file not found at {abs_csv_filepath} (Error in generate_html_table).")
    except Exception as e:
        print(f"Error processing CSV or generating HTML table: {e}")
        traceback.print_exc()
        return format_error(f"An unexpected error ({type(e).__name__}) occurred during table generation.")


# --- update_index_html function ---
def update_index_html(template_path: str, output_path: str, table_html_content: str):
    """
    Reads the template, injects the table/error message and timestamp using placeholders,
    and writes the output HTML.
    """
    abs_template_path = os.path.abspath(template_path)
    abs_output_path = os.path.abspath(output_path)
    print(f"Updating '{abs_output_path}' using template '{abs_template_path}'...")

    try:
        # Read the template content
        print(f"Reading template file: {abs_template_path}")
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        print(f"Template content read successfully (Length: {len(template_content)} chars).")

        # Check if placeholders exist BEFORE attempting replacement
        table_placeholder_found = TABLE_PLACEHOLDER in template_content
        timestamp_placeholder_found = TIMESTAMP_PLACEHOLDER in template_content

        if not table_placeholder_found:
            print(f"ERROR: Table placeholder '{TABLE_PLACEHOLDER}' was NOT found in the template content read from '{template_path}'. Replacement will fail.")
        else:
            print(f"Table placeholder '{TABLE_PLACEHOLDER}' found in template.")

        if not timestamp_placeholder_found:
            print(f"ERROR: Timestamp placeholder '{TIMESTAMP_PLACEHOLDER}' was NOT found in the template content read from '{template_path}'. Replacement will fail.")
        else:
            print(f"Timestamp placeholder '{TIMESTAMP_PLACEHOLDER}' found in template.")

        # Only proceed if placeholders were found
        if table_placeholder_found and timestamp_placeholder_found:
            update_time = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
            last_updated_text = f"Last updated: {update_time}"

            # Perform replacements
            print("Performing replacements...")
            content_with_table = template_content.replace(TABLE_PLACEHOLDER, table_html_content)
            final_content = content_with_table.replace(TIMESTAMP_PLACEHOLDER, last_updated_text)
            print("Replacements performed.")

            # Verify replacements happened (should always work if placeholders were found)
            if TABLE_PLACEHOLDER in final_content:
                 print("INTERNAL WARNING: Table placeholder remained after replacement! Check for unexpected characters.")
            else:
                 print("Table placeholder replaced successfully.")
            if TIMESTAMP_PLACEHOLDER in final_content:
                 print("INTERNAL WARNING: Timestamp placeholder remained after replacement! Check for unexpected characters.")
            else:
                 print("Last updated placeholder replaced successfully.")

            # Write the final content to the output file
            print(f"Writing updated content to: {abs_output_path}")
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(final_content)
            print(f"Successfully wrote updated content to {output_path}")

        else:
            # If placeholders weren't found, write the original template content back
            # to avoid deleting the file or leaving it in an inconsistent state.
            # Log an error message.
            print("ERROR: Placeholders not found in template. Writing original template content back to output file to prevent data loss.")
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(template_content) # Write original content back

    except FileNotFoundError:
        print(f"Error: Template file not found at {template_path}")
        # Avoid writing if template doesn't exist
    except Exception as e:
        print(f"Error updating index.html: {e}")
        traceback.print_exc()


# --- Main execution block ---
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
        print(f"\nNo CSV file found in {DATA_DIR}. Generating error message for HTML content.")
        table_html_content = f'<div class="{ERROR_MESSAGE_CLASS}">Error: Could not find the latest data file to generate the table. Check workflow logs.</div>'

    # Ensure table_html_content is always a string before updating
    if not isinstance(table_html_content, str):
        print("ERROR: table_html_content is not a string! Defaulting to an error message.")
        table_html_content = f'<div class="{ERROR_MESSAGE_CLASS}">Internal Error: Failed to generate valid HTML content.</div>'

    print(f"\nUpdating {OUTPUT_HTML_FILE}...")
    update_index_html(TEMPLATE_FILE, OUTPUT_HTML_FILE, table_html_content)

    print("\nPage generation process complete.")
