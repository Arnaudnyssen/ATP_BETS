# generate_page.py (Using Comment Placeholders - Recommended Version)

import pandas as pd
from datetime import datetime
import os
import glob
import pytz
import traceback
from typing import Optional

# --- Constants ---
# Use comment placeholders for robust replacement
TABLE_PLACEHOLDER = ""
TIMESTAMP_PLACEHOLDER = ""

ERROR_MESSAGE_CLASS = "error-message" # CSS class for styling errors
DATA_DIR = "data_archive"             # Directory for CSV files
CSV_PATTERN = "sackmann_data_*.csv"   # Pattern for CSV files
TEMPLATE_FILE = "index.html"          # Source template HTML file
OUTPUT_HTML_FILE = "index.html"       # Destination HTML file (overwrites template)


# --- Helper Functions ---

def find_latest_csv(directory: str, pattern: str) -> Optional[str]:
    """
    Finds the most recently created CSV file matching the pattern in a directory.

    Args:
        directory (str): The directory to search in.
        pattern (str): The glob pattern for the CSV files (e.g., "sackmann_data_*.csv").

    Returns:
        Optional[str]: The full path to the latest file, or None if no matching file found.
    """
    try:
        search_path = os.path.join(directory, pattern)
        abs_search_path = os.path.abspath(search_path)
        print(f"Searching for pattern: {abs_search_path}")
        list_of_files = glob.glob(search_path)

        if not list_of_files:
            print(f"No files found matching pattern '{pattern}' in directory '{directory}'.")
            return None

        # Find the latest file based on modification time (more reliable across systems)
        latest_file = max(list_of_files, key=os.path.getmtime)

        print(f"Found latest CSV file: {latest_file} (Full path: {os.path.abspath(latest_file)})")
        return latest_file
    except Exception as e:
        print(f"Error finding latest CSV file in '{directory}': {e}")
        traceback.print_exc()
        return None

def format_error_html(message: str) -> str:
    """Formats an error message as an HTML snippet with a specific CSS class."""
    print(f"Error generating table: {message}") # Log the error for debugging
    # Return HTML string wrapped in a div for styling
    return f'<div class="{ERROR_MESSAGE_CLASS}">{message} Check workflow logs for details.</div>'

def generate_html_table(csv_filepath: str) -> str:
    """
    Reads the CSV file and generates an HTML table string.
    Returns a styled error message string if any step fails.

    Args:
        csv_filepath (str): The path to the CSV file.

    Returns:
        str: An HTML string containing the data table or a styled error message.
    """
    abs_csv_filepath = os.path.abspath(csv_filepath)
    print(f"Attempting to read CSV: {abs_csv_filepath}")

    # 1. Check if file exists
    if not os.path.exists(csv_filepath):
         return format_error_html(f"Data file not found at {abs_csv_filepath}.")

    try:
        # 2. Check if file is empty
        if os.path.getsize(csv_filepath) == 0:
            return format_error_html(f"No match data available (data file is empty: {abs_csv_filepath}).")

        # 3. Read CSV into DataFrame
        df = pd.read_csv(csv_filepath)

        # 4. Check if DataFrame is empty after reading
        if df.empty:
            print(f"Warning: DataFrame is empty after reading {csv_filepath}.")
            return format_error_html("No match data available (DataFrame is empty after reading).")

        print("Preparing data for HTML table...")

        # 5. Drop timestamp column if present
        if 'ScrapeTimestampUTC' in df.columns:
             df_display = df.drop(columns=['ScrapeTimestampUTC']).copy()
             print("Dropped 'ScrapeTimestampUTC' column.")
        else:
             df_display = df.copy()
             print("Note: 'ScrapeTimestampUTC' column not found in CSV.")

        # 6. Define and select relevant columns
        columns_to_display = ['Player', 'Probability (%)', 'Decimal_Odds', 'Round', 'Tournament_URL']
        print(f"Desired columns: {columns_to_display}")
        print(f"Available columns in CSV: {df_display.columns.tolist()}")
        existing_columns = [col for col in columns_to_display if col in df_display.columns]

        if not existing_columns:
             return format_error_html(f"No relevant columns ({', '.join(columns_to_display)}) found in the data file.")
        if 'Player' not in existing_columns:
             return format_error_html("'Player' column is missing from the data file.")

        df_display = df_display[existing_columns].copy()
        print(f"Selected columns for display: {existing_columns}")

        # 7. Rename columns for display
        rename_map = {'Probability (%)': 'Probability','Decimal_Odds': 'Odds','Tournament_URL': 'Tournament'}
        rename_map_existing = {k: v for k, v in rename_map.items() if k in df_display.columns}
        if rename_map_existing:
            df_display.rename(columns=rename_map_existing, inplace=True)
            print(f"Renamed columns: {rename_map_existing}")

        # 8. Process Tournament URL for readability
        if 'Tournament' in df_display.columns:
            print("Processing 'Tournament' column...")
            try:
                 df_display['Tournament'] = df_display['Tournament'].apply(
                     lambda url: url.split('/')[-2].replace('-', ' ').title()
                     if isinstance(url, str) and '/' in url and len(url.split('/')) > 2
                     else 'Unknown Tournament'
                 )
                 print("Processed 'Tournament' column successfully.")
            except Exception as e:
                 print(f"Warning: Could not process Tournament URLs cleanly - {e}. Setting to 'Unknown'.")
                 df_display['Tournament'] = 'Unknown Tournament'

        # 9. Format Probability column
        if 'Probability' in df_display.columns:
            print("Processing 'Probability' column...")
            try:
                df_display['Probability'] = pd.to_numeric(df_display['Probability'], errors='coerce')
                if pd.api.types.is_numeric_dtype(df_display['Probability']) and not df_display['Probability'].isna().all():
                    df_display.sort_values(by='Probability', ascending=False, inplace=True, na_position='last')
                    print("Sorted by 'Probability'.")
                # Format *after* sorting
                df_display['Probability'] = df_display['Probability'].map('{:.1f}%'.format, na_action='ignore')
                print("Formatted 'Probability' column.")
            except Exception as e: print(f"Warning: Could not sort/format Probability column - {e}")

        # 10. Format Odds column
        if 'Odds' in df_display.columns:
             print("Processing 'Odds' column...")
             try:
                df_display['Odds'] = pd.to_numeric(df_display['Odds'], errors='coerce')
                df_display['Odds'] = df_display['Odds'].map('{:.2f}'.format, na_action='ignore')
                print("Formatted 'Odds' column.")
             except Exception as e: print(f"Warning: Could not format Odds column - {e}")

        # 11. Generate HTML table string
        print("Generating HTML string from DataFrame...")
        try:
            html_table = df_display.to_html(
                index=False,        # Don't include DataFrame index
                border=0,           # No border attribute on table tag
                classes='dataframe',# Add 'dataframe' class for styling
                escape=True,        # Escape HTML characters in data (important for security)
                na_rep='-'          # How to represent missing values (NaN)
            )
            # Basic validation of the generated HTML
            if "<table" not in html_table or "</table>" not in html_table:
                 print("Warning: pandas to_html did not return expected table tags.")
                 return format_error_html("Could not format data into a table.")
            print(f"HTML table string generated successfully (Length: {len(html_table)} chars).")
            return html_table # Success
        except Exception as e_html:
             print(f"Error during pandas to_html conversion: {e_html}")
             traceback.print_exc()
             return format_error_html(f"Failed during HTML conversion ({type(e_html).__name__}).")

    # Handle specific pandas error for empty/malformed CSV
    except pd.errors.EmptyDataError:
        return format_error_html(f"No match data available (file is empty or unreadable by pandas: {abs_csv_filepath}).")
    # Handle file not found error if somehow missed earlier
    except FileNotFoundError:
         return format_error_html(f"Data file not found at {abs_csv_filepath} (Error in generate_html_table).")
    # Catch any other unexpected errors during processing
    except Exception as e:
        print(f"Error processing CSV or generating HTML table: {e}")
        traceback.print_exc()
        return format_error_html(f"An unexpected error ({type(e).__name__}) occurred during table generation.")


def update_index_html(template_path: str, output_path: str, table_html_content: str):
    """
    Reads the template HTML, injects the generated table (or error message)
    and timestamp into specific placeholder locations, and writes the output HTML file.

    Args:
        template_path (str): Path to the source index.html template.
        output_path (str): Path where the updated index.html will be written.
        table_html_content (str): The HTML string for the data table or an error message.
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

        # Check if the comment placeholders exist BEFORE attempting replacement
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

            # Perform replacements using the correct comment placeholders
            print("Performing replacements...")
            content_with_table = template_content.replace(TABLE_PLACEHOLDER, table_html_content)
            final_content = content_with_table.replace(TIMESTAMP_PLACEHOLDER, last_updated_text)
            print("Replacements performed.")

            # Verify replacements happened (should always work if placeholders were found)
            if TABLE_PLACEHOLDER in final_content:
                 print("ERROR: Table placeholder remained after replacement! Check for unexpected characters or issues with replace function.")
            else:
                 print("Table placeholder replaced successfully.")
            if TIMESTAMP_PLACEHOLDER in final_content:
                 print("ERROR: Timestamp placeholder remained after replacement! Check for unexpected characters or issues with replace function.")
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
            print("ERROR: Placeholders not found in template. Writing original template content back to output file to prevent data loss.")
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(template_content) # Write original content back

    except FileNotFoundError:
        print(f"Error: Template file not found at {template_path}")
        # Avoid writing if template doesn't exist
    except Exception as e:
        print(f"Error updating index.html: {e}")
        traceback.print_exc()


# --- Main Execution Logic ---
if __name__ == "__main__":
    print("Starting page generation process...")
    print(f"Looking for latest CSV in: {os.path.abspath(DATA_DIR)}")
    print(f"Using template: {os.path.abspath(TEMPLATE_FILE)}")
    print(f"Outputting to: {os.path.abspath(OUTPUT_HTML_FILE)}")

    print("\nFinding latest data file...")
    latest_csv = find_latest_csv(DATA_DIR, CSV_PATTERN)

    table_html_content = "" # Initialize with empty string

    if latest_csv:
        print(f"\nGenerating HTML table from: {latest_csv}")
        # generate_html_table handles errors internally and returns HTML string (table or error div)
        table_html_content = generate_html_table(latest_csv)
    else:
        # If no CSV found, create the standard error message HTML
        print(f"\nNo CSV file found in {DATA_DIR}. Generating error message for HTML content.")
        table_html_content = format_error_html(f"Error: Could not find the latest data file ({CSV_PATTERN}) in {DATA_DIR}.")


    # Ensure table_html_content is definitely a string before proceeding
    if not isinstance(table_html_content, str):
        print("ERROR: table_html_content is not a string! Defaulting to an internal error message.")
        table_html_content = format_error_html("Internal Error: Failed to generate valid HTML content.")

    # Update the HTML file
    print(f"\nUpdating {OUTPUT_HTML_FILE}...")
    update_index_html(TEMPLATE_FILE, OUTPUT_HTML_FILE, table_html_content)

    print("\nPage generation process complete.")
