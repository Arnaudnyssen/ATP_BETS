# generate_page.py (Simplified: Uses pandas.to_html and comment placeholders)

import pandas as pd
from datetime import datetime
import os
import glob
import pytz
import traceback
from typing import Optional, List
import html # Still needed for escaping timestamp

# --- Constants ---
# *** Use unique HTML comment placeholders ***
TABLE_PLACEHOLDER = "``"
TIMESTAMP_PLACEHOLDER = "``"

ERROR_MESSAGE_CLASS = "error-message" # For displaying errors via format_error_html
DATA_DIR = "data_archive"
CSV_PATTERN = "sackmann_matchups_*.csv"
TEMPLATE_FILE = "index.html"
OUTPUT_HTML_FILE = "index.html" # Overwrite the template

# Columns to display in the final table
# Define the desired order and subset of columns
DISPLAY_COLS_ORDERED = [
    'Player1Name', 'Player1_Match_Prob', 'Player1_Match_Odds',
    'Player2Name', 'Player2_Match_Prob', 'Player2_Match_Odds',
    'Round', 'TournamentName'
]
# Define user-friendly headers for the table
DISPLAY_HEADERS = [
    "Player 1", "P1 Prob", "P1 Odds",
    "Player 2", "P2 Prob", "P2 Odds",
    "Round", "Tournament"
]

# --- Helper Functions ---

def find_latest_csv(directory: str, pattern: str) -> Optional[str]:
    """Finds the most recently modified CSV file matching the pattern."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        search_dir = os.path.join(script_dir, directory)
        search_path = os.path.join(search_dir, pattern)
        print(f"Searching for pattern: {search_path}")
        list_of_files = glob.glob(search_path)
        if not list_of_files:
            print(f"No files found matching pattern '{pattern}' in directory '{search_dir}'.")
            return None
        list_of_files = [f for f in list_of_files if os.path.isfile(f)]
        if not list_of_files:
             print(f"No *files* found matching pattern '{pattern}' in directory '{search_dir}'.")
             return None
        latest_file = max(list_of_files, key=os.path.getmtime)
        print(f"Found latest CSV file: {latest_file} (Full path: {os.path.abspath(latest_file)})")
        return latest_file
    except Exception as e:
        print(f"Error finding latest CSV file in '{directory}': {e}")
        traceback.print_exc()
        return None

def format_error_html(message: str) -> str:
    """Formats an error message as an HTML snippet to be inserted."""
    print(f"Error generating table: {message}")
    # Returns the message wrapped in a div with error styling
    return f'<div class="{ERROR_MESSAGE_CLASS}">{html.escape(message)} Check logs for details.</div>'

def generate_html_table_from_csv(csv_filepath: str) -> str:
    """
    Reads CSV, filters completed matches, formats, sorts,
    and generates an HTML table string using pandas.to_html().
    Returns error HTML on failure.
    """
    abs_csv_filepath = os.path.abspath(csv_filepath)
    print(f"Generating HTML table from: {abs_csv_filepath} using pandas.to_html()")

    if not os.path.exists(csv_filepath):
         return format_error_html(f"Data file not found: {abs_csv_filepath}")

    try:
        if os.path.getsize(csv_filepath) == 0:
            return format_error_html(f"Data file is empty: {abs_csv_filepath}")

        df = pd.read_csv(csv_filepath)

        if df.empty:
            return format_error_html(f"Data file '{os.path.basename(csv_filepath)}' contains no match data.")

        print(f"Read {len(df)} rows initially.")

        # --- Filtering ---
        # Ensure probability columns are numeric for filtering
        df['Player1_Match_Prob'] = pd.to_numeric(df['Player1_Match_Prob'], errors='coerce')
        df['Player2_Match_Prob'] = pd.to_numeric(df['Player2_Match_Prob'], errors='coerce')

        # Filter out rows where either probability is exactly 0 or 100
        original_count = len(df)
        df_filtered = df[
            (df['Player1_Match_Prob'] != 0.0) & (df['Player1_Match_Prob'] != 100.0) &
            (df['Player2_Match_Prob'] != 0.0) & (df['Player2_Match_Prob'] != 100.0)
        ].copy() # Use .copy() to avoid SettingWithCopyWarning
        filtered_count = len(df_filtered)
        print(f"Filtered out {original_count - filtered_count} rows (Prob = 0% or 100%). {filtered_count} rows remain.")

        if df_filtered.empty:
            return format_error_html("No upcoming matches found after filtering.")

        # --- Formatting for Display ---
        print("Formatting data for display...")
        # Format Probabilities (apply after filtering)
        df_filtered['Player1_Match_Prob'] = df_filtered['Player1_Match_Prob'].map('{:.1f}%'.format, na_action='ignore')
        df_filtered['Player2_Match_Prob'] = df_filtered['Player2_Match_Prob'].map('{:.1f}%'.format, na_action='ignore')
        # Format Odds
        df_filtered['Player1_Match_Odds'] = pd.to_numeric(df_filtered['Player1_Match_Odds'], errors='coerce').map('{:.2f}'.format, na_action='ignore')
        df_filtered['Player2_Match_Odds'] = pd.to_numeric(df_filtered['Player2_Match_Odds'], errors='coerce').map('{:.2f}'.format, na_action='ignore')
        # Handle potential NaNs introduced by formatting/filtering
        df_filtered.fillna('-', inplace=True)
        print("Data formatting complete.")

        # --- Sorting ---
        try:
            df_filtered.sort_values(by=['TournamentName', 'Round'], inplace=True, na_position='last')
            print("Sorted matchups by Tournament and Round.")
        except Exception as e:
             print(f"Warning: Error during sorting: {e}")

        # --- Select and Rename Columns for Display ---
        # Ensure all display columns exist before selecting
        missing_display_cols = [col for col in DISPLAY_COLS_ORDERED if col not in df_filtered.columns]
        if missing_display_cols:
            return format_error_html(f"Data is missing columns needed for display: {', '.join(missing_display_cols)}")

        df_display = df_filtered[DISPLAY_COLS_ORDERED]
        # Assign the user-friendly headers
        df_display.columns = DISPLAY_HEADERS

        # --- Generate HTML using pandas ---
        print("Generating HTML table using df.to_html()...")
        html_table = df_display.to_html(
            classes='dataframe', # Apply the CSS class used in index.html
            index=False,         # Don't include the DataFrame index
            escape=True,         # Escape HTML characters in data (important for security)
            na_rep='-',          # Representation for missing values
            border=0             # Remove default pandas border attribute
        )

        # Quick check to ensure table generation didn't fail silently
        if not html_table or not isinstance(html_table, str):
             return format_error_html("Failed to generate HTML table using pandas.to_html().")

        print("HTML table generated successfully via pandas.")
        return html_table

    except pd.errors.EmptyDataError:
        return format_error_html(f"Data file is empty or invalid: {abs_csv_filepath}")
    except KeyError as e:
        print(f"Error generating HTML table: Missing expected column {e}")
        traceback.print_exc()
        return format_error_html(f"Internal Error: Missing expected column '{e}' in data.")
    except Exception as e:
        print(f"Error generating HTML table: {e}")
        traceback.print_exc()
        return format_error_html(f"Unexpected error during HTML table generation: {type(e).__name__}")


def update_index_html(template_path: str, output_path: str, table_html_content: str):
    """Reads template, injects table and timestamp using comment placeholders."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    abs_template_path = os.path.join(script_dir, template_path)
    abs_output_path = os.path.join(script_dir, output_path)

    print(f"Updating '{os.path.basename(output_path)}' using template '{os.path.basename(template_path)}'...")
    print(f"Template absolute path: {abs_template_path}")
    print(f"Output absolute path: {abs_output_path}")

    try:
        print(f"Reading template file: {abs_template_path}")
        with open(abs_template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        print(f"Template content read successfully (Length: {len(template_content)} chars).")

        # *** Check for the correct COMMENT placeholders ***
        print(f"Looking for table placeholder: {repr(TABLE_PLACEHOLDER)}")
        table_placeholder_found = TABLE_PLACEHOLDER in template_content
        print(f"Looking for timestamp placeholder: {repr(TIMESTAMP_PLACEHOLDER)}")
        timestamp_placeholder_found = TIMESTAMP_PLACEHOLDER in template_content

        if not table_placeholder_found:
            print(f"CRITICAL ERROR: Table placeholder {repr(TABLE_PLACEHOLDER)} NOT found in template '{template_path}'. Cannot insert table.")
        else:
            print(f"Table placeholder '{TABLE_PLACEHOLDER}' found.")

        if not timestamp_placeholder_found:
            print(f"CRITICAL ERROR: Timestamp placeholder {repr(TIMESTAMP_PLACEHOLDER)} NOT found in template '{template_path}'. Cannot insert timestamp.")
        else:
            print(f"Timestamp placeholder '{TIMESTAMP_PLACEHOLDER}' found.")

        # Only proceed if BOTH placeholders were found
        if table_placeholder_found and timestamp_placeholder_found:
            update_time = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
            last_updated_text = f"Last updated: {html.escape(update_time)}"

            print("Performing replacements...")
            # Replace placeholders
            content_with_table = template_content.replace(TABLE_PLACEHOLDER, table_html_content)
            final_content = content_with_table.replace(TIMESTAMP_PLACEHOLDER, last_updated_text)
            print("Replacements performed.")

            # Verification
            replacement_successful = True
            if TABLE_PLACEHOLDER in final_content and table_html_content != TABLE_PLACEHOLDER: # Check if it's still the placeholder itself
                print("ERROR: Table placeholder possibly remained after replacement!")
                replacement_successful = False
            else:
                print("Table placeholder replaced successfully.")

            if TIMESTAMP_PLACEHOLDER in final_content and last_updated_text != TIMESTAMP_PLACEHOLDER:
                print("ERROR: Timestamp placeholder possibly remained after replacement!")
                replacement_successful = False
            else:
                print("Timestamp placeholder replaced successfully.")

            if replacement_successful:
                print(f"Writing updated content to: {abs_output_path}")
                with open(abs_output_path, 'w', encoding='utf-8') as f:
                    f.write(final_content)
                print(f"Successfully wrote updated content to {os.path.basename(output_path)}")
            else:
                 print("ERROR: One or more placeholders may have remained after replacement. Check output file.")
                 # Still write the file as *some* replacement might have worked
                 print(f"Writing potentially incomplete content to: {abs_output_path}")
                 with open(abs_output_path, 'w', encoding='utf-8') as f:
                     f.write(final_content)

        else:
            print(f"ERROR: Required placeholders not found in template '{template_path}'. HTML file not updated.")
            # Do NOT write the output file if placeholders are missing

    except FileNotFoundError:
        print(f"CRITICAL ERROR: Template file not found at '{abs_template_path}'. Cannot generate page.")
        traceback.print_exc()
    except Exception as e:
        print(f"CRITICAL ERROR updating index.html: {e}")
        traceback.print_exc()


# --- Main Execution Logic ---
if __name__ == "__main__":
    print("Starting page generation process (Simplified with to_html)...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_abs = os.path.join(script_dir, DATA_DIR)
    template_file_abs = os.path.join(script_dir, TEMPLATE_FILE)
    output_file_abs = os.path.join(script_dir, OUTPUT_HTML_FILE)

    print(f"Script directory: {script_dir}")
    print(f"Looking for latest CSV in: {data_dir_abs}")
    print(f"Using template: {template_file_abs}")
    print(f"Outputting to: {output_file_abs}")

    print("\nFinding latest data file...")
    latest_csv = find_latest_csv(data_dir_abs, CSV_PATTERN)

    table_html_content = ""

    if latest_csv:
        print(f"\nGenerating HTML table from: {os.path.basename(latest_csv)}")
        # Use the new function leveraging pandas.to_html()
        table_html_content = generate_html_table_from_csv(latest_csv)
    else:
        print(f"\nNo CSV file found matching '{CSV_PATTERN}' in {data_dir_abs}. Generating error message.")
        table_html_content = format_error_html(f"Error: Could not find latest data file ({CSV_PATTERN}) in {DATA_DIR}.")

    if not isinstance(table_html_content, str):
        print("ERROR: table_html_content is not a string! Defaulting.")
        table_html_content = format_error_html("Internal Error: Failed to generate valid HTML content.")

    print(f"\nUpdating {OUTPUT_HTML_FILE} using template {TEMPLATE_FILE}...")
    # Pass relative paths; function makes them absolute
    update_index_html(TEMPLATE_FILE, OUTPUT_HTML_FILE, table_html_content)

    print("\nPage generation process complete.")

