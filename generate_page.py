# generate_page.py (Reads matchup CSV, displays side-by-side, uses comment placeholders)

import pandas as pd
from datetime import datetime
import os
import glob
import pytz
import traceback
from typing import Optional, List
import html # Import standard html library for escaping

# --- Constants ---
# *** Use unique HTML comment placeholders for robust replacement ***
TABLE_PLACEHOLDER = "" # Use HTML comment as placeholder
TIMESTAMP_PLACEHOLDER = "" # Use HTML comment as placeholder

ERROR_MESSAGE_CLASS = "error-message"
DATA_DIR = "data_archive"
# Use the correct CSV pattern for matchup files
CSV_PATTERN = "sackmann_matchups_*.csv"
TEMPLATE_FILE = "index.html" # Assumes template is in the same dir as script
OUTPUT_HTML_FILE = "index.html" # Overwrite the template with the generated file

# Expected columns in the matchup CSV
EXPECTED_COLS = [
    'TournamentName', 'Round', 'Player1Name', 'Player2Name',
    'Player1_Match_Prob', 'Player2_Match_Prob',
    'Player1_Match_Odds', 'Player2_Match_Odds'
]

# --- Helper Functions ---

def find_latest_csv(directory: str, pattern: str) -> Optional[str]:
    """Finds the most recently modified CSV file matching the pattern."""
    try:
        # Construct the search path relative to the script's directory if DATA_DIR is relative
        script_dir = os.path.dirname(os.path.abspath(__file__))
        search_dir = os.path.join(script_dir, directory) # Ensure absolute path for glob
        search_path = os.path.join(search_dir, pattern)
        print(f"Searching for pattern: {search_path}")

        list_of_files = glob.glob(search_path)
        if not list_of_files:
            print(f"No files found matching pattern '{pattern}' in directory '{search_dir}'.")
            return None

        # Filter out directories if pattern accidentally matches them
        list_of_files = [f for f in list_of_files if os.path.isfile(f)]
        if not list_of_files:
             print(f"No *files* found matching pattern '{pattern}' in directory '{search_dir}'.")
             return None

        latest_file = max(list_of_files, key=os.path.getmtime)
        print(f"Found latest CSV file: {latest_file} (Full path: {os.path.abspath(latest_file)})")
        return latest_file
    except Exception as e:
        print(f"Error finding latest CSV file in '{directory}': {e}")
        traceback.print_exc() # Show full traceback for debugging
        return None

def format_error_html(message: str) -> str:
    """Formats an error message as an HTML snippet."""
    print(f"Error generating table: {message}")
    # Returns the message wrapped in a div with error styling
    # This will replace the TABLE_PLACEHOLDER if an error occurs
    return f'<div class="{ERROR_MESSAGE_CLASS}">{html.escape(message)} Check logs for details.</div>'

def generate_matchup_html_table(csv_filepath: str) -> str:
    """
    Reads the matchup CSV and generates an HTML table string
    with players displayed side-by-side.
    Returns a styled error message string if any step fails.
    """
    abs_csv_filepath = os.path.abspath(csv_filepath)
    print(f"Generating matchup table from: {abs_csv_filepath}")

    if not os.path.exists(csv_filepath):
         return format_error_html(f"Matchup data file not found: {abs_csv_filepath}")

    try:
        # Check for empty file before reading
        if os.path.getsize(csv_filepath) == 0:
            return format_error_html(f"Matchup data file is empty: {abs_csv_filepath}")

        df = pd.read_csv(csv_filepath)

        if df.empty:
            # Handle case where CSV has headers but no data rows
            return format_error_html(f"Matchup data file '{os.path.basename(csv_filepath)}' contains no match data.")

        # Validate necessary columns exist
        missing_cols = [col for col in EXPECTED_COLS if col not in df.columns]
        if missing_cols:
            return format_error_html(f"CSV ('{os.path.basename(csv_filepath)}') is missing required columns: {', '.join(missing_cols)}. Found: {df.columns.tolist()}")

        print(f"Read {len(df)} matchups. Preparing HTML...")

        # Select only expected columns for display to avoid issues with extra cols
        # Make a copy to avoid SettingWithCopyWarning
        df_display = df[EXPECTED_COLS].copy()

        # --- Formatting ---
        print("Formatting data...")
        # Format Probabilities to 1 decimal place %
        for col in ['Player1_Match_Prob', 'Player2_Match_Prob']:
            # Use pd.to_numeric with errors='coerce' to handle non-numeric values gracefully
            df_display[col] = pd.to_numeric(df_display[col], errors='coerce')
            # Apply formatting, map will skip NaNs automatically
            df_display[col] = df_display[col].map('{:.1f}%'.format)
        # Format Odds to 2 decimal places
        for col in ['Player1_Match_Odds', 'Player2_Match_Odds']:
            df_display[col] = pd.to_numeric(df_display[col], errors='coerce')
            df_display[col] = df_display[col].map('{:.2f}'.format)
        # Handle potential None/NaN values *after* formatting (replace None/NaN with '-')
        df_display.fillna('-', inplace=True)
        print("Data formatting complete.")

        # --- Sorting ---
        # Sort primarily by Tournament Name, then by Round (using custom order if needed)
        # For simplicity, standard alphabetical sort for Round is used here.
        # If a specific order like R128 -> R64 -> ... -> F is needed,
        # convert 'Round' to a categorical type with specified order.
        try:
            # Basic sort first
            df_display.sort_values(by=['TournamentName', 'Round'], inplace=True, na_position='last')
            print("Sorted matchups by Tournament and Round.")
        except Exception as e:
             print(f"Warning: Error during sorting: {e}") # Log but continue

        # --- Manual HTML Table Generation ---
        print("Generating HTML string manually...")
        html_rows: List[str] = []
        # Define headers exactly as they should appear
        headers = ["Player 1", "Prob", "Odds", "vs", "Player 2", "Prob", "Odds", "Round", "Tournament"]
        html_rows.append("<thead><tr>" + "".join(f"<th>{html.escape(h)}</th>" for h in headers) + "</tr></thead>")
        html_rows.append("<tbody>")

        for _, row in df_display.iterrows():
            # Use standard html.escape on all string data going into the table
            p1_name = html.escape(str(row.get('Player1Name', '')))
            p2_name = html.escape(str(row.get('Player2Name', '')))
            round_val = html.escape(str(row.get('Round', '')))
            tournament_val = html.escape(str(row.get('TournamentName', '')))
            # Get already formatted values (no need to escape these as they are numbers/percentages)
            p1_prob = row['Player1_Match_Prob']
            p1_odds = row['Player1_Match_Odds']
            p2_prob = row['Player2_Match_Prob']
            p2_odds = row['Player2_Match_Odds']

            # Build the HTML table row
            html_rows.append("<tr>"
                             f"<td>{p1_name}</td>"
                             # Apply text-align directly here for simplicity
                             f"<td style='text-align: right;'>{p1_prob}</td>"
                             f"<td style='text-align: right;'>{p1_odds}</td>"
                             "<td style='text-align: center; font-weight: bold;'>vs</td>"
                             f"<td>{p2_name}</td>"
                             f"<td style='text-align: right;'>{p2_prob}</td>"
                             f"<td style='text-align: right;'>{p2_odds}</td>"
                             f"<td>{round_val}</td>"
                             f"<td>{tournament_val}</td>"
                             "</tr>")
        html_rows.append("</tbody>")

        # Combine into a full table structure, adding the required class
        # Return ONLY the table string, not the surrounding div
        full_html_table = f'<table class="dataframe">{ "".join(html_rows) }</table>'
        print("HTML matchup table generated successfully.")
        return full_html_table

    except pd.errors.EmptyDataError:
        return format_error_html(f"Matchup data file is empty or invalid: {abs_csv_filepath}")
    except KeyError as e:
        print(f"Error generating matchup HTML table: Missing expected column {e}")
        traceback.print_exc()
        return format_error_html(f"Internal Error: Missing expected column '{e}' in data.")
    except Exception as e:
        print(f"Error generating matchup HTML table: {e}")
        traceback.print_exc() # Show full traceback for debugging
        # Return error message WITHOUT surrounding div, formatted for display
        return format_error_html(f"Unexpected error during HTML table generation: {type(e).__name__}")


def update_index_html(template_path: str, output_path: str, table_html_content: str):
    """Reads template, injects table and timestamp using comment placeholders."""
    # Determine paths relative to the script's location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    abs_template_path = os.path.join(script_dir, template_path)
    abs_output_path = os.path.join(script_dir, output_path) # Output in the same dir

    print(f"Updating '{os.path.basename(output_path)}' using template '{os.path.basename(template_path)}'...")
    print(f"Template absolute path: {abs_template_path}")
    print(f"Output absolute path: {abs_output_path}")

    try:
        print(f"Reading template file: {abs_template_path}")
        with open(abs_template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        print(f"Template content read successfully (Length: {len(template_content)} chars).")

        # *** Check for the correct COMMENT placeholders ***
        # Use repr() to show invisible characters in logs if placeholder not found
        print(f"Looking for table placeholder: {repr(TABLE_PLACEHOLDER)}")
        table_placeholder_found = TABLE_PLACEHOLDER in template_content
        print(f"Looking for timestamp placeholder: {repr(TIMESTAMP_PLACEHOLDER)}")
        timestamp_placeholder_found = TIMESTAMP_PLACEHOLDER in template_content

        if not table_placeholder_found:
            print(f"ERROR: Table placeholder {repr(TABLE_PLACEHOLDER)} NOT found in template '{template_path}'.")
        else:
            print(f"Table placeholder '{TABLE_PLACEHOLDER}' found.")

        if not timestamp_placeholder_found:
            print(f"ERROR: Timestamp placeholder {repr(TIMESTAMP_PLACEHOLDER)} NOT found in template '{template_path}'.")
        else:
            print(f"Timestamp placeholder '{TIMESTAMP_PLACEHOLDER}' found.")

        # Only proceed if placeholders were found
        if table_placeholder_found and timestamp_placeholder_found:
            # Generate timestamp *after* confirming placeholders exist
            update_time = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
            # Escape the timestamp string just in case, though unlikely to contain HTML chars
            last_updated_text = f"Last updated: {html.escape(update_time)}"

            print("Performing replacements...")
            # *** Replace the COMMENT placeholders ***
            # Replace table first
            content_with_table = template_content.replace(TABLE_PLACEHOLDER, table_html_content)
            # Then replace timestamp in the intermediate result
            final_content = content_with_table.replace(TIMESTAMP_PLACEHOLDER, last_updated_text)
            print("Replacements performed.")

            # Verification step
            replacement_successful = True
            if TABLE_PLACEHOLDER in final_content:
                print("ERROR: Table placeholder remained after replacement!")
                replacement_successful = False
            else:
                print("Table placeholder replaced successfully.")

            if TIMESTAMP_PLACEHOLDER in final_content:
                print("ERROR: Timestamp placeholder remained after replacement!")
                replacement_successful = False
            else:
                print("Last updated placeholder replaced successfully.")

            if replacement_successful:
                print(f"Writing updated content to: {abs_output_path}")
                with open(abs_output_path, 'w', encoding='utf-8') as f:
                    f.write(final_content)
                print(f"Successfully wrote updated content to {os.path.basename(output_path)}")
            else:
                 print("ERROR: One or more placeholders remained after replacement. Writing original content back to avoid corruption.")
                 # Optionally write original content back, or just log the error
                 # with open(abs_output_path, 'w', encoding='utf-8') as f:
                 #    f.write(template_content)

        else:
            print(f"ERROR: Required placeholders not found in template '{template_path}'. Cannot update HTML. Check the template file.")
            # Do NOT write the output file if placeholders are missing, leave the old one.

    except FileNotFoundError:
        print(f"CRITICAL ERROR: Template file not found at '{abs_template_path}'. Cannot generate page.")
        traceback.print_exc()
    except Exception as e:
        print(f"CRITICAL ERROR updating index.html: {e}")
        traceback.print_exc()


# --- Main Execution Logic ---
if __name__ == "__main__":
    print("Starting page generation process...")
    # Assume DATA_DIR, TEMPLATE_FILE, OUTPUT_HTML_FILE are relative to the script's location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_abs = os.path.join(script_dir, DATA_DIR)
    template_file_abs = os.path.join(script_dir, TEMPLATE_FILE)
    output_file_abs = os.path.join(script_dir, OUTPUT_HTML_FILE)

    print(f"Script directory: {script_dir}")
    print(f"Looking for latest CSV in: {data_dir_abs}")
    print(f"Using template: {template_file_abs}")
    print(f"Outputting to: {output_file_abs}")

    print("\nFinding latest data file...")
    # Pass the absolute path to the data directory
    latest_csv = find_latest_csv(data_dir_abs, CSV_PATTERN)

    table_html_content = "" # Initialize variable

    if latest_csv:
        print(f"\nGenerating HTML table from: {os.path.basename(latest_csv)}")
        # Call the matchup table generation function
        table_html_content = generate_matchup_html_table(latest_csv)
    else:
        print(f"\nNo CSV file found matching '{CSV_PATTERN}' in {data_dir_abs}. Generating error message.")
        # Generate error message HTML (no outer div needed here, format_error_html adds it)
        table_html_content = format_error_html(f"Error: Could not find latest data file ({CSV_PATTERN}) in {DATA_DIR}.")

    # Ensure table_html_content is always a string before passing to update_index_html
    if not isinstance(table_html_content, str):
        print("ERROR: table_html_content is not a string! Defaulting to an error message.")
        table_html_content = format_error_html("Internal Error: Failed to generate valid HTML content.")

    print(f"\nUpdating {OUTPUT_HTML_FILE} using template {TEMPLATE_FILE}...")
    # Pass relative paths to update_index_html, it will make them absolute internally
    update_index_html(TEMPLATE_FILE, OUTPUT_HTML_FILE, table_html_content)

    print("\nPage generation process complete.")
