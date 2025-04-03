# generate_page.py (Reads matchup CSV and displays side-by-side)

import pandas as pd
from datetime import datetime
import os
import glob
import pytz
import traceback
from typing import Optional, List

# --- Constants ---
# Use comment placeholders for robust replacement
TABLE_PLACEHOLDER = ""
TIMESTAMP_PLACEHOLDER = ""

ERROR_MESSAGE_CLASS = "error-message"
DATA_DIR = "data_archive"
# *** Use the correct CSV pattern for matchup files ***
CSV_PATTERN = "sackmann_matchups_*.csv"
TEMPLATE_FILE = "index.html"
OUTPUT_HTML_FILE = "index.html"

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
        search_path = os.path.join(directory, pattern)
        abs_search_path = os.path.abspath(search_path)
        print(f"Searching for pattern: {abs_search_path}")
        list_of_files = glob.glob(search_path)
        if not list_of_files:
            print(f"No files found matching pattern '{pattern}' in directory '{directory}'.")
            return None
        latest_file = max(list_of_files, key=os.path.getmtime)
        print(f"Found latest CSV file: {latest_file} (Full path: {os.path.abspath(latest_file)})")
        return latest_file
    except Exception as e:
        print(f"Error finding latest CSV file in '{directory}': {e}")
        traceback.print_exc()
        return None

def format_error_html(message: str) -> str:
    """Formats an error message as an HTML snippet."""
    print(f"Error generating table: {message}")
    return f'<div class="{ERROR_MESSAGE_CLASS}">{message} Check logs.</div>'

def generate_html_table(csv_filepath: str) -> str:
    """
    Reads the matchup CSV and generates an HTML table string
    with players displayed side-by-side.
    """
    abs_csv_filepath = os.path.abspath(csv_filepath)
    print(f"Generating matchup table from: {abs_csv_filepath}")

    if not os.path.exists(csv_filepath):
         return format_error_html(f"Matchup data file not found: {abs_csv_filepath}")

    try:
        if os.path.getsize(csv_filepath) == 0:
            return format_error_html(f"Matchup data file is empty: {abs_csv_filepath}")

        df = pd.read_csv(csv_filepath)

        if df.empty:
            return format_error_html("Matchup data file read but DataFrame is empty.")

        # Validate necessary columns exist
        missing_cols = [col for col in EXPECTED_COLS if col not in df.columns]
        if missing_cols:
            return format_error_html(f"CSV ('{csv_filepath}') is missing required columns: {', '.join(missing_cols)}")

        print(f"Read {len(df)} matchups. Preparing HTML...")

        # --- Custom HTML Table Generation ---
        html_rows: List[str] = []
        # Define table headers for the new layout
        headers = ["Player 1", "P1 Prob", "P1 Odds", "vs", "Player 2", "P2 Prob", "P2 Odds", "Round", "Tournament"]
        html_rows.append("<thead><tr>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr></thead>")
        html_rows.append("<tbody>")

        # Sort by tournament then round (optional, but improves readability)
        try:
            # Attempt to sort. Handle potential errors if columns don't exist as expected.
            df.sort_values(by=['TournamentName', 'Round'], inplace=True, na_position='last')
            print("Sorted matchups by Tournament and Round.")
        except KeyError as e:
            print(f"Warning: Could not sort by 'TournamentName' or 'Round' - column missing? {e}")
        except Exception as e:
             print(f"Warning: Error during sorting: {e}")


        # Iterate through DataFrame rows (each row is a match)
        for _, row in df.iterrows():
            # Format data for display, handling potential None/NaN or unexpected types
            try:
                p1_prob_str = f"{float(row['Player1_Match_Prob']):.1f}%" if pd.notna(row['Player1_Match_Prob']) else "-"
            except (ValueError, TypeError): p1_prob_str = "-"
            try:
                p1_odds_str = f"{float(row['Player1_Match_Odds']):.2f}" if pd.notna(row['Player1_Match_Odds']) else "-"
            except (ValueError, TypeError): p1_odds_str = "-"
            try:
                p2_prob_str = f"{float(row['Player2_Match_Prob']):.1f}%" if pd.notna(row['Player2_Match_Prob']) else "-"
            except (ValueError, TypeError): p2_prob_str = "-"
            try:
                p2_odds_str = f"{float(row['Player2_Match_Odds']):.2f}" if pd.notna(row['Player2_Match_Odds']) else "-"
            except (ValueError, TypeError): p2_odds_str = "-"

            # Escape player names and other text data to prevent HTML injection
            p1_name = pd.io.formats.html.escape(str(row.get('Player1Name', ''))) # Use .get for safety
            p2_name = pd.io.formats.html.escape(str(row.get('Player2Name', '')))
            round_val = pd.io.formats.html.escape(str(row.get('Round', '')))
            tournament_val = pd.io.formats.html.escape(str(row.get('TournamentName', '')))

            # Build the HTML table row
            html_rows.append("<tr>")
            html_rows.append(f"<td>{p1_name}</td>")
            html_rows.append(f"<td style='text-align: right;'>{p1_prob_str}</td>")
            html_rows.append(f"<td style='text-align: right;'>{p1_odds_str}</td>")
            html_rows.append("<td style='text-align: center; font-weight: bold;'>vs</td>")
            html_rows.append(f"<td>{p2_name}</td>")
            html_rows.append(f"<td style='text-align: right;'>{p2_prob_str}</td>")
            html_rows.append(f"<td style='text-align: right;'>{p2_odds_str}</td>")
            html_rows.append(f"<td>{round_val}</td>")
            html_rows.append(f"<td>{tournament_val}</td>")
            html_rows.append("</tr>")

        html_rows.append("</tbody>")

        # Combine into a full table structure
        full_html_table = f'<table class="dataframe">{ "".join(html_rows) }</table>'
        print("HTML matchup table generated successfully.")
        return full_html_table

    except Exception as e:
        print(f"Error generating matchup HTML table: {e}")
        traceback.print_exc()
        return format_error_html(f"Unexpected error during HTML table generation: {type(e).__name__}")


def update_index_html(template_path: str, output_path: str, table_html_content: str):
    """Reads template, injects table and timestamp using comment placeholders."""
    abs_template_path = os.path.abspath(template_path)
    abs_output_path = os.path.abspath(output_path)
    print(f"Updating '{abs_output_path}' using template '{abs_template_path}'...")

    try:
        print(f"Reading template file: {abs_template_path}")
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        print(f"Template content read successfully (Length: {len(template_content)} chars).")

        # Check for comment placeholders
        table_placeholder_found = TABLE_PLACEHOLDER in template_content
        timestamp_placeholder_found = TIMESTAMP_PLACEHOLDER in template_content

        # Use repr() to show invisible characters in logs if placeholder not found
        if not table_placeholder_found: print(f"ERROR: Table placeholder {repr(TABLE_PLACEHOLDER)} NOT found in template.")
        else: print(f"Table placeholder '{TABLE_PLACEHOLDER}' found.")

        if not timestamp_placeholder_found: print(f"ERROR: Timestamp placeholder {repr(TIMESTAMP_PLACEHOLDER)} NOT found in template.")
        else: print(f"Timestamp placeholder '{TIMESTAMP_PLACEHOLDER}' found.")

        # Only proceed if placeholders were found
        if table_placeholder_found and timestamp_placeholder_found:
            update_time = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
            last_updated_text = f"Last updated: {update_time}"

            print("Performing replacements...")
            content_with_table = template_content.replace(TABLE_PLACEHOLDER, table_html_content)
            final_content = content_with_table.replace(TIMESTAMP_PLACEHOLDER, last_updated_text)
            print("Replacements performed.")

            # Verify replacements
            if TABLE_PLACEHOLDER in final_content: print("ERROR: Table placeholder remained after replacement!")
            else: print("Table placeholder replaced successfully.")
            if TIMESTAMP_PLACEHOLDER in final_content: print("ERROR: Timestamp placeholder remained after replacement!")
            else: print("Last updated placeholder replaced successfully.")

            print(f"Writing updated content to: {abs_output_path}")
            with open(output_path, 'w', encoding='utf-8') as f: f.write(final_content)
            print(f"Successfully wrote updated content to {output_path}")
        else:
            print("ERROR: Placeholders not found in template. Writing original template content back.")
            with open(output_path, 'w', encoding='utf-8') as f: f.write(template_content)

    except FileNotFoundError: print(f"Error: Template file not found at {template_path}")
    except Exception as e: print(f"Error updating index.html: {e}"); traceback.print_exc()


# --- Main Execution Logic ---
if __name__ == "__main__":
    print("Starting page generation process...")
    print(f"Looking for latest CSV in: {os.path.abspath(DATA_DIR)}")
    print(f"Using template: {os.path.abspath(TEMPLATE_FILE)}")
    print(f"Outputting to: {os.path.abspath(OUTPUT_HTML_FILE)}")

    print("\nFinding latest data file...")
    latest_csv = find_latest_csv(DATA_DIR, CSV_PATTERN) # Uses updated pattern

    table_html_content = ""

    if latest_csv:
        print(f"\nGenerating HTML table from: {latest_csv}")
        table_html_content = generate_html_table(latest_csv) # Calls updated function
    else:
        print(f"\nNo CSV file found in {DATA_DIR}. Generating error message.")
        table_html_content = format_error_html(f"Error: Could not find latest data file ({CSV_PATTERN}) in {DATA_DIR}.")

    if not isinstance(table_html_content, str):
        print("ERROR: table_html_content is not a string! Defaulting.")
        table_html_content = format_error_html("Internal Error: Failed to generate valid HTML.")

    print(f"\nUpdating {OUTPUT_HTML_FILE}...")
    # This relies on TEMPLATE_FILE (index.html) having comment placeholders
    update_index_html(TEMPLATE_FILE, OUTPUT_HTML_FILE, table_html_content)

    print("\nPage generation process complete.")
