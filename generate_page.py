# generate_page.py (Targets specific empty divs in the selected index.html)
# WARNING: This approach is fragile. Using comment placeholders is recommended.

import pandas as pd
from datetime import datetime
import os
import glob
import pytz
import traceback
from typing import Optional, List

# --- Constants ---
# WARNING: These placeholders target specific empty divs based on the
# currently selected index.html artifact. Fragile!
TABLE_PLACEHOLDER = '<div class="table-container">\n    </div>'
TIMESTAMP_PLACEHOLDER = '<div class="last-updated">\n    </div>'

ERROR_MESSAGE_CLASS = "error-message"
DATA_DIR = "data_archive"
# Use the correct CSV pattern for matchup files
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
        list_of_files = glob.glob(search_path)
        if not list_of_files:
            print(f"No files found: {directory}/{pattern}")
            return None
        latest_file = max(list_of_files, key=os.path.getmtime)
        print(f"Found latest CSV: {latest_file}")
        return latest_file
    except Exception as e:
        print(f"Error finding latest CSV: {e}")
        # traceback.print_exc() # Keep commented unless debugging
        return None

def format_error_html(message: str) -> str:
    """Formats an error message as an HTML snippet."""
    print(f"Error generating table: {message}")
    # Returns the message wrapped in a div with error styling
    # Also wrap in the container div structure to replace the placeholder
    return f'<div class="table-container"><div class="{ERROR_MESSAGE_CLASS}">{message} Check logs.</div></div>'

def generate_matchup_html_table(csv_filepath: str) -> str:
    """
    Reads the matchup CSV and generates an HTML table showing players side-by-side.
    Returns a styled error message string (wrapped in container div) if any step fails.
    """
    print(f"Generating matchup table from: {csv_filepath}")
    if not os.path.exists(csv_filepath):
         return format_error_html(f"Matchup data file not found: {csv_filepath}")
    try:
        if os.path.getsize(csv_filepath) == 0:
            return format_error_html(f"Matchup data file is empty: {csv_filepath}")

        df = pd.read_csv(csv_filepath)
        if df.empty:
            return format_error_html("Matchup data file read but DataFrame is empty.")

        # Validate necessary columns exist
        missing_cols = [col for col in EXPECTED_COLS if col not in df.columns]
        if missing_cols:
            return format_error_html(f"CSV is missing required columns: {', '.join(missing_cols)}. Found: {df.columns.tolist()}")

        print(f"Read {len(df)} matchups. Preparing HTML...")
        df_display = df[EXPECTED_COLS].copy()

        # --- Formatting ---
        for col in ['Player1_Match_Prob', 'Player2_Match_Prob']:
            df_display[col] = pd.to_numeric(df_display[col], errors='coerce')
            df_display[col] = df_display[col].map('{:.1f}%'.format, na_action='ignore')
        for col in ['Player1_Match_Odds', 'Player2_Match_Odds']:
            df_display[col] = pd.to_numeric(df_display[col], errors='coerce')
            df_display[col] = df_display[col].map('{:.2f}'.format, na_action='ignore')
        df_display.fillna('-', inplace=True) # Replace any remaining NaNs

        # --- Sorting ---
        try: df.sort_values(by=['TournamentName', 'Round'], inplace=True, na_position='last')
        except: print("Warning: Could not sort data.") # Non-critical

        # --- Manual HTML Table Generation ---
        html_rows: List[str] = []
        headers = ["Player 1", "Prob", "Odds", "", "Player 2", "Prob", "Odds", "Round", "Tournament"]
        html_rows.append("<thead><tr>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr></thead>")
        html_rows.append("<tbody>")

        for _, row in df_display.iterrows():
            # Escape text data
            p1_name = pd.io.formats.html.escape(str(row.get('Player1Name', '')))
            p2_name = pd.io.formats.html.escape(str(row.get('Player2Name', '')))
            round_val = pd.io.formats.html.escape(str(row.get('Round', '')))
            tournament_val = pd.io.formats.html.escape(str(row.get('TournamentName', '')))
            # Add other formatted fields
            p1_prob = row['Player1_Match_Prob']
            p1_odds = row['Player1_Match_Odds']
            p2_prob = row['Player2_Match_Prob']
            p2_odds = row['Player2_Match_Odds']

            # Build the HTML table row
            html_rows.append("<tr>"
                             f"<td>{p1_name}</td>"
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

        full_html_table = f'<table class="dataframe">{ "".join(html_rows) }</table>'
        print("HTML matchup table generated successfully.")
        # Wrap table in the container div structure for replacement
        return f'<div class="table-container">{full_html_table}</div>'

    except Exception as e:
        print(f"Error generating matchup HTML table: {e}")
        traceback.print_exc()
        return format_error_html(f"Unexpected error during HTML table generation: {type(e).__name__}")


def update_index_html(template_path: str, output_path: str, table_html_content: str):
    """Reads template, injects content into specific empty div placeholders, writes output."""
    print(f"Updating '{output_path}' using template '{template_path}'...")
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()

        final_content = template_content # Start with original

        # Replace Table Div using the exact (fragile) placeholder
        if TABLE_PLACEHOLDER in final_content:
            # Replace the entire empty div string with the generated content
            # (which already includes the surrounding div)
            final_content = final_content.replace(TABLE_PLACEHOLDER, table_html_content, 1)
            print("Table placeholder replaced.")
        else:
            print(f"ERROR: Table placeholder ('{repr(TABLE_PLACEHOLDER)}') was NOT found in template.")

        # Replace Timestamp Div using the exact (fragile) placeholder
        if TIMESTAMP_PLACEHOLDER in final_content:
            update_time = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
            # Inject the timestamp *inside* the div structure
            last_updated_html = f'<div class="last-updated">Last updated: {update_time}</div>'
            final_content = final_content.replace(TIMESTAMP_PLACEHOLDER, last_updated_html, 1)
            print("Timestamp placeholder replaced.")
        else:
             print(f"ERROR: Timestamp placeholder ('{repr(TIMESTAMP_PLACEHOLDER)}') was NOT found in template.")

        # Write the result
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(final_content)
        print(f"Successfully wrote updated content to {output_path}")

    except FileNotFoundError:
        print(f"Error: Template file not found at {template_path}")
    except Exception as e:
        print(f"Error updating index.html: {e}")
        traceback.print_exc()


# --- Main Execution Logic ---
if __name__ == "__main__":
    print("Starting page generation process...")

    latest_csv = find_latest_csv(DATA_DIR, CSV_PATTERN)

    if latest_csv:
        # generate_matchup_html_table returns the full div content or an error div
        table_html_content = generate_matchup_html_table(latest_csv)
    else:
        # Format error message and wrap it in the container div structure
        error_message = format_error_html(f"Error: Could not find latest data file ({CSV_PATTERN}) in {DATA_DIR}.")
        table_html_content = f'<div class="table-container">{error_message}</div>' # Wrap error

    if not isinstance(table_html_content, str): # Should always be string now
        error_message = format_error_html("Internal Error: Failed to generate valid HTML content.")
        table_html_content = f'<div class="table-container">{error_message}</div>' # Wrap error

    update_index_html(TEMPLATE_FILE, OUTPUT_HTML_FILE, table_html_content)

    print("Page generation process complete.")
