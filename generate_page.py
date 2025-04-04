# generate_page.py (Generates full HTML page, avoiding template/placeholders)

import pandas as pd
from datetime import datetime
import os
import glob
import pytz
import traceback
from typing import Optional, List
import html # Still needed for escaping timestamp

# --- Constants ---
# No template/placeholder constants needed anymore

ERROR_MESSAGE_CLASS = "error-message" # For displaying errors in the generated HTML
DATA_DIR = "data_archive"
CSV_PATTERN = "sackmann_matchups_*.csv"
# TEMPLATE_FILE = "index.html" # No longer used
OUTPUT_HTML_FILE = "index.html" # File to overwrite with generated content

# Columns to display in the final table
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

def format_error_html_for_table(message: str) -> str:
    """Formats an error message as an HTML snippet to be used INSTEAD of the table."""
    print(f"Error generating table: {message}")
    # Returns the message wrapped in a div with error styling
    # This will be placed where the table would normally go
    return f'<div class="{ERROR_MESSAGE_CLASS}" style="padding: 20px;">{html.escape(message)} Check logs for details.</div>'

def generate_html_table_from_csv(csv_filepath: str) -> str:
    """
    Reads CSV, filters, formats, sorts, and generates an HTML table string.
    Returns error HTML string on failure.
    """
    abs_csv_filepath = os.path.abspath(csv_filepath)
    print(f"Generating HTML table from: {abs_csv_filepath} using pandas.to_html()")

    if not os.path.exists(csv_filepath):
         return format_error_html_for_table(f"Data file not found: {abs_csv_filepath}")

    try:
        if os.path.getsize(csv_filepath) == 0:
            return format_error_html_for_table(f"Data file is empty: {abs_csv_filepath}")

        df = pd.read_csv(csv_filepath)

        if df.empty:
            return format_error_html_for_table(f"Data file '{os.path.basename(csv_filepath)}' contains no match data.")

        print(f"Read {len(df)} rows initially.")

        # --- Filtering ---
        df['Player1_Match_Prob'] = pd.to_numeric(df['Player1_Match_Prob'], errors='coerce')
        df['Player2_Match_Prob'] = pd.to_numeric(df['Player2_Match_Prob'], errors='coerce')
        original_count = len(df)
        df_filtered = df[
            (df['Player1_Match_Prob'].notna()) & (df['Player1_Match_Prob'] != 0.0) & (df['Player1_Match_Prob'] != 100.0) &
            (df['Player2_Match_Prob'].notna()) & (df['Player2_Match_Prob'] != 0.0) & (df['Player2_Match_Prob'] != 100.0)
        ].copy()
        filtered_count = len(df_filtered)
        print(f"Filtered out {original_count - filtered_count} rows (Prob = 0%, 100%, or NaN). {filtered_count} rows remain.")

        if df_filtered.empty:
            if original_count > 0: return format_error_html_for_table("No upcoming matches found (all matches appear completed or have invalid data).")
            else: return format_error_html_for_table("No upcoming matches found after filtering (original file might have been empty or only contained headers).")

        # --- Formatting, Sorting, Column Selection (same as before) ---
        print("Formatting data for display...")
        df_filtered['Player1_Match_Prob'] = df_filtered['Player1_Match_Prob'].map('{:.1f}%'.format, na_action='ignore')
        df_filtered['Player2_Match_Prob'] = df_filtered['Player2_Match_Prob'].map('{:.1f}%'.format, na_action='ignore')
        df_filtered['Player1_Match_Odds'] = pd.to_numeric(df_filtered['Player1_Match_Odds'], errors='coerce').map('{:.2f}'.format, na_action='ignore')
        df_filtered['Player2_Match_Odds'] = pd.to_numeric(df_filtered['Player2_Match_Odds'], errors='coerce').map('{:.2f}'.format, na_action='ignore')
        df_filtered.fillna('-', inplace=True)
        print("Data formatting complete.")

        try:
            df_filtered.sort_values(by=['TournamentName', 'Round'], inplace=True, na_position='last')
            print("Sorted matchups by Tournament and Round.")
        except Exception as e:
             print(f"Warning: Error during sorting: {e}")

        missing_display_cols = [col for col in DISPLAY_COLS_ORDERED if col not in df_filtered.columns]
        if missing_display_cols:
            return format_error_html_for_table(f"Data is missing columns needed for display: {', '.join(missing_display_cols)}")
        df_display = df_filtered[DISPLAY_COLS_ORDERED]
        df_display.columns = DISPLAY_HEADERS

        # --- Generate HTML table string using pandas ---
        print("Generating HTML table string using df.to_html()...")
        html_table = df_display.to_html(
            classes='dataframe', index=False, escape=True, na_rep='-', border=0
        )
        if not html_table or not isinstance(html_table, str):
             return format_error_html_for_table("Failed to generate HTML table using pandas.to_html().")
        print("HTML table string generated successfully via pandas.")
        return html_table # Return just the <table>...</table> string

    except pd.errors.EmptyDataError:
        return format_error_html_for_table(f"Data file is empty or invalid: {abs_csv_filepath}")
    except KeyError as e:
        print(f"Error generating HTML table: Missing expected column {e}")
        traceback.print_exc()
        return format_error_html_for_table(f"Internal Error: Missing expected column '{e}' in data.")
    except Exception as e:
        print(f"Error generating HTML table: {e}")
        traceback.print_exc()
        return format_error_html_for_table(f"Unexpected error during HTML table generation: {type(e).__name__}")


# --- Function to generate the FULL HTML page content ---
def generate_full_html_page(table_content_html: str, timestamp_str: str) -> str:
    """
    Constructs the entire HTML page as a string, embedding the table and timestamp.
    """
    # Define the HTML structure using an f-string.
    # CSS is included directly here.
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Upcoming Tennis Odds (Sackmann Model)</title>
    <style>
        :root {{ /* Define CSS variables */
            --primary-color: #0056b3;
            --secondary-color: #007bff;
            --light-gray: #f8f9fa;
            --medium-gray: #dee2e6;
            --dark-gray: #343a40;
            --white: #ffffff;
            --hover-color: #e9ecef;
            --shadow-color: rgba(0,0,0,0.06);
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol";
            line-height: 1.65;
            padding: 25px;
            max-width: 1050px;
            margin: 25px auto;
            background-color: var(--light-gray);
            color: var(--dark-gray);
        }}
        h1 {{
            color: var(--primary-color);
            border-bottom: 3px solid var(--primary-color);
            padding-bottom: 12px;
            margin-bottom: 30px;
            font-weight: 600;
        }}
        p {{
            margin-bottom: 20px;
        }}
        .table-container {{
            overflow-x: auto;
            box-shadow: 0 4px 10px var(--shadow-color);
            border-radius: 6px;
            background-color: var(--white);
            border: 1px solid var(--medium-gray);
            min-height: 100px;
            margin-bottom: 20px;
        }}
        table.dataframe {{
            width: 100%;
            border-collapse: collapse;
            margin: 0;
        }}
        table.dataframe th, table.dataframe td {{
            border: none;
            border-bottom: 1px solid var(--medium-gray);
            padding: 12px 14px;
            text-align: left;
            vertical-align: middle;
            white-space: nowrap;
        }}
        table.dataframe tbody tr:last-child td {{
            border-bottom: none;
        }}
        /* Column Styles - Adjust indices if needed based on DISPLAY_HEADERS */
        table.dataframe th:nth-child(1), table.dataframe td:nth-child(1) {{ width: 25%; white-space: normal; font-weight: 500;}} /* Player 1 */
        table.dataframe th:nth-child(2), table.dataframe td:nth-child(2) {{ width: 8%; text-align: right;}} /* P1 Prob */
        table.dataframe th:nth-child(3), table.dataframe td:nth-child(3) {{ width: 8%; text-align: right;}} /* P1 Odds */
        table.dataframe th:nth-child(4), table.dataframe td:nth-child(4) {{ width: 25%; white-space: normal; font-weight: 500;}} /* Player 2 */
        table.dataframe th:nth-child(5), table.dataframe td:nth-child(5) {{ width: 8%; text-align: right;}} /* P2 Prob */
        table.dataframe th:nth-child(6), table.dataframe td:nth-child(6) {{ width: 8%; text-align: right;}} /* P2 Odds */
        table.dataframe th:nth-child(7), table.dataframe td:nth-child(7) {{ width: 8%; }} /* Round */
        table.dataframe th:nth-child(8), table.dataframe td:nth-child(8) {{ width: 10%; white-space: normal;}} /* Tournament */
        table.dataframe thead th {{
            background-color: var(--secondary-color);
            color: var(--white);
            font-weight: 600;
            border-bottom: 2px solid var(--primary-color);
            white-space: nowrap;
            position: sticky;
            top: 0;
            z-index: 1;
        }}
        table.dataframe tbody tr:nth-child(even) {{
            background-color: var(--light-gray);
        }}
        table.dataframe tbody tr:hover {{
            background-color: var(--hover-color);
        }}
        .last-updated {{
            margin-top: 30px;
            padding-top: 15px;
            border-top: 1px solid var(--medium-gray);
            font-size: 0.9em;
            color: #6c757d;
            text-align: center;
        }}
        .{ERROR_MESSAGE_CLASS} {{ /* Style for error messages */
            color: #dc3545;
            font-weight: bold;
            text-align: center;
        }}
        @media (max-width: 768px) {{
            body {{ padding: 15px; }}
            h1 {{ font-size: 1.5em; }}
            table.dataframe th, table.dataframe td {{
                white-space: normal;
                padding: 10px 8px;
                font-size: 0.9em;
            }}
             table.dataframe th:nth-child(1), table.dataframe td:nth-child(1) {{ width: 30%;}}
             table.dataframe th:nth-child(4), table.dataframe td:nth-child(4) {{ width: 30%;}}
             table.dataframe th:nth-child(8), table.dataframe td:nth-child(8) {{ width: auto;}}
        }}
    </style>
</head>
<body>

    <h1>Upcoming Tennis Match Odds (Sackmann Model)</h1>
    <p>This page displays probabilities and calculated decimal odds for upcoming ATP and Challenger matches based on the Tennis Abstract Sackmann model ratings.</p>

    <div class="table-container">
        {table_content_html}
    </div>

    <div class="last-updated">
        {timestamp_str}
    </div>

</body>
</html>
"""
    return html_content


# --- Main Execution Logic ---
if __name__ == "__main__":
    print("Starting page generation process (Full HTML Generation)...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_abs = os.path.join(script_dir, DATA_DIR)
    output_file_abs = os.path.join(script_dir, OUTPUT_HTML_FILE) # Output path

    print(f"Script directory: {script_dir}")
    print(f"Looking for latest CSV in: {data_dir_abs}")
    # print(f"Using template: NO LONGER USING TEMPLATE") # Removed template logic
    print(f"Outputting to: {output_file_abs}")

    print("\nFinding latest data file...")
    latest_csv = find_latest_csv(data_dir_abs, CSV_PATTERN)

    table_html_content = "" # Initialize

    if latest_csv:
        print(f"\nGenerating HTML table content from: {os.path.basename(latest_csv)}")
        # Generate just the table HTML (or error HTML)
        table_html_content = generate_html_table_from_csv(latest_csv)
    else:
        print(f"\nNo CSV file found matching '{CSV_PATTERN}' in {data_dir_abs}. Generating error message.")
        # Generate error message HTML to put in the table container
        table_html_content = format_error_html_for_table(f"Error: Could not find latest data file ({CSV_PATTERN}) in {DATA_DIR}.")

    # Ensure content is always a string
    if not isinstance(table_html_content, str):
        print("ERROR: table_html_content is not a string! Defaulting.")
        table_html_content = format_error_html_for_table("Internal Error: Failed to generate valid HTML content.")

    # Get the timestamp string
    update_time = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
    timestamp_str = f"Last updated: {html.escape(update_time)}"

    # --- Generate the full page ---
    print("\nGenerating full HTML page content...")
    full_html = generate_full_html_page(table_html_content, timestamp_str)
    print("Full HTML page content generated.")

    # --- Write the full HTML to the output file ---
    try:
        print(f"Writing generated HTML content to: {output_file_abs}")
        with open(output_file_abs, 'w', encoding='utf-8') as f:
            f.write(full_html)
        print(f"Successfully wrote generated HTML to {os.path.basename(output_file_abs)}")
    except Exception as e:
        print(f"CRITICAL ERROR writing final HTML file: {e}")
        traceback.print_exc()

    print("\nPage generation process complete.")
