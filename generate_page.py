# generate_page.py (v12 - Refactored for Scope)
# Encapsulates table generation logic in a function to ensure
# variable assignment before use. Continues bold row highlighting.

import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob
import pytz
import traceback
import html
from typing import Optional, List

# --- Constants ---
DATA_DIR = "data_archive"
PROCESSED_CSV_PATTERN = "processed_comparison_*.csv" # Input file pattern
OUTPUT_HTML_FILE = "index.html"
INTERESTING_SPREAD_THRESHOLD = 0.50 # Highlight row if abs(spread) > 0.50

# --- Column Definitions (for styling and display) ---
DISPLAY_COLS_ORDERED = [
    'TournamentName', 'Round', 'Player1Name', 'Player2Name',
    'Player1_Match_Prob', 'bc_p1_prob', 'Player2_Match_Prob', 'bc_p2_prob',
    'Player1_Match_Odds', 'bc_p1_odds', 'Player2_Match_Odds', 'bc_p2_odds',
    'p1_spread', 'p2_spread'
]
DISPLAY_HEADERS = [
    "Tournament", "R", "Player 1", "Player 2",
    "P1 Prob (S)", "P1 Prob (BC)", "P2 Prob (S)", "P2 Prob (BC)",
    "P1 Odds (S)", "P1 Odds (BC)", "P2 Odds (S)", "P2 Odds (BC)",
    "P1 Spread", "P2 Spread"
]

# --- Helper Functions ---
def find_latest_csv(directory: str, pattern: str) -> Optional[str]:
    """Finds the most recently modified CSV file matching the pattern."""
    try:
        if not os.path.isabs(directory):
             script_dir = os.path.dirname(os.path.abspath(__file__))
             search_dir = os.path.join(script_dir, directory)
        else:
             search_dir = directory
        search_path = os.path.join(search_dir, pattern); print(f"Searching for pattern: {search_path}")
        list_of_files = glob.glob(search_path)
        if not list_of_files: print(f"  No files found matching pattern."); return None
        list_of_files = [f for f in list_of_files if os.path.isfile(f)]
        if not list_of_files: print(f"  No *files* found matching pattern."); return None
        latest_file = max(list_of_files, key=os.path.getmtime); print(f"Found latest CSV file: {os.path.basename(latest_file)}")
        return latest_file
    except Exception as e: print(f"Error finding latest CSV file in '{directory}' with pattern '{pattern}': {e}"); traceback.print_exc(); return None

def format_simple_error_html(message: str) -> str:
    """Formats a simple error message as HTML for the table container."""
    print(f"Error generating table: {message}")
    return f'<div style="padding: 20px; text-align: center; color: #dc3545; background-color: #f8d7da; border: 1px solid #f5c6cb; border-radius: 4px;"><strong>Error:</strong> {html.escape(message)} Check logs for details.</div>'


# --- HTML Generation Functions ---
def apply_table_styles(row: pd.Series) -> List[str]:
    """
    Applies CSS class 'interesting-spread-row' to all cells in a row
    if the absolute spread for either player exceeds the threshold.
    """
    # (Function unchanged from v11)
    styles = [''] * len(row.index)
    try:
        p1_spread_val = pd.to_numeric(row.get('p1_spread'), errors='coerce')
        p2_spread_val = pd.to_numeric(row.get('p2_spread'), errors='coerce')
        p1_spread_abs = abs(p1_spread_val)
        p2_spread_abs = abs(p2_spread_val)
        if (not pd.isna(p1_spread_abs) and p1_spread_abs > INTERESTING_SPREAD_THRESHOLD) or \
           (not pd.isna(p2_spread_abs) and p2_spread_abs > INTERESTING_SPREAD_THRESHOLD):
            styles = ['interesting-spread-row'] * len(row.index)
    except Exception as e_row_spread:
        print(f"Warning: Error during interesting spread row check: {e_row_spread}")
    return styles


def generate_html_table(df: pd.DataFrame) -> str:
    """Generates the HTML table using Pandas Styler from the processed DataFrame."""
    # (Function unchanged from v11)
    if df is None or df.empty:
        return format_simple_error_html("No processed match data provided to generate_html_table.")
    try:
        print("Formatting final processed data for display...")
        cols_to_use = [col for col in DISPLAY_COLS_ORDERED if col in df.columns]
        missing_display_cols = [col for col in DISPLAY_COLS_ORDERED if col not in df.columns]
        if missing_display_cols:
            print(f"Warning: Processed data missing expected display columns: {', '.join(missing_display_cols)}. Table might look incomplete.")

        df_numeric = df[cols_to_use].copy()
        df_display = df[cols_to_use].copy()

        formatters = {
            'Player1_Match_Prob': '{:.1f}%'.format, 'Player2_Match_Prob': '{:.1f}%'.format,
            'bc_p1_prob': '{:.1f}%'.format, 'bc_p2_prob': '{:.1f}%'.format,
            'Player1_Match_Odds': '{:.2f}'.format, 'Player2_Match_Odds': '{:.2f}'.format,
            'bc_p1_odds': '{:.2f}'.format, 'bc_p2_odds': '{:.2f}'.format,
            'p1_spread': '{:+.2f}'.format, 'p2_spread': '{:+.2f}'.format
        }

        for col, fmt in formatters.items():
            if col in df_display.columns:
                 df_display[col] = pd.to_numeric(df_display[col], errors='coerce').map(fmt, na_action='ignore')

        df_display.fillna('-', inplace=True)
        print("Data formatting complete.")

        try:
            round_map = {'R128': 128, 'R64': 64, 'R32': 32, 'R16': 16, 'QF': 8, 'SF': 4, 'F': 2, 'W': 1}
            sort_cols = []
            if 'TournamentName' in df_display.columns:
                sort_cols.append('TournamentName')
            if 'Round' in df_display.columns:
                df_display['RoundSort'] = df_display['Round'].map(round_map).fillna(999)
                sort_cols.append('RoundSort')

            if sort_cols:
                df_display.sort_values(by=sort_cols, inplace=True, na_position='last')
                df_numeric = df_numeric.loc[df_display.index]
                if 'RoundSort' in df_display.columns:
                    df_display.drop(columns=['RoundSort'], inplace=True)
                print(f"Sorted matchups by: {', '.join(sort_cols).replace('RoundSort', 'Round')}.")
            else:
                print("Warning: Neither 'TournamentName' nor 'Round' column found for sorting.")
        except Exception as e_sort:
            print(f"Warning: Error during sorting: {e_sort}")

        current_headers = [DISPLAY_HEADERS[DISPLAY_COLS_ORDERED.index(col)] for col in cols_to_use]
        df_display.columns = current_headers

        print("Applying styles and generating HTML table string using Styler...")
        styler = df_numeric.style.apply(apply_table_styles, axis=1)
        styler.set_table_attributes('class="dataframe"')
        styler.data = df_display
        html_table = styler.to_html(index=False, escape=True, na_rep='-', border=0)

        if not html_table or not isinstance(html_table, str):
            return format_simple_error_html("Pandas Styler failed to generate HTML string.")

        print("HTML table string generated successfully via Styler.")
        return html_table

    except Exception as e:
        print(f"Error generating HTML table: {e}")
        traceback.print_exc()
        return format_simple_error_html(f"Unexpected error during HTML table generation: {type(e).__name__}")

def generate_full_html_page(table_content_html: str, timestamp_str: str) -> str:
    """Constructs the entire HTML page with updated styles, embedding the table and timestamp."""
    # (CSS unchanged from v11)
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Upcoming Tennis Odds Comparison (Sackmann vs Betcenter)</title>
    <style>
        /* --- Modern, Sleek, Simple Palette & Layout --- */
        :root {{
            --bg-color: #ffffff;
            --text-color: #333333;
            --primary-color: #0a68f5;
            --header-bg-color: #f8f9fa;
            --header-text-color: #343a40;
            --border-color: #e9ecef;
            --row-alt-bg-color: #f8f9fa;
            --hover-bg-color: #e9ecef;
            --shadow-color: rgba(0, 0, 0, 0.05);
            /* Row Highlighting Color */
            --interesting-spread-row-text-color: #000000; /* Black for bold text */
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol";
            line-height: 1.55;
            padding: 20px;
            max-width: 98%;
            margin: 20px auto;
            background-color: var(--bg-color);
            color: var(--text-color);
        }}

        h1 {{
            color: var(--primary-color);
            border-bottom: 2px solid var(--primary-color);
            padding-bottom: 10px;
            margin-bottom: 25px;
            font-weight: 600;
            font-size: 1.7em;
        }}

        p {{
            margin-bottom: 15px;
            font-size: 0.95em;
            color: #555;
        }}
        p .highlight {{
             padding: 1px 4px;
             border-radius: 3px;
             font-weight: bold; /* Make inline highlight bold */
        }}

        .table-container {{
            overflow-x: auto;
            box-shadow: 0 2px 6px var(--shadow-color);
            border-radius: 6px;
            background-color: var(--bg-color);
            border: 1px solid var(--border-color);
            margin-bottom: 20px;
            -webkit-overflow-scrolling: touch;
        }}

        table.dataframe {{
            width: 100%;
            border-collapse: collapse;
            margin: 0;
            font-size: 0.85em;
        }}

        table.dataframe th,
        table.dataframe td {{
            border: none;
            border-bottom: 1px solid var(--border-color);
            padding: 7px 8px;
            text-align: left;
            vertical-align: middle;
            white-space: nowrap;
        }}

        table.dataframe tbody tr:last-child td {{
            border-bottom: none;
        }}

        /* Column Widths */
        table.dataframe th:nth-child(1), table.dataframe td:nth-child(1) {{ width: 15%; white-space: normal;}} /* Tournament */
        table.dataframe th:nth-child(3), table.dataframe td:nth-child(3) {{ width: 15%; white-space: normal; font-weight: 500;}} /* Player 1 */
        table.dataframe th:nth-child(4), table.dataframe td:nth-child(4) {{ width: 15%; white-space: normal; font-weight: 500;}} /* Player 2 */
        table.dataframe th:nth-child(2), table.dataframe td:nth-child(2) {{ width: 3%; }}  /* Round (R) */
        table.dataframe th:nth-child(5), table.dataframe td:nth-child(5) {{ width: 6%; text-align: right;}} /* P1 Prob (S) */
        table.dataframe th:nth-child(6), table.dataframe td:nth-child(6) {{ width: 6%; text-align: right;}} /* P1 Prob (BC) */
        table.dataframe th:nth-child(7), table.dataframe td:nth-child(7) {{ width: 6%; text-align: right;}} /* P2 Prob (S) */
        table.dataframe th:nth-child(8), table.dataframe td:nth-child(8) {{ width: 6%; text-align: right;}} /* P2 Prob (BC) */
        table.dataframe th:nth-child(9), table.dataframe td:nth-child(9) {{ width: 5%; text-align: right;}} /* P1 Odds (S) */
        table.dataframe th:nth-child(10), table.dataframe td:nth-child(10) {{ width: 5%; text-align: right;}} /* P1 Odds (BC) */
        table.dataframe th:nth-child(11), table.dataframe td:nth-child(11) {{ width: 5%; text-align: right;}} /* P2 Odds (S) */
        table.dataframe th:nth-child(12), table.dataframe td:nth-child(12) {{ width: 5%; text-align: right;}} /* P2 Odds (BC) */
        table.dataframe th:nth-child(13), table.dataframe td:nth-child(13) {{ width: 4%; text-align: right;}} /* P1 Spread */
        table.dataframe th:nth-child(14), table.dataframe td:nth-child(14) {{ width: 4%; text-align: right;}} /* P2 Spread */

        /* Header Styling */
        table.dataframe thead th {{
            background-color: var(--header-bg-color);
            color: var(--header-text-color);
            font-weight: 600;
            border-bottom: 2px solid var(--border-color);
            position: sticky;
            top: 0;
            z-index: 1;
        }}

        /* Row Styling */
        table.dataframe tbody tr:nth-child(even) td {{
            background-color: var(--row-alt-bg-color);
        }}
        table.dataframe tbody tr:hover td {{
            background-color: var(--hover-bg-color) !important;
        }}

        /* --- Interesting Spread Row Styling (Bold) --- */
        table.dataframe td.interesting-spread-row {{
            font-weight: bold !important;
            color: var(--interesting-spread-row-text-color) !important;
        }}

        .last-updated {{
            margin-top: 25px;
            padding-top: 15px;
            border-top: 1px solid var(--border-color);
            font-size: 0.85em;
            color: #6c757d;
            text-align: center;
        }}

        /* Responsive Adjustments */
        @media (max-width: 1200px) {{
            table.dataframe th, table.dataframe td {{ font-size: 0.82em; padding: 6px 7px; }}
        }}
        @media (max-width: 992px) {{
             body {{ padding: 15px; max-width: 100%; }}
             h1 {{ font-size: 1.5em; }}
             table.dataframe th, table.dataframe td {{ font-size: 0.78em; padding: 6px 5px; white-space: normal; }}
             table.dataframe th:nth-child(n), table.dataframe td:nth-child(n) {{ width: auto;}}
             table.dataframe th:nth-child(3), table.dataframe td:nth-child(3),
             table.dataframe th:nth-child(4), table.dataframe td:nth-child(4) {{ font-weight: normal;}}
        }}
        @media (max-width: 768px) {{
            table.dataframe th, table.dataframe td {{ font-size: 0.72em; padding: 5px 4px; }}
            h1 {{ font-size: 1.3em; }}
            p {{ font-size: 0.9em; }}
        }}
    </style>
</head>
<body>
    <h1>Upcoming Tennis Odds Comparison (Sackmann vs Betcenter)</h1>

    <p>Comparison of probabilities and calculated odds from the Tennis Abstract Sackmann model against betting odds scraped from Betcenter.be. The 'Spread' columns show the difference between Betcenter odds and Sackmann's calculated odds (Positive means Betcenter odds are higher).
    <br> - Rows in <span class="highlight">bold</span> indicate a significant disagreement (spread > {INTERESTING_SPREAD_THRESHOLD:.2f}) between the sources for at least one player.
    </p>
    <p>Matches involving qualifiers or appearing completed based on Sackmann data are filtered out. Name matching uses Title Case and may not be perfect.</p>

    <div class="table-container">{table_content_html}</div>
    <div class="last-updated">{timestamp_str}</div>
</body>
</html>"""
    return html_content

# --- NEW Function to Encapsulate Table Generation Logic ---
def get_main_table_html(data_dir: str) -> str:
    """
    Finds the latest processed data, loads it, and generates the HTML table content.
    Returns either the HTML table string or a formatted error string.
    """
    final_df = None
    table_html_content = format_simple_error_html("Initialization error or process did not start correctly.") # Default

    try:
        print("\nFinding latest processed data file...")
        latest_processed_csv = find_latest_csv(data_dir, PROCESSED_CSV_PATTERN)

        if latest_processed_csv:
            print(f"Loading processed data from: {os.path.basename(latest_processed_csv)}")
            try:
                final_df = pd.read_csv(latest_processed_csv)
                if final_df.empty:
                     print(f"  Warning: Loaded processed data file is empty.")
                     table_html_content = format_simple_error_html("Loaded processed data file is empty.")
                else:
                     print(f"  Successfully loaded processed data. Shape: {final_df.shape}")
                     print(f"\nGenerating HTML table content from final data (Shape: {final_df.shape})...")
                     # Generate the table HTML - this function handles its own errors
                     table_html_content = generate_html_table(final_df)

            except Exception as load_err:
                error_msg = f"Error loading or processing CSV '{os.path.basename(latest_processed_csv)}': {load_err}"
                print(f"  {error_msg}")
                traceback.print_exc()
                table_html_content = format_simple_error_html(error_msg) # Assign error HTML
        else:
            error_msg = f"Could not find latest processed data file ({PROCESSED_CSV_PATTERN}). Run processing script first."
            print(f"  {error_msg}")
            table_html_content = format_simple_error_html(error_msg) # Assign error HTML

    except Exception as outer_err:
         # Catch any unexpected error during file finding itself
         print(f"CRITICAL ERROR finding/loading data: {outer_err}")
         traceback.print_exc()
         table_html_content = format_simple_error_html(f"Critical processing error: {outer_err}")

    # This function now guarantees returning a string (table or error)
    return table_html_content

# --- Main Execution Logic ---
if __name__ == "__main__":
    print("Starting HTML page generation process...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_abs = os.path.join(script_dir, DATA_DIR)
    output_file_abs = os.path.join(script_dir, OUTPUT_HTML_FILE)
    print(f"Script directory: {script_dir}"); print(f"Looking for latest processed CSV in: {data_dir_abs}"); print(f"Outputting generated HTML to: {output_file_abs}")

    # Call the new function to get the table HTML or an error message
    table_html_content = get_main_table_html(data_dir_abs)

    # Now generate the full page using the result
    update_time = datetime.now(pytz.timezone('Europe/Brussels')).strftime('%Y-%m-%d %H:%M:%S %Z')
    timestamp_str = f"Last updated: {html.escape(update_time)}"
    print("\nGenerating full HTML page content...");
    full_html = generate_full_html_page(table_html_content, timestamp_str) # This should now always work
    print("Full HTML page content generated.")

    try:
        print(f"Writing generated HTML content to: {output_file_abs}")
        with open(output_file_abs, 'w', encoding='utf-8') as f: f.write(full_html)
        print(f"Successfully wrote generated HTML to {os.path.basename(output_file_abs)}")
    except Exception as e: print(f"CRITICAL ERROR writing final HTML file: {e}"); traceback.print_exc()

    print("\nPage generation process complete.")

