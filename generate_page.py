# generate_page.py (Simplified - Final Fix - Debug Code Removed - NameError Fix)
# Loads the pre-processed/merged data CSV and generates the HTML page.

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
MERGED_CSV_PATTERN = "merged_matchups_*.csv" # Input file pattern
OUTPUT_HTML_FILE = "index.html"
VALUE_BET_THRESHOLD = 1.10 # Example: Betcenter odds must be 10% higher

# --- Column Definitions (for styling and display) ---
DISPLAY_COLS_ORDERED = [
    'TournamentName', 'Round', 'Player1Name', 'Player2Name',
    'Player1_Match_Prob', 'Player2_Match_Prob', 'Player1_Match_Odds', 'Player2_Match_Odds',
    'bc_p1_odds', 'bc_p2_odds', 'p1_spread', 'p2_spread'
]
DISPLAY_HEADERS = [
    "Tournament", "Round", "Player 1", "Player 2",
    "P1 Prob (Sack.)", "P2 Prob (Sack.)", "P1 Odds (Sack.)", "P2 Odds (Sack.)",
    "P1 Odds (BC)", "P2 Odds (BC)", "P1 Spread", "P2 Spread"
]

# --- Helper Functions ---
def find_latest_csv(directory: str, pattern: str) -> Optional[str]:
    """Finds the most recently modified CSV file matching the pattern."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__)); search_dir = os.path.join(script_dir, directory)
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
    # Basic HTML formatting for the error message
    return f'<div style="padding: 20px; text-align: center; color: #dc3545; background-color: #f8d7da; border: 1px solid #f5c6cb; border-radius: 4px;"><strong>Error:</strong> {html.escape(message)} Check logs for details.</div>'

# --- HTML Generation Functions ---
def apply_table_styles(row: pd.Series) -> List[str]:
    """Applies CSS classes for value bets and spread highlighting."""
    styles = [''] * len(DISPLAY_COLS_ORDERED)
    try: # Value Bet Check
        sack_odds_p1 = pd.to_numeric(row.get('Player1_Match_Odds'), errors='coerce'); bc_odds_p1 = pd.to_numeric(row.get('bc_p1_odds'), errors='coerce')
        sack_odds_p2 = pd.to_numeric(row.get('Player2_Match_Odds'), errors='coerce'); bc_odds_p2 = pd.to_numeric(row.get('bc_p2_odds'), errors='coerce')
        if not pd.isna(sack_odds_p1) and not pd.isna(bc_odds_p1) and bc_odds_p1 >= sack_odds_p1 * VALUE_BET_THRESHOLD:
            try: styles[DISPLAY_COLS_ORDERED.index('bc_p1_odds')] = 'value-bet-p1'
            except ValueError: pass
        if not pd.isna(sack_odds_p2) and not pd.isna(bc_odds_p2) and bc_odds_p2 >= sack_odds_p2 * VALUE_BET_THRESHOLD:
            try: styles[DISPLAY_COLS_ORDERED.index('bc_p2_odds')] = 'value-bet-p2'
            except ValueError: pass
    except Exception as e_val: print(f"Warning: Error during value bet styling: {e_val}")
    try: # Spread Check
        p1_spread = pd.to_numeric(row.get('p1_spread'), errors='coerce'); p2_spread = pd.to_numeric(row.get('p2_spread'), errors='coerce')
        if not pd.isna(p1_spread):
            try:
                idx = DISPLAY_COLS_ORDERED.index('p1_spread')
                if p1_spread > 0: styles[idx] = 'spread-positive'
                elif p1_spread < 0: styles[idx] = 'spread-negative'
            except ValueError: pass
        if not pd.isna(p2_spread):
            try:
                idx = DISPLAY_COLS_ORDERED.index('p2_spread')
                if p2_spread > 0: styles[idx] = 'spread-positive'
                elif p2_spread < 0: styles[idx] = 'spread-negative'
            except ValueError: pass
    except Exception as e_spread: print(f"Warning: Error during spread styling: {e_spread}")
    return styles

def generate_html_table(df: pd.DataFrame) -> str:
    """Generates the HTML table using Pandas Styler from the pre-merged DataFrame."""
    if df is None or df.empty:
        # This case should ideally be handled before calling this function,
        # but we return an error message just in case.
        return format_simple_error_html("No merged match data provided to generate_html_table.")
    try:
        print("Formatting final merged data for display...")
        missing_display_cols = [col for col in DISPLAY_COLS_ORDERED if col not in df.columns]
        if missing_display_cols:
            return format_simple_error_html(f"Merged data missing columns: {', '.join(missing_display_cols)}.")

        # Create copies for formatting and styling
        df_numeric = df[DISPLAY_COLS_ORDERED].copy()
        df_display = df[DISPLAY_COLS_ORDERED].copy()

        # Define formatters for numeric columns
        formatters = {
            'Player1_Match_Prob': '{:.1f}%'.format, 'Player2_Match_Prob': '{:.1f}%'.format,
            'Player1_Match_Odds': '{:.2f}'.format, 'Player2_Match_Odds': '{:.2f}'.format,
            'bc_p1_odds': '{:.2f}'.format, 'bc_p2_odds': '{:.2f}'.format,
            'p1_spread': '{:+.2f}'.format, 'p2_spread': '{:+.2f}'.format
        }

        # Apply formatting to the display DataFrame
        for col, fmt in formatters.items():
            if col in df_display.columns:
                 # Use .map() for robust NaN handling during formatting
                 df_display[col] = pd.to_numeric(df_display[col], errors='coerce').map(fmt, na_action='ignore')

        # Fill remaining NaNs and finalize display DataFrame
        df_display.fillna('-', inplace=True)
        print("Data formatting complete.")

        # Sorting logic
        try:
            round_map = {'R128': 128, 'R64': 64, 'R32': 32, 'R16': 16, 'QF': 8, 'SF': 4, 'F': 2, 'W': 1}
            # Ensure 'Round' column exists before trying to map/sort
            if 'Round' in df_display.columns:
                df_display['RoundSort'] = df_display['Round'].map(round_map).fillna(999)
                # Ensure 'TournamentName' also exists for sorting
                sort_cols = ['TournamentName', 'RoundSort'] if 'TournamentName' in df_display.columns else ['RoundSort']
                df_display.sort_values(by=sort_cols, inplace=True, na_position='last')
                df_numeric = df_numeric.loc[df_display.index] # Align numeric data index
                df_display.drop(columns=['RoundSort'], inplace=True)
                print("Sorted matchups by available columns (Tournament/Round).")
            else:
                print("Warning: 'Round' column not found for sorting.")
        except Exception as e_sort:
            print(f"Warning: Error during sorting: {e_sort}")

        # Set display headers AFTER sorting/indexing
        df_display.columns = DISPLAY_HEADERS

        print("Applying styles and generating HTML table string using Styler...")
        styler = df_numeric.style.apply(apply_table_styles, axis=1) # Style based on numeric data
        styler.set_table_attributes('class="dataframe"')
        styler.data = df_display # Use the formatted data for the final HTML output
        html_table = styler.to_html(index=False, escape=True, na_rep='-', border=0)

        if not html_table or not isinstance(html_table, str):
            # This case indicates an issue within styler.to_html() itself
            return format_simple_error_html("Pandas Styler failed to generate HTML string.")

        print("HTML table string generated successfully via Styler.")
        return html_table

    except Exception as e:
        print(f"Error generating HTML table: {e}")
        traceback.print_exc()
        # Return a formatted error message if any exception occurs within this function
        return format_simple_error_html(f"Unexpected error during HTML table generation: {type(e).__name__}")

def generate_full_html_page(table_content_html: str, timestamp_str: str) -> str:
    """Constructs the entire HTML page, embedding the table and timestamp."""
    # (CSS and HTML structure remain the same as previous version)
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Upcoming Tennis Odds Comparison (Sackmann vs Betcenter)</title>
    <style>
        :root {{
            --primary-color: #0056b3; --secondary-color: #007bff; --light-gray: #f8f9fa;
            --medium-gray: #dee2e6; --dark-gray: #343a40; --white: #ffffff; --hover-color: #e9ecef;
            --shadow-color: rgba(0,0,0,0.06); --value-bet-bg-color: #d4edda; --value-bet-text-color: #155724;
            --betcenter-bg-color: #fff0f5; --betcenter-header-bg-color: #ffe4e1;
            --spread-positive-bg-color: #e6ffed; --spread-positive-text-color: #006400;
            --spread-negative-bg-color: #ffeeee; --spread-negative-text-color: #a52a2a;
            --spread-header-bg-color: #f5f5f5;
        }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol"; line-height: 1.6; padding: 20px; max-width: 1400px; margin: 20px auto; background-color: var(--light-gray); color: var(--dark-gray); }}
        h1 {{ color: var(--primary-color); border-bottom: 3px solid var(--primary-color); padding-bottom: 10px; margin-bottom: 25px; font-weight: 600; font-size: 1.8em; }}
        p {{ margin-bottom: 15px; font-size: 0.95em; }}
        .table-container {{ overflow-x: auto; box-shadow: 0 4px 10px var(--shadow-color); border-radius: 6px; background-color: var(--white); border: 1px solid var(--medium-gray); min-height: 100px; margin-bottom: 20px; }}
        table.dataframe {{ width: 100%; border-collapse: collapse; margin: 0; font-size: 0.85em; }}
        table.dataframe th, table.dataframe td {{ border: none; border-bottom: 1px solid var(--medium-gray); padding: 8px 10px; text-align: left; vertical-align: middle; white-space: nowrap; }}
        table.dataframe tbody tr:last-child td {{ border-bottom: none; }}
        table.dataframe th:nth-child(1), table.dataframe td:nth-child(1) {{ width: 12%; white-space: normal;}} /* Tournament */
        table.dataframe th:nth-child(2), table.dataframe td:nth-child(2) {{ width: 4%; }}  /* Round */
        table.dataframe th:nth-child(3), table.dataframe td:nth-child(3) {{ width: 13%; white-space: normal; font-weight: 500;}} /* Player 1 */
        table.dataframe th:nth-child(4), table.dataframe td:nth-child(4) {{ width: 13%; white-space: normal; font-weight: 500;}} /* Player 2 */
        table.dataframe th:nth-child(5), table.dataframe td:nth-child(5) {{ width: 7%; text-align: right;}} /* P1 Prob (Sack) */
        table.dataframe th:nth-child(6), table.dataframe td:nth-child(6) {{ width: 7%; text-align: right;}} /* P2 Prob (Sack) */
        table.dataframe th:nth-child(7), table.dataframe td:nth-child(7) {{ width: 7%; text-align: right;}} /* P1 Odds (Sack) */
        table.dataframe th:nth-child(8), table.dataframe td:nth-child(8) {{ width: 7%; text-align: right;}} /* P2 Odds (Sack) */
        table.dataframe th:nth-child(9), table.dataframe td:nth-child(9) {{ width: 7%; text-align: right; background-color: var(--betcenter-header-bg-color);}} /* P1 Odds (BC) Header */
        table.dataframe th:nth-child(10), table.dataframe td:nth-child(10) {{ width: 7%; text-align: right; background-color: var(--betcenter-header-bg-color);}} /* P2 Odds (BC) Header */
        table.dataframe th:nth-child(11), table.dataframe td:nth-child(11) {{ width: 7%; text-align: right; background-color: var(--spread-header-bg-color);}} /* P1 Spread Header */
        table.dataframe th:nth-child(12), table.dataframe td:nth-child(12) {{ width: 6%; text-align: right; background-color: var(--spread-header-bg-color);}} /* P2 Spread Header */
        table.dataframe td:nth-child(9), table.dataframe td:nth-child(10) {{ background-color: var(--betcenter-bg-color); }}
        table.dataframe td:nth-child(11), table.dataframe td:nth-child(12) {{ background-color: var(--spread-header-bg-color); }}
        table.dataframe thead th {{ background-color: var(--secondary-color); color: var(--white); font-weight: 600; border-bottom: 2px solid var(--primary-color); position: sticky; top: 0; z-index: 1; }}
        table.dataframe tbody tr:nth-child(even) td {{ background-color: var(--light-gray); }}
        table.dataframe tbody tr:nth-child(even) td:nth-child(9), table.dataframe tbody tr:nth-child(even) td:nth-child(10) {{ background-color: var(--betcenter-bg-color); opacity: 0.9; }}
        table.dataframe tbody tr:nth-child(even) td:nth-child(11), table.dataframe tbody tr:nth-child(even) td:nth-child(12) {{ background-color: var(--spread-header-bg-color); opacity: 0.9; }}
        table.dataframe tbody tr:hover td {{ background-color: var(--hover-color); }}
        table.dataframe tbody tr:hover td:nth-child(9), table.dataframe tbody tr:hover td:nth-child(10) {{ background-color: #fddde6; }}
        table.dataframe tbody tr:hover td:nth-child(11), table.dataframe tbody tr:hover td:nth-child(12) {{ background-color: #e9e9e9; }}
        table.dataframe td.value-bet-p1, table.dataframe td.value-bet-p2 {{ background-color: var(--value-bet-bg-color) !important; color: var(--value-bet-text-color); font-weight: bold; }}
        table.dataframe tbody tr:hover td.value-bet-p1, table.dataframe tbody tr:hover td.value-bet-p2 {{ background-color: #b8dfc1 !important; }}
        table.dataframe td.spread-positive {{ background-color: var(--spread-positive-bg-color) !important; color: var(--spread-positive-text-color); font-weight: 500; }}
        table.dataframe td.spread-negative {{ background-color: var(--spread-negative-bg-color) !important; color: var(--spread-negative-text-color); font-weight: 500; }}
        table.dataframe tbody tr:hover td.spread-positive {{ background-color: #c8e6c9 !important; }}
        table.dataframe tbody tr:hover td.spread-negative {{ background-color: #ffcdd2 !important; }}
        .last-updated {{ margin-top: 25px; padding-top: 15px; border-top: 1px solid var(--medium-gray); font-size: 0.9em; color: #6c757d; text-align: center; }}
        /* Error message div style defined inline via format_simple_error_html */
        @media (max-width: 1200px) {{ table.dataframe th, table.dataframe td {{ font-size: 0.8em; padding: 7px 8px; }} }}
        @media (max-width: 992px) {{ body {{ padding: 15px; max-width: 100%; }} h1 {{ font-size: 1.5em; }} table.dataframe th, table.dataframe td {{ font-size: 0.75em; padding: 6px 5px; white-space: normal; }} table.dataframe th:nth-child(n), table.dataframe td:nth-child(n) {{ width: auto;}} table.dataframe th:nth-child(3), table.dataframe td:nth-child(3), table.dataframe th:nth-child(4), table.dataframe td:nth-child(4) {{ font-weight: normal;}} }}
        @media (max-width: 768px) {{ table.dataframe th, table.dataframe td {{ font-size: 0.7em; padding: 5px 4px; }} }}
    </style>
</head>
<body>
    <h1>Upcoming Tennis Match Odds Comparison (Sackmann vs Betcenter)</h1>
    <p>Comparison of probabilities and calculated odds from the Tennis Abstract Sackmann model against betting odds scraped from Betcenter.be. The 'Spread' columns show the difference between Betcenter odds and Sackmann's calculated odds (Positive means Betcenter odds are higher). Cells highlighted in <span style="background-color: var(--value-bet-bg-color); color: var(--value-bet-text-color); padding: 0 3px; border-radius: 3px;">green</span> indicate potential value bets where Betcenter odds are at least {int((VALUE_BET_THRESHOLD-1)*100)}% higher than the model's implied odds.</p>
    <p>Matches involving qualifiers or appearing completed based on Sackmann data are filtered out. Name matching uses Title Case and may not be perfect.</p>
    <div class="table-container">{table_content_html}</div>
    <div class="last-updated">{timestamp_str}</div>
</body>
</html>"""
    return html_content

# --- Main Execution Logic ---
if __name__ == "__main__":
    print("Starting HTML page generation process...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_abs = os.path.join(script_dir, DATA_DIR)
    output_file_abs = os.path.join(script_dir, OUTPUT_HTML_FILE)
    print(f"Script directory: {script_dir}"); print(f"Looking for latest merged CSV in: {data_dir_abs}"); print(f"Outputting generated HTML to: {output_file_abs}")

    # Initialize with a default error message. This ensures the variable *always* exists.
    table_html_content = format_simple_error_html("Initialization error or process did not start correctly.")
    final_df = None # Initialize df to None

    try:
        print("\nFinding latest merged data file...")
        latest_merged_csv = find_latest_csv(data_dir_abs, MERGED_CSV_PATTERN)

        if latest_merged_csv:
            print(f"Loading merged data from: {os.path.basename(latest_merged_csv)}")
            try:
                # Attempt to load the dataframe
                final_df = pd.read_csv(latest_merged_csv)
                if final_df.empty:
                     print(f"  Warning: Loaded merged data file is empty.")
                     table_html_content = format_simple_error_html("Loaded merged data file is empty.")
                     # Keep final_df as empty dataframe, generate_html_table will handle it
                else:
                     print(f"  Successfully loaded merged data. Shape: {final_df.shape}")
                     # Generate the table ONLY if df is loaded and not empty
                     print(f"\nGenerating HTML table content from final data (Shape: {final_df.shape})...")
                     table_html_content = generate_html_table(final_df) # Assignment happens here

            except Exception as load_err:
                error_msg = f"Error loading or processing merged CSV '{os.path.basename(latest_merged_csv)}': {load_err}"
                print(f"  {error_msg}")
                traceback.print_exc()
                table_html_content = format_simple_error_html(error_msg) # Assign error HTML
                # final_df remains None or potentially partially loaded, but table won't be generated
        else:
            error_msg = f"Could not find latest merged data file ({MERGED_CSV_PATTERN}). Run processing script first."
            print(f"  {error_msg}")
            table_html_content = format_simple_error_html(error_msg) # Assign error HTML
            # final_df remains None

    except Exception as main_err:
         # Catch any unexpected error during file finding itself
         print(f"CRITICAL ERROR in main processing block (e.g., file finding): {main_err}")
         traceback.print_exc()
         table_html_content = format_simple_error_html(f"Critical processing error: {main_err}")

    # --- Generate and Write Page ---
    # At this point, table_html_content MUST be defined (either valid table or error HTML)
    update_time = datetime.now(pytz.timezone('Europe/Brussels')).strftime('%Y-%m-%d %H:%M:%S %Z')
    timestamp_str = f"Last updated: {html.escape(update_time)}"
    print("\nGenerating full HTML page content...");
    # Use the determined table_html_content (could be table or error message)
    full_html = generate_full_html_page(table_html_content, timestamp_str)
    print("Full HTML page content generated.")

    try:
        print(f"Writing generated HTML content to: {output_file_abs}")
        with open(output_file_abs, 'w', encoding='utf-8') as f: f.write(full_html)
        print(f"Successfully wrote generated HTML to {os.path.basename(output_file_abs)}")
    except Exception as e: print(f"CRITICAL ERROR writing final HTML file: {e}"); traceback.print_exc()

    print("\nPage generation process complete.")

