# generate_page.py (v13 - Fix Tuple Import)
# Loads processed comparison data and strategy log.
# Generates HTML page with tabs to display both tables.

import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob
import pytz
import traceback
import html
from typing import Optional, List, Dict, Any, Tuple # Added Tuple import

# --- Constants ---
DATA_DIR = "data_archive"
PROCESSED_CSV_PATTERN = "processed_comparison_*.csv"
STRATEGY_LOG_FILENAME = "strategy_log.csv" # Input log file
OUTPUT_HTML_FILE = "index.html"
INTERESTING_SPREAD_THRESHOLD = 0.50

# --- Column Definitions ---
# For Odds Comparison Table
COMP_COLS_ORDERED = [
    'TournamentName', 'Round', 'Player1Name', 'Player2Name',
    'Player1_Match_Prob', 'bc_p1_prob', 'Player2_Match_Prob', 'bc_p2_prob',
    'Player1_Match_Odds', 'bc_p1_odds', 'Player2_Match_Odds', 'bc_p2_odds',
    'p1_spread', 'p2_spread'
]
COMP_HEADERS = [
    "Tournament", "R", "Player 1", "Player 2",
    "P1 Prob (S)", "P1 Prob (BC)", "P2 Prob (S)", "P2 Prob (BC)",
    "P1 Odds (S)", "P1 Odds (BC)", "P2 Odds (S)", "P2 Odds (BC)",
    "P1 Spread", "P2 Spread"
]
# For Strategy Log Table
LOG_COLS_DISPLAY = [
    'BetDate', 'Strategy', 'Tournament', 'Player1', 'Player2', 'BetOnPlayer',
    'BetType', 'TriggerValue', 'BetAmount', 'BetOdds',
    'SackmannProb', 'BetcenterProb', 'MatchResult', 'ProfitLoss'
]
LOG_HEADERS = [
    "Date", "Strategy", "Tournament", "Player 1", "Player 2", "Bet On",
    "Trigger Type", "Trigger Val", "Stake", "Odds Taken",
    "P(S)", "P(BC)", "Result", "P/L"
]


# --- Helper Functions ---
# (find_latest_csv, format_simple_error_html remain the same)
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

def format_simple_error_html(message: str, context: str = "table") -> str:
    """Formats a simple error message as HTML."""
    print(f"Error generating {context}: {message}")
    return f'<div style="padding: 20px; text-align: center; color: #dc3545; background-color: #f8d7da; border: 1px solid #f5c6cb; border-radius: 4px;"><strong>Error ({context}):</strong> {html.escape(message)} Check logs for details.</div>'


# --- HTML Generation Functions ---
def apply_comp_table_styles(row: pd.Series) -> List[str]:
    """Applies CSS class 'interesting-spread-row' for the comparison table."""
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

def generate_comparison_table(df: pd.DataFrame) -> str:
    """Generates the HTML table for odds comparison using Pandas Styler."""
    # (Function unchanged from v12)
    if df is None or df.empty:
        return format_simple_error_html("No processed comparison data available.", context="comparison table")
    try:
        print("Formatting comparison data for display...")
        cols_to_use = [col for col in COMP_COLS_ORDERED if col in df.columns]
        missing_display_cols = [col for col in COMP_COLS_ORDERED if col not in df.columns]
        if missing_display_cols:
            print(f"Warning: Comparison data missing expected display columns: {', '.join(missing_display_cols)}.")

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
        print("Comparison data formatting complete.")

        try: # Sorting
            round_map = {'R128': 128, 'R64': 64, 'R32': 32, 'R16': 16, 'QF': 8, 'SF': 4, 'F': 2, 'W': 1}
            sort_cols = []
            if 'TournamentName' in df_display.columns: sort_cols.append('TournamentName')
            if 'Round' in df_display.columns:
                df_display['RoundSort'] = df_display['Round'].map(round_map).fillna(999); sort_cols.append('RoundSort')
            if sort_cols:
                df_display.sort_values(by=sort_cols, inplace=True, na_position='last')
                df_numeric = df_numeric.loc[df_display.index]
                if 'RoundSort' in df_display.columns: df_display.drop(columns=['RoundSort'], inplace=True)
                print(f"Sorted comparison table by: {', '.join(sort_cols).replace('RoundSort', 'Round')}.")
            else: print("Warning: Neither 'TournamentName' nor 'Round' column found for sorting comparison table.")
        except Exception as e_sort: print(f"Warning: Error during comparison table sorting: {e_sort}")

        current_headers = [COMP_HEADERS[COMP_COLS_ORDERED.index(col)] for col in cols_to_use]
        df_display.columns = current_headers

        print("Applying styles to comparison table...")
        styler = df_numeric.style.apply(apply_comp_table_styles, axis=1)
        styler.set_table_attributes('class="dataframe comparison-table"')
        styler.data = df_display
        html_table = styler.to_html(index=False, escape=True, na_rep='-', border=0)

        if not html_table or not isinstance(html_table, str):
            return format_simple_error_html("Pandas Styler failed to generate comparison HTML string.")
        print("Comparison HTML table string generated successfully.")
        return html_table

    except Exception as e:
        print(f"Error generating comparison HTML table: {e}")
        traceback.print_exc()
        return format_simple_error_html(f"Unexpected error during comparison table generation: {type(e).__name__}", context="comparison table")

def generate_strategy_log_table(df_log: pd.DataFrame) -> str:
    """Generates the HTML table for the strategy log using Pandas Styler."""
    # (Function unchanged from v12)
    if df_log is None or df_log.empty:
        return "<p>No strategy log data found or log is empty.</p>"
    try:
        print("Formatting strategy log data for display...")
        cols_to_display = [col for col in LOG_COLS_DISPLAY if col in df_log.columns]
        df_display = df_log[cols_to_display].copy()

        formatters = {
            'TriggerValue': '{:.3f}'.format,
            'BetAmount': '{:.3f}'.format,
            'BetOdds': '{:.2f}'.format,
            'SackmannProb': '{:.1f}%'.format,
            'BetcenterProb': '{:.1f}%'.format,
            'ProfitLoss': '{:+.2f}'.format
        }
        for col, fmt in formatters.items():
            if col in df_display.columns:
                 df_display[col] = pd.to_numeric(df_display[col], errors='coerce').map(fmt, na_action='ignore')

        header_map = {col: LOG_HEADERS[LOG_COLS_DISPLAY.index(col)] for col in cols_to_display}
        df_display.rename(columns=header_map, inplace=True)

        if 'Date' in df_display.columns:
             df_display.sort_values(by='Date', ascending=False, inplace=True)

        df_display.fillna('-', inplace=True)
        print("Strategy log formatting complete.")

        print("Generating strategy log HTML table string using Styler...")
        styler = df_display.style
        styler.set_table_attributes('class="dataframe strategy-log-table"')
        html_table = styler.to_html(index=False, escape=True, na_rep='-', border=0)

        if not html_table or not isinstance(html_table, str):
            return format_simple_error_html("Pandas Styler failed to generate strategy log HTML string.", context="strategy log")
        print("Strategy log HTML table string generated successfully.")
        return html_table

    except Exception as e:
        print(f"Error generating strategy log HTML table: {e}")
        traceback.print_exc()
        return format_simple_error_html(f"Unexpected error during strategy log table generation: {type(e).__name__}", context="strategy log")


def generate_full_html_page(comp_table_html: str, log_table_html: str, timestamp_str: str) -> str:
    """Constructs the entire HTML page with tabs, embedding both tables and timestamp."""
    # (Function unchanged from v12)
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Tennis Odds Comparison & Strategy Log</title>
    <style>
        /* --- Base Styles --- */
        :root {{
            --bg-color: #ffffff; --text-color: #333333; --primary-color: #0a68f5;
            --header-bg-color: #f8f9fa; --header-text-color: #343a40; --border-color: #e9ecef;
            --row-alt-bg-color: #f8f9fa; --hover-bg-color: #e0e0e0; --shadow-color: rgba(0, 0, 0, 0.05);
            --interesting-spread-row-text-color: #000000;
            --tab-border-color: #dee2e6; --tab-active-border-color: var(--primary-color);
            --tab-text-color: #495057; --tab-active-text-color: var(--primary-color);
            --tab-hover-bg: #e9ecef;
        }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; line-height: 1.55; padding: 20px; max-width: 98%; margin: 20px auto; background-color: var(--bg-color); color: var(--text-color); }}
        h1 {{ color: var(--primary-color); border-bottom: 2px solid var(--primary-color); padding-bottom: 10px; margin-bottom: 25px; font-weight: 600; font-size: 1.7em; }}
        p {{ margin-bottom: 15px; font-size: 0.95em; color: #555; }}
        p .highlight {{ padding: 1px 4px; border-radius: 3px; font-weight: bold; }}

        /* --- Tab Styles --- */
        .tab-container {{ margin-bottom: 25px; border-bottom: 1px solid var(--tab-border-color); }}
        .tab-button {{
            background-color: transparent; border: none; border-bottom: 3px solid transparent;
            padding: 10px 15px; margin-bottom: -1px; /* Overlap bottom border */
            cursor: pointer; font-size: 1em; color: var(--tab-text-color);
            transition: border-color 0.2s ease-in-out, color 0.2s ease-in-out;
        }}
        .tab-button:hover {{ background-color: var(--tab-hover-bg); }}
        .tab-button.active {{
            border-bottom-color: var(--tab-active-border-color);
            color: var(--tab-active-text-color); font-weight: 600;
        }}
        .tab-content {{ display: none; padding-top: 20px; }}
        .tab-content.active {{ display: block; }}

        /* --- Table Styles --- */
        .table-container {{ overflow-x: auto; box-shadow: 0 2px 6px var(--shadow-color); border-radius: 6px; background-color: var(--bg-color); border: 1px solid var(--border-color); margin-bottom: 20px; -webkit-overflow-scrolling: touch; }}
        table.dataframe {{ width: 100%; border-collapse: collapse; margin: 0; font-size: 0.85em; }}
        table.dataframe th, table.dataframe td {{ border: none; border-bottom: 1px solid var(--border-color); padding: 7px 8px; text-align: left; vertical-align: middle; white-space: nowrap; }}
        table.dataframe tbody tr:last-child td {{ border-bottom: none; }}

        /* Comparison Table Specific Widths */
        table.comparison-table th:nth-child(1), table.comparison-table td:nth-child(1) {{ width: 15%; white-space: normal;}}
        table.comparison-table th:nth-child(3), table.comparison-table td:nth-child(3) {{ width: 15%; white-space: normal; font-weight: 500;}}
        table.comparison-table th:nth-child(4), table.comparison-table td:nth-child(4) {{ width: 15%; white-space: normal; font-weight: 500;}}
        table.comparison-table th:nth-child(2), table.comparison-table td:nth-child(2) {{ width: 3%; }}
        table.comparison-table th:nth-child(n+5):nth-child(-n+8) {{ width: 6%; text-align: right;}} /* Probs */
        table.comparison-table th:nth-child(n+9):nth-child(-n+12) {{ width: 5%; text-align: right;}} /* Odds */
        table.comparison-table th:nth-child(n+13) {{ width: 4%; text-align: right;}} /* Spreads */

        /* Strategy Log Table Specific Widths (Example - Adjust as needed) */
        table.strategy-log-table th:nth-child(1), table.strategy-log-table td:nth-child(1) {{ width: 8%; }} /* Date */
        table.strategy-log-table th:nth-child(2), table.strategy-log-table td:nth-child(2) {{ width: 8%; }} /* Strategy */
        table.strategy-log-table th:nth-child(3), table.strategy-log-table td:nth-child(3) {{ width: 12%; white-space: normal;}} /* Tournament */
        table.strategy-log-table th:nth-child(4), table.strategy-log-table td:nth-child(4) {{ width: 12%; white-space: normal;}} /* P1 */
        table.strategy-log-table th:nth-child(5), table.strategy-log-table td:nth-child(5) {{ width: 12%; white-space: normal;}} /* P2 */
        table.strategy-log-table th:nth-child(6), table.strategy-log-table td:nth-child(6) {{ width: 5%; }} /* Bet On */
        table.strategy-log-table th:nth-child(n+7) {{ text-align: right; width: 6%;}} /* Numeric cols */


        /* General Table Header Styling */
        table.dataframe thead th {{ background-color: var(--header-bg-color); color: var(--header-text-color); font-weight: 600; border-bottom: 2px solid var(--border-color); position: sticky; top: 0; z-index: 1; }}
        /* General Row Styling */
        table.dataframe tbody tr:nth-child(even) td {{ background-color: var(--row-alt-bg-color); }}
        table.dataframe tbody tr:hover td {{ background-color: var(--hover-bg-color) !important; }}

        /* Interesting Spread Row Styling (Bold) */
        table.dataframe td.interesting-spread-row {{ font-weight: bold !important; color: var(--interesting-spread-row-text-color) !important; }}

        .last-updated {{ margin-top: 25px; padding-top: 15px; border-top: 1px solid var(--border-color); font-size: 0.85em; color: #6c757d; text-align: center; }}

        /* Responsive Adjustments */
        @media (max-width: 1200px) {{ table.dataframe th, table.dataframe td {{ font-size: 0.82em; padding: 6px 7px; }} }}
        @media (max-width: 992px) {{ body {{ padding: 15px; max-width: 100%; }} h1 {{ font-size: 1.5em; }} table.dataframe th, table.dataframe td {{ font-size: 0.78em; padding: 6px 5px; white-space: normal; }} table.dataframe th:nth-child(n), table.dataframe td:nth-child(n) {{ width: auto;}} table.dataframe th:nth-child(3), table.dataframe td:nth-child(3), table.dataframe th:nth-child(4), table.dataframe td:nth-child(4) {{ font-weight: normal;}} }}
        @media (max-width: 768px) {{ table.dataframe th, table.dataframe td {{ font-size: 0.72em; padding: 5px 4px; }} h1 {{ font-size: 1.3em; }} p {{ font-size: 0.9em; }} }}
    </style>
</head>
<body>
    <h1>Tennis Odds & Strategy Tracker</h1>

    <div class="tab-container">
        <button class="tab-button active" onclick="openTab(event, 'comparisonTab')">Odds Comparison</button>
        <button class="tab-button" onclick="openTab(event, 'logTab')">Strategy Log</button>
        </div>

    <div id="comparisonTab" class="tab-content active">
        <h2>Odds Comparison (Sackmann vs Betcenter)</h2>
        <p>Comparison of probabilities and calculated odds from the Tennis Abstract Sackmann model against betting odds scraped from Betcenter.be. The 'Spread' columns show the difference between Betcenter odds and Sackmann's calculated odds (Positive means Betcenter odds are higher).
        <br> - Rows in <span class="highlight">bold</span> indicate a significant disagreement (spread > {INTERESTING_SPREAD_THRESHOLD:.2f}) between the sources for at least one player.
        </p>
        <p>Matches involving qualifiers or appearing completed based on Sackmann data are filtered out. Name matching uses Title Case and may not be perfect.</p>
        <div class="table-container">{comp_table_html}</div>
    </div>

    <div id="logTab" class="tab-content">
        <h2>Strategy Log</h2>
        <p>Record of hypothetical bets identified by different strategies based on the daily odds comparison. Profit/Loss calculation depends on actual match results being available.</p>
        <div class="table-container">{log_table_html}</div>
    </div>

    <div class="last-updated">{timestamp_str}</div>

    <script>
        function openTab(evt, tabName) {{
            var i, tabcontent, tablinks;
            tabcontent = document.getElementsByClassName("tab-content");
            for (i = 0; i < tabcontent.length; i++) {{
                tabcontent[i].style.display = "none";
            }}
            tablinks = document.getElementsByClassName("tab-button");
            for (i = 0; i < tablinks.length; i++) {{
                tablinks[i].className = tablinks[i].className.replace(" active", "");
            }}
            document.getElementById(tabName).style.display = "block";
            evt.currentTarget.className += " active";
        }}
        // Ensure the default tab is shown on load
         document.addEventListener('DOMContentLoaded', (event) => {{
            // Check if any tab content is already marked active in HTML, if not, activate the first one
            let activeTab = document.querySelector('.tab-content.active');
            if (!activeTab) {{
                 let defaultTab = document.getElementById('comparisonTab');
                 if(defaultTab) {{ defaultTab.style.display = 'block'; }}
                 // Also mark the corresponding button active if needed
                 let defaultButton = document.querySelector('.tab-button'); // Assumes first button corresponds to first tab
                 if(defaultButton && !defaultButton.classList.contains('active')){{ defaultButton.className += " active"; }}
            }}
         }});
    </script>
    </body>
</html>"""
    return html_content

# --- Main Function to Load Data and Generate Page ---
def get_main_content_html(data_dir: str) -> Tuple[str, str]:
    """
    Loads comparison and log data, generates HTML for both tables.
    Returns tuple: (comparison_table_html, log_table_html)
    """
    # (Function unchanged from v12)
    comparison_html = format_simple_error_html("Comparison data failed to load or process.", "comparison table")
    log_html = "<p>Strategy log file not found or is empty.</p>" # Default message

    # Load Comparison Data
    try:
        print("\nFinding latest processed comparison data file...")
        latest_processed_csv = find_latest_csv(data_dir, PROCESSED_CSV_PATTERN)
        if latest_processed_csv:
            print(f"Loading comparison data from: {os.path.basename(latest_processed_csv)}")
            df_comparison = pd.read_csv(latest_processed_csv)
            if df_comparison.empty:
                 print(f"  Warning: Loaded comparison data file is empty.")
                 comparison_html = format_simple_error_html("Loaded comparison data file is empty.", "comparison table")
            else:
                 print(f"  Successfully loaded comparison data. Shape: {df_comparison.shape}")
                 print(f"\nGenerating comparison HTML table...")
                 comparison_html = generate_comparison_table(df_comparison)
        else:
            error_msg = f"Could not find latest processed data file ({PROCESSED_CSV_PATTERN})."
            print(f"  {error_msg}")
            comparison_html = format_simple_error_html(error_msg, "comparison table")
    except Exception as e_comp:
        print(f"Error loading/processing comparison data: {e_comp}")
        traceback.print_exc()
        comparison_html = format_simple_error_html(f"Error loading comparison data: {e_comp}", "comparison table")

    # Load Strategy Log Data
    try:
        log_file_path = os.path.join(data_dir, STRATEGY_LOG_FILENAME)
        print(f"\nChecking for strategy log file: {log_file_path}")
        if os.path.exists(log_file_path):
            print(f"Loading strategy log data from: {STRATEGY_LOG_FILENAME}")
            df_log = pd.read_csv(log_file_path)
            if df_log.empty:
                print("Strategy log file is empty.")
                log_html = "<p>Strategy log is empty.</p>"
            else:
                print(f"Successfully loaded strategy log. Shape: {df_log.shape}")
                print("\nGenerating strategy log HTML table...")
                log_html = generate_strategy_log_table(df_log)
        else:
            print("Strategy log file does not exist.")
            # Keep default message: log_html = "<p>Strategy log file not found or is empty.</p>"

    except Exception as e_log:
        print(f"Error loading/processing strategy log: {e_log}")
        traceback.print_exc()
        log_html = format_simple_error_html(f"Error loading strategy log: {e_log}", "strategy log")

    return comparison_html, log_html

# --- Main Execution Logic ---
if __name__ == "__main__":
    print("Starting HTML page generation process...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_abs = os.path.join(script_dir, DATA_DIR)
    output_file_abs = os.path.join(script_dir, OUTPUT_HTML_FILE)
    print(f"Script directory: {script_dir}"); print(f"Data archive directory: {data_dir_abs}"); print(f"Outputting generated HTML to: {output_file_abs}")

    # Call the function to get the HTML for both tables
    comparison_table_html, log_table_html = get_main_content_html(data_dir_abs)

    # Generate the full page embedding both pieces of content
    update_time = datetime.now(pytz.timezone('Europe/Brussels')).strftime('%Y-%m-%d %H:%M:%S %Z')
    timestamp_str = f"Last updated: {html.escape(update_time)}"
    print("\nGenerating full HTML page content with tabs...");
    # Pass both HTML strings to the page generator
    full_html = generate_full_html_page(comparison_table_html, log_table_html, timestamp_str)
    print("Full HTML page content generated.")

    try:
        print(f"Writing generated HTML content to: {output_file_abs}")
        with open(output_file_abs, 'w', encoding='utf-8') as f: f.write(full_html)
        print(f"Successfully wrote generated HTML to {os.path.basename(output_file_abs)}")
    except Exception as e: print(f"CRITICAL ERROR writing final HTML file: {e}"); traceback.print_exc()

    print("\nPage generation process complete.")

