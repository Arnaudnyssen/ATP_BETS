# generate_page.py (v17 - Fix Styler KeyError v2)
# Applies styles and sets styler.data before renaming headers in final HTML string.

import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob
import pytz
import traceback
import html
from typing import Optional, List, Dict, Any, Tuple

# --- Constants ---
DATA_DIR = "data_archive"
PROCESSED_CSV_PATTERN = "processed_comparison_*.csv"
STRATEGY_LOG_FILENAME = "strategy_log.csv"
OUTPUT_HTML_FILE = "index.html"
# INTERESTING_SPREAD_THRESHOLD = 0.50 # Not used for highlighting anymore

# --- Column Definitions ---
# Internal names used in DataFrames
COMP_COLS_ORDERED = [
    'TournamentName', 'Round', 'Player1Name', 'Player2Name',
    'Player1_Match_Prob', 'bc_p1_prob', 'Player2_Match_Prob', 'bc_p2_prob',
    'Player1_Match_Odds', 'bc_p1_odds', 'Player2_Match_Odds', 'bc_p2_odds',
    'p1_spread', 'rel_p1_spread', 'p2_spread', 'rel_p2_spread'
]
# Display headers corresponding to the above order
COMP_HEADERS = [
    "Tournament", "R", "Player 1", "Player 2",
    "P1 Prob (S)", "P1 Prob (BC)", "P2 Prob (S)", "P2 Prob (BC)",
    "P1 Odds (S)", "P1 Odds (BC)", "P2 Odds (S)", "P2 Odds (BC)",
    "P1 Sprd", "Rel Sprd", "P2 Sprd", "Rel Sprd"
]
# Create a mapping for header replacement later
COMP_HEADER_MAP = dict(zip(COMP_COLS_ORDERED, COMP_HEADERS))

LOG_COLS_DISPLAY = [
    'BetDate', 'Strategy', 'Tournament', 'Player1', 'Player2', 'BetOnPlayer',
    'BetType', 'TriggerValue', 'BetAmount', 'BetOdds',
    'SackmannProb', 'BetcenterProb', 'Edge',
    'MatchResult', 'ProfitLoss'
]
LOG_HEADERS = [
    "Date", "Strategy", "Tournament", "P1", "P2", "Bet On",
    "Trigger Type", "Trigger Val", "Stake", "Odds",
    "P(S)", "P(BC)", "Edge",
    "Result", "P/L"
]
LOG_HEADER_MAP = dict(zip(LOG_COLS_DISPLAY, LOG_HEADERS))


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
    """
    Applies CSS classes for spread sign highlighting to specific spread cells.
    Expects original column names (e.g., 'p1_spread').
    """
    # (Function unchanged)
    styles = [''] * len(row.index)
    cols_in_row = row.index
    try:
        p1_spread = pd.to_numeric(row.get('p1_spread'), errors='coerce')
        p2_spread = pd.to_numeric(row.get('p2_spread'), errors='coerce')
        if 'p1_spread' in cols_in_row and not pd.isna(p1_spread):
            try:
                idx_abs = cols_in_row.get_loc('p1_spread')
                if p1_spread > 0: styles[idx_abs] = 'spread-positive'
                elif p1_spread < 0: styles[idx_abs] = 'spread-negative'
            except KeyError: pass
        if 'rel_p1_spread' in cols_in_row and not pd.isna(p1_spread):
            try:
                idx_rel = cols_in_row.get_loc('rel_p1_spread')
                if p1_spread > 0: styles[idx_rel] = 'spread-positive'
                elif p1_spread < 0: styles[idx_rel] = 'spread-negative'
            except KeyError: pass
        if 'p2_spread' in cols_in_row and not pd.isna(p2_spread):
            try:
                idx_abs = cols_in_row.get_loc('p2_spread')
                if p2_spread > 0: styles[idx_abs] = 'spread-positive'
                elif p2_spread < 0: styles[idx_abs] = 'spread-negative'
            except KeyError: pass
        if 'rel_p2_spread' in cols_in_row and not pd.isna(p2_spread):
            try:
                idx_rel = cols_in_row.get_loc('rel_p2_spread')
                if p2_spread > 0: styles[idx_rel] = 'spread-positive'
                elif p2_spread < 0: styles[idx_rel] = 'spread-negative'
            except KeyError: pass
    except Exception as e_spread: print(f"Warning: Error during spread sign styling: {e_spread}")
    return styles


def generate_comparison_table(df: pd.DataFrame) -> str:
    """Generates the HTML table for odds comparison using Pandas Styler."""
    if df is None or df.empty:
        return format_simple_error_html("No processed comparison data available.", context="comparison table")
    try:
        print("Formatting comparison data for display...")
        cols_to_use = [col for col in COMP_COLS_ORDERED if col in df.columns]
        missing_display_cols = [col for col in COMP_COLS_ORDERED if col not in df.columns]
        if missing_display_cols:
            print(f"Warning: Comparison data missing expected display columns: {', '.join(missing_display_cols)}.")

        df_numeric_original = df[cols_to_use].copy() # Keep original numeric data
        df_display = df[cols_to_use].copy() # Work on this for display content

        # Apply Formatting to df_display
        formatters = {
            'Player1_Match_Prob': '{:.1f}%'.format, 'Player2_Match_Prob': '{:.1f}%'.format,
            'bc_p1_prob': '{:.1f}%'.format, 'bc_p2_prob': '{:.1f}%'.format,
            'Player1_Match_Odds': '{:.2f}'.format, 'Player2_Match_Odds': '{:.2f}'.format,
            'bc_p1_odds': '{:.2f}'.format, 'bc_p2_odds': '{:.2f}'.format,
            'p1_spread': '{:+.2f}'.format, 'p2_spread': '{:+.2f}'.format,
            'rel_p1_spread': '{:+.1%}'.format, 'rel_p2_spread': '{:+.1%}'.format
        }
        for col, fmt in formatters.items():
            if col in df_display.columns:
                 df_display[col] = pd.to_numeric(df_display[col], errors='coerce').map(fmt, na_action='ignore')
        df_display.fillna('-', inplace=True)
        print("Comparison data formatting complete.")

        # Sorting (applied to df_display, then use index to sort df_numeric)
        try:
            round_map = {'R128': 128, 'R64': 64, 'R32': 32, 'R16': 16, 'QF': 8, 'SF': 4, 'F': 2, 'W': 1}
            sort_cols = []
            if 'TournamentName' in df_display.columns: sort_cols.append('TournamentName')
            if 'Round' in df_display.columns:
                df_display['RoundSort'] = df_display['Round'].map(round_map).fillna(999); sort_cols.append('RoundSort')
            if sort_cols:
                df_display.sort_values(by=sort_cols, inplace=True, na_position='last')
                df_numeric = df_numeric_original.loc[df_display.index] # Reorder original numeric using sorted index
                if 'RoundSort' in df_display.columns: df_display.drop(columns=['RoundSort'], inplace=True)
                print(f"Sorted comparison table by: {', '.join(sort_cols).replace('RoundSort', 'Round')}.")
            else:
                print("Warning: Neither 'TournamentName' nor 'Round' column found for sorting comparison table.")
                df_numeric = df_numeric_original # Use original order if no sort happened
        except Exception as e_sort:
            print(f"Warning: Error during comparison table sorting: {e_sort}")
            df_numeric = df_numeric_original # Use original order on error

        # Reset index AFTER sorting and aligning numeric data
        print("Resetting index before applying styles...")
        df_numeric = df_numeric.reset_index(drop=True)
        df_display = df_display.reset_index(drop=True)

        # Apply Styles based on df_numeric (original column names)
        print("Applying styles to comparison table...")
        styler = df_numeric.style.apply(apply_comp_table_styles, axis=1)
        styler.set_table_attributes('class="dataframe comparison-table"')

        # Set styler data using df_display (formatted, sorted, original column names)
        styler.data = df_display

        # Render HTML
        html_table = styler.to_html(index=False, escape=True, na_rep='-', border=0)
        if not html_table or not isinstance(html_table, str):
            return format_simple_error_html("Pandas Styler failed to generate comparison HTML string.")

        # --- Replace original column names with display headers in the final HTML string ---
        print("Renaming headers in HTML output...")
        header_row_match = re.search(r'<thead>.*?<tr>(.*?)</tr>.*?</thead>', html_table, re.DOTALL | re.IGNORECASE)
        if header_row_match:
             header_html = header_row_match.group(1)
             new_header_html = header_html
             for original_name, display_name in COMP_HEADER_MAP.items():
                  # Be careful with regex replacement - match whole header content
                  pattern = r'(<th[^>]*>)\s*' + re.escape(original_name) + r'\s*(</th>)'
                  replacement = r'\1' + display_name + r'\2'
                  new_header_html = re.sub(pattern, replacement, new_header_html, flags=re.IGNORECASE)
             html_table = html_table.replace(header_html, new_header_html)
        else:
             print("Warning: Could not find header row in generated HTML for replacement.")
        # ----------------------------------------------------------------------------------

        print("Comparison HTML table string generated successfully.")
        return html_table

    except KeyError as e:
         print(f"KeyError during Styler processing (likely non-unique index/cols): {e}")
         traceback.print_exc()
         return format_simple_error_html(f"Styler incompatible index/column error: {e}", context="comparison table")
    except Exception as e:
        print(f"Error generating comparison HTML table: {e}")
        traceback.print_exc()
        return format_simple_error_html(f"Unexpected error during comparison table generation: {type(e).__name__}", context="comparison table")

# (generate_strategy_log_table function remains the same, but also use header replacement)
def generate_strategy_log_table(df_log: pd.DataFrame) -> str:
    """Generates the HTML table for the strategy log using Pandas Styler."""
    if df_log is None or df_log.empty:
        return "<p>No strategy log data found or log is empty.</p>"
    try:
        print("Formatting strategy log data for display...")
        cols_to_display = [col for col in LOG_COLS_DISPLAY if col in df_log.columns]
        if 'Edge' not in cols_to_display and 'Edge' in LOG_COLS_DISPLAY:
             df_log['Edge'] = np.nan
             edge_index_target = LOG_COLS_DISPLAY.index('Edge')
             cols_to_display.insert(edge_index_target, 'Edge')

        df_display_log = df_log[cols_to_display].copy() # Use a distinct name

        formatters = {
            'TriggerValue': '{:.3f}'.format, 'BetAmount': '{:.3f}'.format,
            'BetOdds': '{:.2f}'.format, 'SackmannProb': '{:.1f}%'.format,
            'BetcenterProb': '{:.1f}%'.format, 'Edge': '{:+.3f}'.format,
            'ProfitLoss': '{:+.2f}'.format
        }
        for col, fmt in formatters.items():
            if col in df_display_log.columns:
                 df_display_log[col] = pd.to_numeric(df_display_log[col], errors='coerce').map(fmt, na_action='ignore')

        # Don't rename columns on the DataFrame yet
        # header_map = {col: LOG_HEADERS[LOG_COLS_DISPLAY.index(col)] for col in cols_to_display}
        # df_display_log.rename(columns=header_map, inplace=True)

        if 'BetDate' in df_display_log.columns: # Sort by internal name
             df_display_log.sort_values(by='BetDate', ascending=False, inplace=True)

        df_display_log.fillna('-', inplace=True)
        # Reset index before styling
        df_display_log = df_display_log.reset_index(drop=True)
        print("Strategy log formatting complete.")


        print("Generating strategy log HTML table string using Styler...")
        styler_log = df_display_log.style # Style the formatted data directly
        styler_log.set_table_attributes('class="dataframe strategy-log-table"')
        # styler_log.data = df_display_log # Already set
        html_table_log = styler_log.to_html(index=False, escape=True, na_rep='-', border=0)

        if not html_table_log or not isinstance(html_table_log, str):
            return format_simple_error_html("Pandas Styler failed to generate strategy log HTML string.", context="strategy log")

        # --- Replace original column names with display headers in the final HTML string ---
        print("Renaming headers in strategy log HTML output...")
        log_header_row_match = re.search(r'<thead>.*?<tr>(.*?)</tr>.*?</thead>', html_table_log, re.DOTALL | re.IGNORECASE)
        if log_header_row_match:
             log_header_html = log_header_row_match.group(1)
             new_log_header_html = log_header_html
             for original_name, display_name in LOG_HEADER_MAP.items():
                  if original_name in cols_to_display: # Only replace headers that were actually present
                      pattern = r'(<th[^>]*>)\s*' + re.escape(original_name) + r'\s*(</th>)'
                      replacement = r'\1' + display_name + r'\2'
                      new_log_header_html = re.sub(pattern, replacement, new_log_header_html, flags=re.IGNORECASE)
             html_table_log = html_table_log.replace(log_header_html, new_log_header_html)
        else:
             print("Warning: Could not find header row in generated strategy log HTML for replacement.")
        # ----------------------------------------------------------------------------------

        print("Strategy log HTML table string generated successfully.")
        return html_table_log

    except Exception as e:
        print(f"Error generating strategy log HTML table: {e}")
        traceback.print_exc()
        return format_simple_error_html(f"Unexpected error during strategy log table generation: {type(e).__name__}", context="strategy log")


# (generate_full_html_page function remains the same)
def generate_full_html_page(comp_table_html: str, log_table_html: str, timestamp_str: str) -> str:
    """Constructs the entire HTML page with tabs, embedding both tables and timestamp."""
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Tennis Odds Comparison & Strategy Log</title>
    <style>
        /* --- Refined Palette & Layout --- */
        :root {{
            --bg-color: #f8f9fa; --content-bg-color: #ffffff; --text-color: #212529;
            --primary-color: #0d6efd; --header-bg-color: #e9ecef; --header-text-color: #343a40;
            --border-color: #dee2e6; --row-alt-bg-color: #f8f9fa; --hover-bg-color: #e9ecef;
            --shadow-color: rgba(0, 0, 0, 0.06);
            --spread-positive-bg-color: #d1e7dd; --spread-positive-text-color: #0f5132;
            --spread-negative-bg-color: #f8d7da; --spread-negative-text-color: #842029;
            --tab-border-color: var(--border-color); --tab-active-border-color: var(--primary-color);
            --tab-text-color: #495057; --tab-active-text-color: var(--primary-color); --tab-hover-bg: #f1f3f5;
        }}
        body {{ font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; padding: 1.5rem; max-width: 1600px; margin: 1.5rem auto; background-color: var(--bg-color); color: var(--text-color); font-size: 16px; }}
        h1 {{ color: var(--primary-color); border-bottom: 2px solid var(--primary-color); padding-bottom: 0.75rem; margin-bottom: 1.75rem; font-weight: 600; font-size: 1.8em; }}
        h2 {{ margin-top: 0; margin-bottom: 1.25rem; font-weight: 500; font-size: 1.4em; }}
        p {{ margin-bottom: 1rem; font-size: 1em; color: #495057; }}
        p .highlight {{ padding: 2px 5px; border-radius: 4px; font-weight: 500; border: 1px solid var(--border-color); }}
        .tab-container {{ margin-bottom: 0; border-bottom: 1px solid var(--tab-border-color); }}
        .tab-button {{ background-color: transparent; border: none; border-bottom: 4px solid transparent; padding: 0.75rem 1rem; margin-bottom: -1px; cursor: pointer; font-size: 1.05em; color: var(--tab-text-color); transition: border-color 0.2s ease-in-out, color 0.2s ease-in-out; }}
        .tab-button:hover {{ background-color: var(--tab-hover-bg); }}
        .tab-button.active {{ border-bottom-color: var(--tab-active-border-color); color: var(--tab-active-text-color); font-weight: 600; }}
        .tab-content {{ display: none; padding: 1.5rem; background-color: var(--content-bg-color); border: 1px solid var(--border-color); border-top: none; border-radius: 0 0 8px 8px; box-shadow: 0 1px 4px var(--shadow-color); margin-bottom: 1.5rem; }}
        .tab-content.active {{ display: block; }}
        .table-container {{ overflow-x: auto; margin-bottom: 0; border: none; box-shadow: none; border-radius: 0; background: none;}}
        table.dataframe {{ width: 100%; border-collapse: collapse; margin: 0; font-size: 0.88em; }}
        table.dataframe th, table.dataframe td {{ border-width: 0; border-bottom: 1px solid var(--border-color); padding: 0.6rem 0.75rem; text-align: left; vertical-align: middle; white-space: nowrap; }}
        table.comparison-table th:nth-child(n+5), table.comparison-table td:nth-child(n+5) {{ text-align: right; }}
        table.strategy-log-table th:nth-child(n+7), table.strategy-log-table td:nth-child(n+7) {{ text-align: right; }}
        table.strategy-log-table th:nth-child(1), table.strategy-log-table td:nth-child(1) {{ text-align: left; }}
        table.dataframe tbody tr:last-child td {{ border-bottom: none; }}
        table.comparison-table th:nth-child(1), table.comparison-table td:nth-child(1) {{ width: 14%; white-space: normal;}}
        table.comparison-table th:nth-child(3), table.comparison-table td:nth-child(3) {{ width: 14%; white-space: normal; font-weight: 500;}}
        table.comparison-table th:nth-child(4), table.comparison-table td:nth-child(4) {{ width: 14%; white-space: normal; font-weight: 500;}}
        table.comparison-table th:nth-child(2), table.comparison-table td:nth-child(2) {{ width: 3%; }}
        table.comparison-table th:nth-child(n+5):nth-child(-n+8) {{ width: 5.5%; }}
        table.comparison-table th:nth-child(n+9):nth-child(-n+12) {{ width: 5%; }}
        table.comparison-table th:nth-child(13), table.comparison-table td:nth-child(13) {{ width: 4%; }}
        table.comparison-table th:nth-child(14), table.comparison-table td:nth-child(14) {{ width: 4.5%; }}
        table.comparison-table th:nth-child(15), table.comparison-table td:nth-child(15) {{ width: 4%; }}
        table.comparison-table th:nth-child(16), table.comparison-table td:nth-child(16) {{ width: 4.5%; }}
        table.strategy-log-table th:nth-child(1), table.strategy-log-table td:nth-child(1) {{ width: 8%; }}
        table.strategy-log-table th:nth-child(2), table.strategy-log-table td:nth-child(2) {{ width: 8%; }}
        table.strategy-log-table th:nth-child(3), table.strategy-log-table td:nth-child(3) {{ width: 10%; white-space: normal;}}
        table.strategy-log-table th:nth-child(4), table.strategy-log-table td:nth-child(4) {{ width: 10%; white-space: normal;}}
        table.strategy-log-table th:nth-child(5), table.strategy-log-table td:nth-child(5) {{ width: 10%; white-space: normal;}}
        table.strategy-log-table th:nth-child(6), table.strategy-log-table td:nth-child(6) {{ width: 5%; }}
        table.strategy-log-table th:nth-child(n+7) {{ width: 5.5%;}}
        table.dataframe thead th {{ background-color: var(--header-bg-color); color: var(--header-text-color); font-weight: 600; border-bottom: 2px solid var(--border-color); position: sticky; top: 0; z-index: 1; }}
        table.dataframe tbody tr:nth-child(even) td {{ background-color: var(--row-alt-bg-color); }}
        table.dataframe tbody tr:hover td {{ background-color: var(--hover-bg-color) !important; }}
        table.dataframe td.spread-positive {{ background-color: var(--spread-positive-bg-color) !important; color: var(--spread-positive-text-color); font-weight: 600; border-radius: 3px; }}
        table.dataframe td.spread-negative {{ background-color: var(--spread-negative-bg-color) !important; color: var(--spread-negative-text-color); font-weight: 600; border-radius: 3px; }}
        .last-updated {{ margin-top: 1.5rem; padding-top: 1rem; border-top: 1px solid var(--border-color); font-size: 0.9em; color: #6c757d; text-align: center; }}
        @media (max-width: 992px) {{ body {{ padding: 1rem; max-width: 100%; }} h1 {{ font-size: 1.6em; }} h2 {{ font-size: 1.3em; }} table.dataframe {{ font-size: 0.85em; }} table.dataframe th, table.dataframe td {{ padding: 0.5rem 0.4rem; white-space: normal; }} table.dataframe th:nth-child(n), table.dataframe td:nth-child(n) {{ width: auto;}} }}
        @media (max-width: 768px) {{ table.dataframe {{ font-size: 0.8em; }} h1 {{ font-size: 1.4em; }} p {{ font-size: 0.95em; }} }}
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
        <p>Comparison of probabilities and calculated odds from the Tennis Abstract Sackmann model against betting odds scraped from Betcenter.be. 'Spread' columns show the absolute difference between Betcenter odds and Sackmann's calculated odds (Positive = BC odds higher). 'Rel Sprd' shows this difference as a percentage of Sackmann's odds. Cells highlighted (<span class="highlight" style="background-color: var(--spread-positive-bg-color); color: var(--spread-positive-text-color);">Green</span>/<span class="highlight" style="background-color: var(--spread-negative-bg-color); color: var(--spread-negative-text-color);">Red</span>) in the Spread columns indicate the direction of the difference.</p>
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
            for (i = 0; i < tabcontent.length; i++) {{ tabcontent[i].style.display = "none"; tabcontent[i].className = tabcontent[i].className.replace(" active", ""); }}
            tablinks = document.getElementsByClassName("tab-button");
            for (i = 0; i < tablinks.length; i++) {{ tablinks[i].className = tablinks[i].className.replace(" active", ""); }}
            let currentTab = document.getElementById(tabName);
            if (currentTab) {{ currentTab.style.display = "block"; currentTab.className += " active"; }}
            if (evt && evt.currentTarget) {{ evt.currentTarget.className += " active"; }}
        }}
         document.addEventListener('DOMContentLoaded', (event) => {{
            let defaultButton = document.querySelector('.tab-button.active');
            let defaultTabId = defaultButton ? defaultButton.getAttribute('onclick').match(/'([^']+)'/)[1] : 'comparisonTab';
            openTab(null, defaultTabId);
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
    # (Function unchanged)
    comparison_html = format_simple_error_html("Comparison data failed to load or process.", "comparison table")
    log_html = "<p>Strategy log file not found or is empty.</p>"

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

    comparison_table_html, log_table_html = get_main_content_html(data_dir_abs)

    update_time = datetime.now(pytz.timezone('Europe/Brussels')).strftime('%Y-%m-%d %H:%M:%S %Z')
    timestamp_str = f"Last updated: {html.escape(update_time)}"
    print("\nGenerating full HTML page content with tabs...");
    full_html = generate_full_html_page(comparison_table_html, log_table_html, timestamp_str)
    print("Full HTML page content generated.")

    try:
        print(f"Writing generated HTML content to: {output_file_abs}")
        with open(output_file_abs, 'w', encoding='utf-8') as f: f.write(full_html)
        print(f"Successfully wrote generated HTML to {os.path.basename(output_file_abs)}")
    except Exception as e: print(f"CRITICAL ERROR writing final HTML file: {e}"); traceback.print_exc()

    print("\nPage generation process complete.")

