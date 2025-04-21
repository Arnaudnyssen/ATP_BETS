# generate_page.py (v18 - Fix Styler ValueError & NameError + Completed Match Filtering)
# - Returns CSS style strings from apply_comp_table_styles instead of classes.
# - Imports 're' module.
# - Filters completed matches from the comparison table based on results files.

import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob
import pytz
import traceback
import html
from typing import Optional, List, Dict, Any, Tuple
import re # <-- Added import re

# MODIFICATION START: Add imports for key generation helpers
try:
    # Attempt to import functions needed for generating consistent keys
    from process_data import create_merge_key, preprocess_player_name
    print("Successfully imported key generation helpers from process_data.")
except ImportError:
    print("ERROR: Cannot import helper functions from process_data.py. Ensure it's accessible.")
    # Define dummy functions if import fails.
    # This allows the script to run, but filtering of completed matches will likely fail.
    def create_merge_key(text: str) -> str:
        print("Warning: Using dummy 'create_merge_key'. Filtering may not work.")
        # Basic key generation attempt (replace non-alphanumeric, lower)
        return re.sub(r'\W+', '', text).lower() if isinstance(text, str) else ""
    def preprocess_player_name(name: str) -> Tuple[str, str]:
        print("Warning: Using dummy 'preprocess_player_name'. Filtering may not work.")
        # Basic key generation attempt (replace non-alphanumeric, lower)
        key = re.sub(r'\W+', '', name).lower() if isinstance(name, str) else ""
        return name, key # Return original name and basic key
# MODIFICATION END

# --- Constants ---
DATA_DIR = "data_archive"
PROCESSED_CSV_PATTERN = "processed_comparison_*.csv"
RESULTS_CSV_PATTERN_BASE = "match_results_" # Base name for results files (e.g., match_results_YYYYMMDD.csv)
STRATEGY_LOG_FILENAME = "strategy_log.csv"
OUTPUT_HTML_FILE = "index.html"

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
    Applies CSS style strings for spread sign highlighting to specific spread cells.
    Returns CSS strings directly, not class names.
    """
    styles = [''] * len(row.index)
    cols_in_row = row.index

    # Define CSS style strings (using CSS variables defined in HTML <style>)
    positive_style = "background-color: var(--spread-positive-bg-color); color: var(--spread-positive-text-color); font-weight: 600; border-radius: 3px;"
    negative_style = "background-color: var(--spread-negative-bg-color); color: var(--spread-negative-text-color); font-weight: 600; border-radius: 3px;"

    try:
        p1_spread = pd.to_numeric(row.get('p1_spread'), errors='coerce')
        p2_spread = pd.to_numeric(row.get('p2_spread'), errors='coerce')

        # Apply to absolute spread column P1
        if 'p1_spread' in cols_in_row and not pd.isna(p1_spread):
            try:
                idx_abs = cols_in_row.get_loc('p1_spread')
                if p1_spread > 0: styles[idx_abs] = positive_style
                elif p1_spread < 0: styles[idx_abs] = negative_style
            except KeyError: pass

        # Apply to relative spread column P1
        if 'rel_p1_spread' in cols_in_row and not pd.isna(p1_spread):
            try:
                idx_rel = cols_in_row.get_loc('rel_p1_spread')
                if p1_spread > 0: styles[idx_rel] = positive_style
                elif p1_spread < 0: styles[idx_rel] = negative_style
            except KeyError: pass

        # Apply to absolute spread column P2
        if 'p2_spread' in cols_in_row and not pd.isna(p2_spread):
            try:
                idx_abs = cols_in_row.get_loc('p2_spread')
                if p2_spread > 0: styles[idx_abs] = positive_style
                elif p2_spread < 0: styles[idx_abs] = negative_style
            except KeyError: pass

        # Apply to relative spread column P2
        if 'rel_p2_spread' in cols_in_row and not pd.isna(p2_spread):
            try:
                idx_rel = cols_in_row.get_loc('rel_p2_spread')
                if p2_spread > 0: styles[idx_rel] = positive_style
                elif p2_spread < 0: styles[idx_rel] = negative_style
            except KeyError: pass

    except Exception as e_spread: print(f"Warning: Error during spread sign styling: {e_spread}")

    return styles


def generate_comparison_table(df: pd.DataFrame) -> str:
    """Generates the HTML table for odds comparison using Pandas Styler."""
    if df is None or df.empty:
        # Check if the original dataframe before filtering was empty or if filtering removed all rows
        # This function now receives the potentially filtered dataframe.
        return format_simple_error_html("No comparison data available to display (possibly all matches completed or data missing).", context="comparison table")
    try:
        print("Formatting comparison data for display...")
        # Use only columns defined in COMP_COLS_ORDERED that actually exist in the (filtered) df
        cols_to_use = [col for col in COMP_COLS_ORDERED if col in df.columns]
        missing_display_cols = [col for col in COMP_COLS_ORDERED if col not in df.columns]
        if missing_display_cols:
            print(f"Warning: Comparison data missing expected display columns: {', '.join(missing_display_cols)}.")

        # Create a copy for numeric operations (styling) and one for display formatting
        # Important: Operate on the potentially filtered df passed to this function
        df_numeric_original = df[cols_to_use].copy()
        df_display = df[cols_to_use].copy()

        # Apply formatting to the display DataFrame
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
                 # Ensure column is numeric before formatting
                 df_display[col] = pd.to_numeric(df_display[col], errors='coerce').map(fmt, na_action='ignore')
        df_display.fillna('-', inplace=True) # Replace remaining NaNs with '-' for display
        print("Comparison data formatting complete.")

        # Sorting logic (applied to both dataframes to keep them aligned)
        try:
            round_map = {'R128': 128, 'R64': 64, 'R32': 32, 'R16': 16, 'QF': 8, 'SF': 4, 'F': 2, 'W': 1}
            sort_cols = []
            if 'TournamentName' in df_display.columns: sort_cols.append('TournamentName')
            if 'Round' in df_display.columns:
                # Create sort key on the display df, apply sort order to both
                df_display['RoundSort'] = df_display['Round'].map(round_map).fillna(999)
                sort_cols.append('RoundSort')
            if sort_cols:
                # Get the sorted index from the display df
                sorted_index = df_display.sort_values(by=sort_cols, na_position='last').index
                # Apply this index to both dataframes
                df_display = df_display.loc[sorted_index]
                df_numeric = df_numeric_original.loc[sorted_index] # Use original numeric data for styling
                if 'RoundSort' in df_display.columns: df_display.drop(columns=['RoundSort'], inplace=True)
                print(f"Sorted comparison table by: {', '.join(sort_cols).replace('RoundSort', 'Round')}.")
            else:
                print("Warning: Neither 'TournamentName' nor 'Round' column found for sorting comparison table.")
                df_numeric = df_numeric_original # Keep original order if no sort columns
        except Exception as e_sort:
            print(f"Warning: Error during comparison table sorting: {e_sort}")
            df_numeric = df_numeric_original # Revert to original numeric data on sort error

        # Reset index after sorting to ensure alignment before applying styles row-wise
        print("Resetting index before applying styles...")
        df_numeric = df_numeric.reset_index(drop=True)
        df_display = df_display.reset_index(drop=True)

        print("Applying styles to comparison table...")
        # Apply styling function to the numeric data (df_numeric)
        styler = df_numeric.style.apply(apply_comp_table_styles, axis=1)
        styler.set_table_attributes('class="dataframe comparison-table"')
        # Set the *display data* (formatted strings) for the final HTML output
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
             # Use the COMP_HEADER_MAP based on the columns actually present (cols_to_use)
             current_header_map = {orig: COMP_HEADER_MAP.get(orig, orig) for orig in cols_to_use}
             for original_name, display_name in current_header_map.items():
                  # More robust regex to match th tags correctly
                  pattern = r'(<th(?:[^>]*\sclass="[^"]*col_heading[^"]*"|[^>]*)>)\s*' + re.escape(original_name) + r'\s*(</th>)'
                  replacement = r'\1' + display_name + r'\2'
                  # Simpler substitution might work if Styler doesn't add complex classes
                  # pattern_simple = r'>' + re.escape(original_name) + r'<'
                  # replacement_simple = '>' + display_name + '<'
                  new_header_html = re.sub(pattern, replacement, new_header_html, flags=re.IGNORECASE)
             html_table = html_table.replace(header_html, new_header_html)
        else:
             print("Warning: Could not find header row in generated HTML for replacement.")
        # ----------------------------------------------------------------------------------

        print("Comparison HTML table string generated successfully.")
        return html_table

    except ValueError as e:
         print(f"ValueError during Styler processing: {e}")
         traceback.print_exc()
         if "Styles supplied as string must follow CSS rule formats" in str(e):
              return format_simple_error_html(f"Styler formatting error: {e}", context="comparison table")
         else:
              return format_simple_error_html(f"Unexpected ValueError during comparison table generation: {e}", context="comparison table")
    except Exception as e:
        print(f"Error generating comparison HTML table: {e}")
        traceback.print_exc()
        return format_simple_error_html(f"Unexpected error during comparison table generation: {type(e).__name__}", context="comparison table")


def generate_strategy_log_table(df_log: pd.DataFrame) -> str:
    """Generates the HTML table for the strategy log using Pandas Styler."""
    if df_log is None or df_log.empty:
        return "<p>No strategy log data found or log is empty.</p>"
    try:
        print("Formatting strategy log data for display...")
        # Use only columns defined in LOG_COLS_DISPLAY that exist in df_log
        cols_to_display = [col for col in LOG_COLS_DISPLAY if col in df_log.columns]
        # If 'Edge' is expected but missing, add it as NaN for consistent display
        if 'Edge' not in cols_to_display and 'Edge' in LOG_COLS_DISPLAY:
             df_log['Edge'] = np.nan # Add the column if missing
             edge_index_target = LOG_COLS_DISPLAY.index('Edge')
             # Insert 'Edge' into the display list at the correct position
             cols_to_display.insert(edge_index_target, 'Edge')

        df_display_log = df_log[cols_to_display].copy() # Use the potentially modified cols_to_display

        # Format numeric columns
        formatters = {
            'TriggerValue': '{:.3f}'.format, 'BetAmount': '{:.3f}'.format,
            'BetOdds': '{:.2f}'.format, 'SackmannProb': '{:.1f}%'.format,
            'BetcenterProb': '{:.1f}%'.format, 'Edge': '{:+.3f}'.format,
            'ProfitLoss': '{:+.2f}'.format
        }
        for col, fmt in formatters.items():
            if col in df_display_log.columns:
                 df_display_log[col] = pd.to_numeric(df_display_log[col], errors='coerce').map(fmt, na_action='ignore')

        df_display_log.fillna('-', inplace=True)
        df_display_log = df_display_log.reset_index(drop=True)
        print("Strategy log formatting complete.")

        print("Generating strategy log HTML table string using Styler...")
        styler_log = df_display_log.style # Use the formatted display data
        styler_log.set_table_attributes('class="dataframe strategy-log-table"')
        html_table_log = styler_log.to_html(index=False, escape=True, na_rep='-', border=0)

        if not html_table_log or not isinstance(html_table_log, str):
            return format_simple_error_html("Pandas Styler failed to generate strategy log HTML string.", context="strategy log")

        print("Renaming headers in strategy log HTML output...")
        log_header_row_match = re.search(r'<thead>.*?<tr>(.*?)</tr>.*?</thead>', html_table_log, re.DOTALL | re.IGNORECASE)
        if log_header_row_match:
             log_header_html = log_header_row_match.group(1)
             new_log_header_html = log_header_html
             # Use the LOG_HEADER_MAP based on the columns actually displayed (cols_to_display)
             current_log_header_map = {orig: LOG_HEADER_MAP.get(orig, orig) for orig in cols_to_display}
             for original_name, display_name in current_log_header_map.items():
                  # Use the more robust pattern for header replacement
                  pattern = r'(<th(?:[^>]*\sclass="[^"]*col_heading[^"]*"|[^>]*)>)\s*' + re.escape(original_name) + r'\s*(</th>)'
                  replacement = r'\1' + display_name + r'\2'
                  new_log_header_html = re.sub(pattern, replacement, new_log_header_html, flags=re.IGNORECASE)
             html_table_log = html_table_log.replace(log_header_html, new_log_header_html)
        else:
             print("Warning: Could not find header row in generated strategy log HTML for replacement.")

        print("Strategy log HTML table string generated successfully.")
        return html_table_log

    except Exception as e:
        print(f"Error generating strategy log HTML table: {e}")
        traceback.print_exc()
        return format_simple_error_html(f"Unexpected error during strategy log table generation: {type(e).__name__}", context="strategy log")


def generate_full_html_page(comp_table_html: str, log_table_html: str, timestamp_str: str) -> str:
    """Constructs the entire HTML page with tabs, embedding both tables and timestamp."""
    # --- Updated CSS - Removed spread classes ---
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
            /* Cell Highlighting Colors (used directly in styles now) */
            --spread-positive-bg-color: #d1e7dd; --spread-positive-text-color: #0f5132;
            --spread-negative-bg-color: #f8d7da; --spread-negative-text-color: #842029;
            /* Tab Colors */
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
        table.strategy-log-table th:nth-child(1), table.strategy-log-table td:nth-child(1) {{ text-align: left; }} /* Date column align left */
        table.dataframe tbody tr:last-child td {{ border-bottom: none; }}
        /* Column Widths for Comparison Table (Adjust as needed) */
        table.comparison-table th:nth-child(1), table.comparison-table td:nth-child(1) {{ width: 14%; white-space: normal;}} /* Tournament */
        table.comparison-table th:nth-child(3), table.comparison-table td:nth-child(3) {{ width: 14%; white-space: normal; font-weight: 500;}} /* Player 1 */
        table.comparison-table th:nth-child(4), table.comparison-table td:nth-child(4) {{ width: 14%; white-space: normal; font-weight: 500;}} /* Player 2 */
        table.comparison-table th:nth-child(2), table.comparison-table td:nth-child(2) {{ width: 3%; }} /* Round */
        table.comparison-table th:nth-child(n+5):nth-child(-n+8) {{ width: 5.5%; }} /* Probs */
        table.comparison-table th:nth-child(n+9):nth-child(-n+12) {{ width: 5%; }} /* Odds */
        table.comparison-table th:nth-child(13), table.comparison-table td:nth-child(13) {{ width: 4%; }} /* P1 Sprd */
        table.comparison-table th:nth-child(14), table.comparison-table td:nth-child(14) {{ width: 4.5%; }} /* Rel Sprd */
        table.comparison-table th:nth-child(15), table.comparison-table td:nth-child(15) {{ width: 4%; }} /* P2 Sprd */
        table.comparison-table th:nth-child(16), table.comparison-table td:nth-child(16) {{ width: 4.5%; }} /* Rel Sprd */
        /* Column Widths for Strategy Log Table (Adjust as needed) */
        table.strategy-log-table th:nth-child(1), table.strategy-log-table td:nth-child(1) {{ width: 8%; }} /* Date */
        table.strategy-log-table th:nth-child(2), table.strategy-log-table td:nth-child(2) {{ width: 8%; }} /* Strategy */
        table.strategy-log-table th:nth-child(3), table.strategy-log-table td:nth-child(3) {{ width: 10%; white-space: normal;}} /* Tournament */
        table.strategy-log-table th:nth-child(4), table.strategy-log-table td:nth-child(4) {{ width: 10%; white-space: normal;}} /* P1 */
        table.strategy-log-table th:nth-child(5), table.strategy-log-table td:nth-child(5) {{ width: 10%; white-space: normal;}} /* P2 */
        table.strategy-log-table th:nth-child(6), table.strategy-log-table td:nth-child(6) {{ width: 5%; }} /* Bet On */
        table.strategy-log-table th:nth-child(n+7) {{ width: 5.5%;}} /* Numeric cols */
        table.dataframe thead th {{ background-color: var(--header-bg-color); color: var(--header-text-color); font-weight: 600; border-bottom: 2px solid var(--border-color); position: sticky; top: 0; z-index: 1; }}
        table.dataframe tbody tr:nth-child(even) td {{ background-color: var(--row-alt-bg-color); }}
        table.dataframe tbody tr:hover td {{ background-color: var(--hover-bg-color) !important; }}
        /* Removed spread-positive / spread-negative classes as styles are now inline */
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
        <p>Matches identified as completed (based on corresponding daily results files) are filtered out. Name matching relies on standardized keys and may not be perfect.</p>
        <div class="table-container">{comp_table_html}</div>
    </div>
    <div id="logTab" class="tab-content">
        <h2>Strategy Log</h2>
        <p>Record of hypothetical bets identified by different strategies based on the daily odds comparison. Profit/Loss calculation depends on actual match results being available and processed.</p>
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
         // Ensure the default tab is shown on load
         document.addEventListener('DOMContentLoaded', (event) => {{
            // Find the button marked as active in the HTML
            let defaultButton = document.querySelector('.tab-button.active');
            // Extract tab ID from its onclick attribute, default to 'comparisonTab' if not found
            let defaultTabId = defaultButton ? defaultButton.getAttribute('onclick').match(/'([^']+)'/)[1] : 'comparisonTab';
            // Open the default tab without needing a click event
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
    Filters comparison data to remove completed matches if results are found.
    Returns tuple: (comparison_table_html, log_table_html)
    """
    comparison_html = format_simple_error_html("Comparison data failed to load or process.", "comparison table")
    log_html = "<p>Strategy log file not found or is empty.</p>" # Default log HTML

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
                 df_comparison_original_count = len(df_comparison) # Store count before filtering

                 # MODIFICATION START: Filter completed matches
                 try:
                     # Extract date (YYYYMMDD) from the comparison filename
                     date_match = re.search(r'_(\d{8})\.csv$', os.path.basename(latest_processed_csv))
                     if date_match:
                         comparison_date_str = date_match.group(1)
                         # Construct the expected results filename for the same date
                         results_filename = f"{RESULTS_CSV_PATTERN_BASE}{comparison_date_str}.csv"
                         results_filepath = os.path.join(data_dir, results_filename)
                         print(f"Checking for corresponding results file: {results_filename}")

                         if os.path.exists(results_filepath):
                             print(f"Found results file. Loading to filter completed matches...")
                             df_results = pd.read_csv(results_filepath)
                             if not df_results.empty and 'WinnerName' in df_results.columns and 'LoserName' in df_results.columns:
                                 # --- Key Generation ---
                                 # Ensure necessary keys exist or can be generated in both dataframes.
                                 # Results keys (assuming results scraper adds them or they are generated here)
                                 if 'WinnerNameKey' not in df_results.columns:
                                     df_results['WinnerNameKey'] = df_results['WinnerName'].apply(lambda x: preprocess_player_name(x)[1])
                                 if 'LoserNameKey' not in df_results.columns:
                                     df_results['LoserNameKey'] = df_results['LoserName'].apply(lambda x: preprocess_player_name(x)[1])
                                 # Attempt to get/create TournamentKey in results (CRUCIAL for accurate match key)
                                 # Best practice: Ensure results scraper includes a TournamentKey matching the comparison data.
                                 if 'TournamentKey' not in df_results.columns:
                                     # Try to derive from TournamentName if available in results
                                     if 'TournamentName' in df_results.columns:
                                         print("Generating TournamentKey in results from TournamentName.")
                                         df_results['TournamentKey'] = df_results['TournamentName'].apply(create_merge_key)
                                     else:
                                         print("Warning: 'TournamentKey' and 'TournamentName' missing in results file. Cannot reliably filter.")
                                         df_results = pd.DataFrame() # Prevent filtering if keys missing

                                 # Comparison keys
                                 if 'TournamentKey' not in df_comparison.columns and 'TournamentName' in df_comparison.columns:
                                      df_comparison['TournamentKey'] = df_comparison['TournamentName'].apply(create_merge_key)
                                 if 'Player1NameKey' not in df_comparison.columns and 'Player1Name' in df_comparison.columns:
                                      df_comparison['Player1NameKey'] = df_comparison['Player1Name'].apply(lambda x: preprocess_player_name(x)[1])
                                 if 'Player2NameKey' not in df_comparison.columns and 'Player2Name' in df_comparison.columns:
                                      df_comparison['Player2NameKey'] = df_comparison['Player2Name'].apply(lambda x: preprocess_player_name(x)[1])

                                 # --- Filtering Logic ---
                                 # Check if all required keys are now present in both dataframes
                                 comp_keys_ok = all(k in df_comparison.columns for k in ['TournamentKey', 'Player1NameKey', 'Player2NameKey'])
                                 res_keys_ok = not df_results.empty and all(k in df_results.columns for k in ['TournamentKey', 'WinnerNameKey', 'LoserNameKey'])

                                 if comp_keys_ok and res_keys_ok:
                                     print("Generating match keys for filtering...")
                                     # Create MatchKey (TournamentKey + sorted PlayerKeys) in both DFs
                                     df_comparison['MatchKey'] = df_comparison.apply(
                                         lambda row: f"{row['TournamentKey']}_" + "_".join(sorted([row['Player1NameKey'], row['Player2NameKey']])), axis=1
                                     )
                                     df_results['MatchKey'] = df_results.apply(
                                         lambda row: f"{row['TournamentKey']}_" + "_".join(sorted([row['WinnerNameKey'], row['LoserNameKey']])), axis=1
                                     )

                                     # Get the set of unique keys for completed matches
                                     completed_match_keys = set(df_results['MatchKey'].unique())
                                     print(f"  Identified {len(completed_match_keys)} unique completed match keys.")

                                     # Filter the comparison dataframe
                                     df_comparison = df_comparison[~df_comparison['MatchKey'].isin(completed_match_keys)]
                                     filtered_rows = len(df_comparison)
                                     print(f"  Filtered out {df_comparison_original_count - filtered_rows} completed matches. Remaining: {filtered_rows}")

                                     # Optional: Drop helper key columns after filtering if desired
                                     # df_comparison = df_comparison.drop(columns=['TournamentKey', 'Player1NameKey', 'Player2NameKey', 'MatchKey'], errors='ignore')

                                 else:
                                     print("Warning: Could not generate all required keys in comparison or results data. Skipping filtering.")
                             else:
                                 print("Results file is empty or missing required name columns. Skipping filtering.")
                         else:
                             print(f"  Results file '{results_filename}' not found. Displaying all matches.")
                     else:
                         print("Warning: Could not extract date from comparison filename. Skipping filtering.")

                 except Exception as e_filter:
                     print(f"Error during completed match filtering: {e_filter}")
                     traceback.print_exc()
                     # Ensure df_comparison is still defined even if filtering fails
                     if 'df_comparison' not in locals():
                         df_comparison = pd.DataFrame() # Set to empty if error occurred before assignment
                 # MODIFICATION END

                 # Generate HTML table using the (potentially filtered) df_comparison
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

    # --- Load and process strategy log (remains unchanged) ---
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
            log_html = "<p>Strategy log file not found.</p>" # More specific message
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

    # Get HTML for both tables (comparison table is now filtered inside this function)
    comparison_table_html, log_table_html = get_main_content_html(data_dir_abs)

    # Get timestamp for the page footer
    update_time = datetime.now(pytz.timezone('Europe/Brussels')).strftime('%Y-%m-%d %H:%M:%S %Z') # Use your local timezone
    timestamp_str = f"Last updated: {html.escape(update_time)}"
    print("\nGenerating full HTML page content with tabs...");
    full_html = generate_full_html_page(comparison_table_html, log_table_html, timestamp_str)
    print("Full HTML page content generated.")

    # Write the final HTML file
    try:
        print(f"Writing generated HTML content to: {output_file_abs}")
        with open(output_file_abs, 'w', encoding='utf-8') as f: f.write(full_html)
        print(f"Successfully wrote generated HTML to {os.path.basename(output_file_abs)}")
    except Exception as e: print(f"CRITICAL ERROR writing final HTML file: {e}"); traceback.print_exc()

    print("\nPage generation process complete.")
