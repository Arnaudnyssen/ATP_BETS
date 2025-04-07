# generate_page.py (Integrates Scooore odds, filters qualifiers, highlights value)

import pandas as pd
from datetime import datetime
import os
import glob
import pytz
import traceback
import re # Added for name preprocessing
from typing import Optional, List, Tuple, Any
import html
import numpy as np # For isnan checks

# --- Constants ---
ERROR_MESSAGE_CLASS = "error-message"
DATA_DIR = "data_archive"
SACKMANN_CSV_PATTERN = "sackmann_matchups_*.csv"
SCOOORE_CSV_PATTERN = "scooore_odds_*.csv" # Pattern for Scooore files
OUTPUT_HTML_FILE = "index.html"
VALUE_BET_THRESHOLD = 1.10 # Highlight if Scooore odds are >= 110% of Sackmann odds

# --- Updated Column order and headers ---
# No change needed here, highlighting is done via CSS classes
DISPLAY_COLS_ORDERED = [
    'TournamentName', 'Round',
    'Player1Name', 'Player2Name',
    'Player1_Match_Prob', 'Player2_Match_Prob', # Sackmann Probabilities
    'Player1_Match_Odds', 'Player2_Match_Odds', # Sackmann Calculated Odds
    'p1_odds', 'p2_odds'                        # Scooore Odds
]
DISPLAY_HEADERS = [
    "Tournament", "Round",
    "Player 1", "Player 2",
    "P1 Prob (Sack.)", "P2 Prob (Sack.)", # Clarify source
    "P1 Odds (Sack.)", "P2 Odds (Sack.)", # Clarify source
    "P1 Odds (Scooore)", "P2 Odds (Scooore)" # Add Scooore headers
]

# --- Helper Functions (preprocess_player_name, find_latest_csv, format_error_html_for_table) ---
# (These functions remain unchanged from the previous version)
def preprocess_player_name(name: str) -> str:
    """Standardizes a single player name string."""
    if not isinstance(name, str): return ""
    try:
        name = re.sub(r'\s*\([^)]*\)', '', name).strip()
        name = re.sub(r'^\*|\*$', '', name).strip()
        name = name.title()
        return name
    except Exception as e:
        print(f"Warning: Could not preprocess name '{name}': {e}")
        return name

def find_latest_csv(directory: str, pattern: str) -> Optional[str]:
    """Finds the most recently modified CSV file matching the pattern."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        search_dir = os.path.join(script_dir, directory)
        search_path = os.path.join(search_dir, pattern)
        print(f"Searching for pattern: {search_path}")
        list_of_files = glob.glob(search_path)
        if not list_of_files: return None
        list_of_files = [f for f in list_of_files if os.path.isfile(f)]
        if not list_of_files: return None
        latest_file = max(list_of_files, key=os.path.getmtime)
        print(f"Found latest CSV file: {latest_file} (Full path: {os.path.abspath(latest_file)})")
        return latest_file
    except Exception as e:
        print(f"Error finding latest CSV file in '{directory}': {e}")
        traceback.print_exc()
        return None

def format_error_html_for_table(message: str) -> str:
    """Formats an error message as an HTML snippet."""
    print(f"Error generating table: {message}")
    return f'<div class="{ERROR_MESSAGE_CLASS}" style="padding: 20px;">{html.escape(message)} Check logs for details.</div>'

# --- Data Loading Functions (load_and_prepare_sackmann_data, load_and_prepare_scooore_data) ---
# (These functions remain unchanged from the previous version)
def load_and_prepare_sackmann_data(csv_filepath: str) -> Optional[pd.DataFrame]:
    """Loads, preprocesses (already done mostly), and filters Sackmann data."""
    abs_csv_filepath = os.path.abspath(csv_filepath)
    print(f"Loading Sackmann data from: {abs_csv_filepath}")
    if not os.path.exists(csv_filepath) or os.path.getsize(csv_filepath) == 0: return None
    try:
        df = pd.read_csv(csv_filepath)
        if df.empty: return None
        print(f"Read {len(df)} rows initially from Sackmann CSV.")
        df['Player1_Match_Prob'] = pd.to_numeric(df['Player1_Match_Prob'], errors='coerce')
        df['Player2_Match_Prob'] = pd.to_numeric(df['Player2_Match_Prob'], errors='coerce')
        original_count_step1 = len(df)
        df = df[ (df['Player1_Match_Prob'].notna()) & (df['Player1_Match_Prob'] > 0.0) & (df['Player1_Match_Prob'] < 100.0) & (df['Player2_Match_Prob'].notna()) & (df['Player2_Match_Prob'] > 0.0) & (df['Player2_Match_Prob'] < 100.0) ].copy()
        print(f"Filtered Sackmann (Prob = 0%, 100%, NaN): {original_count_step1 - len(df)} rows removed. {len(df)} remain.")
        df['Player1Name'] = df['Player1Name'].astype(str).apply(preprocess_player_name)
        df['Player2Name'] = df['Player2Name'].astype(str).apply(preprocess_player_name)
        original_count_step2 = len(df)
        mask_p1_qualifier = df['Player1Name'].str.contains('Qualifier', case=False, na=False)
        mask_p2_qualifier = df['Player2Name'].str.contains('Qualifier', case=False, na=False)
        df = df[~(mask_p1_qualifier | mask_p2_qualifier)].copy()
        print(f"Filtered Sackmann (Qualifiers): {original_count_step2 - len(df)} rows removed. {len(df)} remain.")
        if df.empty: return None
        sackmann_cols = ['TournamentName', 'Round', 'Player1Name', 'Player2Name', 'Player1_Match_Prob', 'Player2_Match_Prob', 'Player1_Match_Odds', 'Player2_Match_Odds']
        df = df[[col for col in sackmann_cols if col in df.columns]]
        print(f"Prepared Sackmann data. Shape: {df.shape}")
        return df
    except Exception as e:
        print(f"Error loading/preparing Sackmann data: {e}"); traceback.print_exc(); return None

def load_and_prepare_scooore_data(csv_filepath: str) -> Optional[pd.DataFrame]:
    """Loads and preprocesses Scooore data."""
    abs_csv_filepath = os.path.abspath(csv_filepath)
    print(f"Loading Scooore data from: {abs_csv_filepath}")
    if not os.path.exists(csv_filepath) or os.path.getsize(csv_filepath) == 0: return None
    try:
        df = pd.read_csv(csv_filepath)
        if df.empty: return None
        print(f"Read {len(df)} rows initially from Scooore CSV.")
        required_cols = ['p1_name', 'p2_name', 'p1_odds', 'p2_odds']
        if not all(col in df.columns for col in required_cols):
             print(f"Error: Scooore DataFrame missing required columns. Found: {df.columns.tolist()}"); return None
        df['Player1Name'] = df['p1_name'].apply(preprocess_player_name)
        df['Player2Name'] = df['p2_name'].apply(preprocess_player_name)
        scooore_cols = ['Player1Name', 'Player2Name', 'p1_odds', 'p2_odds']
        df = df[[col for col in scooore_cols if col in df.columns]].copy()
        print(f"Prepared Scooore data. Shape: {df.shape}")
        return df
    except Exception as e:
        print(f"Error loading/preparing Scooore data: {e}"); traceback.print_exc(); return None

# --- Merge Function (merge_data) ---
# (This function remains unchanged from the previous version)
def merge_data(sackmann_df: pd.DataFrame, scooore_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Merges Sackmann and Scooore dataframes."""
    if scooore_df is None or scooore_df.empty:
        print("Scooore data is missing or empty. Returning only Sackmann data.")
        sackmann_df['p1_odds'] = pd.NA
        sackmann_df['p2_odds'] = pd.NA
        return sackmann_df
    print("Attempting to merge Sackmann and Scooore data...")
    try:
        merged_df = pd.merge(sackmann_df, scooore_df, on=['Player1Name', 'Player2Name'], how='outer')
        print(f"Merged (P1-P1, P2-P2). Shape: {merged_df.shape}")
        unmatched_scooore = merged_df[merged_df['Player1_Match_Prob'].isna() & merged_df['p1_odds'].notna()].copy()
        unmatched_sackmann = merged_df[merged_df['p1_odds'].isna() & merged_df['Player1_Match_Prob'].notna()].copy()
        if not unmatched_scooore.empty and not unmatched_sackmann.empty:
            print(f"Found {len(unmatched_scooore)} unmatched Scooore rows and {len(unmatched_sackmann)} unmatched Sackmann rows. Attempting swapped merge...")
            swapped_scooore = unmatched_scooore[['Player2Name', 'Player1Name', 'p2_odds', 'p1_odds']].copy()
            swapped_scooore.columns = ['Player1Name', 'Player2Name', 'p1_odds', 'p2_odds']
            swapped_merge = pd.merge(unmatched_sackmann.drop(columns=['p1_odds', 'p2_odds']), swapped_scooore, on=['Player1Name', 'Player2Name'], how='inner')
            print(f"Found {len(swapped_merge)} matches via swapped merge.")
            if not swapped_merge.empty:
                merged_df.set_index(['Player1Name', 'Player2Name'], inplace=True)
                swapped_merge.set_index(['Player1Name', 'Player2Name'], inplace=True)
                merged_df.update(swapped_merge[['p1_odds', 'p2_odds']])
                merged_df.reset_index(inplace=True)
                print("Updated main dataframe with swapped matches.")
        print(f"Final merged data shape: {merged_df.shape}")
        return merged_df
    except Exception as e:
        print(f"Error during data merging: {e}"); traceback.print_exc()
        sackmann_df['p1_odds'] = pd.NA; sackmann_df['p2_odds'] = pd.NA
        return sackmann_df

# --- HTML Generation ---

def highlight_value_bets(row: pd.Series) -> List[str]:
    """
    Applies CSS classes to highlight potential value bets based on odds comparison.
    Takes a row of the DataFrame as input.
    Returns a list of CSS class strings, one for each column in the row.
    """
    # Default style (no specific class)
    styles = [''] * len(row.index)

    # Convert odds to numeric for comparison, handle errors/placeholders
    try:
        sack_odds_p1 = pd.to_numeric(row.get('Player1_Match_Odds'), errors='coerce')
        scooore_odds_p1 = pd.to_numeric(row.get('p1_odds'), errors='coerce')
        sack_odds_p2 = pd.to_numeric(row.get('Player2_Match_Odds'), errors='coerce')
        scooore_odds_p2 = pd.to_numeric(row.get('p2_odds'), errors='coerce')
    except Exception: # Catch any conversion issue
        return styles # Return default styles if conversion fails

    # Check for P1 value
    if not pd.isna(sack_odds_p1) and not pd.isna(scooore_odds_p1) and scooore_odds_p1 >= sack_odds_p1 * VALUE_BET_THRESHOLD:
        try:
            # Find the index of the 'p1_odds' column (Scooore P1 Odds)
            p1_odds_idx = row.index.get_loc('p1_odds')
            styles[p1_odds_idx] = 'value-bet-p1' # Apply class to P1 Scooore cell
        except KeyError:
            pass # Column not found

    # Check for P2 value
    if not pd.isna(sack_odds_p2) and not pd.isna(scooore_odds_p2) and scooore_odds_p2 >= sack_odds_p2 * VALUE_BET_THRESHOLD:
        try:
            # Find the index of the 'p2_odds' column (Scooore P2 Odds)
            p2_odds_idx = row.index.get_loc('p2_odds')
            styles[p2_odds_idx] = 'value-bet-p2' # Apply class to P2 Scooore cell
        except KeyError:
            pass # Column not found

    return styles


def generate_html_table(df: pd.DataFrame) -> str:
    """
    Formats the merged DataFrame, sorts, selects/reorders columns, applies value
    highlighting, and generates an HTML table string using Pandas Styler.
    Returns error HTML string on failure.
    """
    if df is None or df.empty:
         return format_error_html_for_table("No combined match data available to display.")

    try:
        print("Formatting final merged data for display...")
        # Keep numeric copies for highlighting logic before formatting for display
        df_numeric = df.copy()
        df_numeric['Player1_Match_Odds'] = pd.to_numeric(df_numeric['Player1_Match_Odds'], errors='coerce')
        df_numeric['Player2_Match_Odds'] = pd.to_numeric(df_numeric['Player2_Match_Odds'], errors='coerce')
        df_numeric['p1_odds'] = pd.to_numeric(df_numeric['p1_odds'], errors='coerce')
        df_numeric['p2_odds'] = pd.to_numeric(df_numeric['p2_odds'], errors='coerce')


        # --- Apply Display Formatting ---
        # Format probabilities
        df['Player1_Match_Prob'] = pd.to_numeric(df['Player1_Match_Prob'], errors='coerce').map('{:.1f}%'.format, na_action='ignore')
        df['Player2_Match_Prob'] = pd.to_numeric(df['Player2_Match_Prob'], errors='coerce').map('{:.1f}%'.format, na_action='ignore')
        # Format Sackmann odds
        df['Player1_Match_Odds'] = pd.to_numeric(df['Player1_Match_Odds'], errors='coerce').map('{:.2f}'.format, na_action='ignore')
        df['Player2_Match_Odds'] = pd.to_numeric(df['Player2_Match_Odds'], errors='coerce').map('{:.2f}'.format, na_action='ignore')
        # Format Scooore odds
        df['p1_odds'] = pd.to_numeric(df['p1_odds'], errors='coerce').map('{:.2f}'.format, na_action='ignore')
        df['p2_odds'] = pd.to_numeric(df['p2_odds'], errors='coerce').map('{:.2f}'.format, na_action='ignore')

        # Fill any remaining NaNs with '-' AFTER formatting
        df.fillna('-', inplace=True)
        print("Data formatting complete.")

        try:
            # Sort by Tournament and Round
            df.sort_values(by=['TournamentName', 'Round'], inplace=True, na_position='last', key=lambda x: pd.to_numeric(x.astype(str).str.replace('R','', regex=False).replace('QF','4', regex=False).replace('SF','2', regex=False).replace('F','1', regex=False).replace('W','0', regex=False), errors='coerce') if x.name == 'Round' else x)
            # Apply the same sort order to the numeric dataframe used for styling
            df_numeric = df_numeric.loc[df.index]
            print("Sorted matchups by Tournament and Round.")
        except Exception as e:
             print(f"Warning: Error during sorting: {e}")

        # Check if all necessary display columns exist
        missing_display_cols = [col for col in DISPLAY_COLS_ORDERED if col not in df.columns]
        if missing_display_cols:
            return format_error_html_for_table(f"Data is missing columns needed for display: {', '.join(missing_display_cols)}. Check merge logic and `DISPLAY_COLS_ORDERED` list.")

        # Select and reorder columns for the final table
        df_display = df[DISPLAY_COLS_ORDERED]
        df_numeric_display = df_numeric[DISPLAY_COLS_ORDERED] # Use this for styling apply
        df_display.columns = DISPLAY_HEADERS # Set headers on the display version

        # --- Generate HTML table string using Pandas Styler ---
        print("Applying styles and generating HTML table string using Styler...")

        # Apply the highlighting function row by row (axis=1)
        # The function returns CSS classes for each cell in the row
        styler = df_numeric_display.style.apply(highlight_value_bets, axis=1)

        # Apply the display headers to the styler object AFTER applying styles
        styler.set_table_attributes('class="dataframe"') # Apply base CSS class
        styler.set_caption(None) # Remove default caption if any
        # Use the formatted data (df_display) for the actual cell values
        styler.data = df_display

        # Render HTML
        html_table = styler.to_html(
            index=False,         # Don't include DataFrame index
            escape=True,         # Escape HTML characters
            na_rep='-',          # Representation for missing values (already applied)
            border=0             # No default table border
        )

        if not html_table or not isinstance(html_table, str):
             return format_error_html_for_table("Failed to generate HTML table using pandas Styler.")
        print("HTML table string generated successfully via Styler.")
        return html_table

    except KeyError as e:
        print(f"Error generating HTML table: Missing expected column {e}")
        traceback.print_exc()
        return format_error_html_for_table(f"Internal Error: Missing expected column '{e}' in data. Check merge logic or `DISPLAY_COLS_ORDERED` list.")
    except Exception as e:
        print(f"Error generating HTML table: {e}")
        traceback.print_exc()
        return format_error_html_for_table(f"Unexpected error during HTML table generation: {type(e).__name__}")


def generate_full_html_page(table_content_html: str, timestamp_str: str) -> str:
    """
    Constructs the entire HTML page as a string, embedding the table and timestamp.
    CSS updated for 10 columns and value highlighting.
    """
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Upcoming Tennis Odds Comparison</title>
    <style>
        :root {{
            --primary-color: #0056b3;
            --secondary-color: #007bff;
            --light-gray: #f8f9fa;
            --medium-gray: #dee2e6;
            --dark-gray: #343a40;
            --white: #ffffff;
            --hover-color: #e9ecef;
            --shadow-color: rgba(0,0,0,0.06);
            --value-bet-bg-color: #d4edda; /* Greenish background for value bets */
            --value-bet-text-color: #155724; /* Dark green text */
            --scooore-bg-color: #f0f8ff; /* Light blue for regular Scooore cells */
            --scooore-header-bg-color: #e7f3ff; /* Slightly darker blue for Scooore headers */
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol";
            line-height: 1.6; padding: 20px; max-width: 1200px; margin: 20px auto;
            background-color: var(--light-gray); color: var(--dark-gray);
        }}
        h1 {{
            color: var(--primary-color); border-bottom: 3px solid var(--primary-color);
            padding-bottom: 10px; margin-bottom: 25px; font-weight: 600; font-size: 1.8em;
        }}
        p {{ margin-bottom: 15px; font-size: 0.95em; }}
        .table-container {{
            overflow-x: auto; box-shadow: 0 4px 10px var(--shadow-color);
            border-radius: 6px; background-color: var(--white);
            border: 1px solid var(--medium-gray); min-height: 100px; margin-bottom: 20px;
        }}
        /* Base table style from Pandas Styler */
        table.dataframe {{
            width: 100%; border-collapse: collapse; margin: 0; font-size: 0.9em;
        }}
        /* Default cell style from Pandas Styler (overridden by specific classes below) */
        table.dataframe th, table.dataframe td {{
            border: none; border-bottom: 1px solid var(--medium-gray);
            padding: 10px 12px; text-align: left; vertical-align: middle;
            white-space: nowrap;
        }}
        table.dataframe tbody tr:last-child td {{ border-bottom: none; }}

        /* --- Column Widths (Applied via TH/TD selectors) --- */
        table.dataframe th:nth-child(1), table.dataframe td:nth-child(1) {{ width: 14%; white-space: normal;}} /* Tournament */
        table.dataframe th:nth-child(2), table.dataframe td:nth-child(2) {{ width: 5%; }}  /* Round */
        table.dataframe th:nth-child(3), table.dataframe td:nth-child(3) {{ width: 16%; white-space: normal; font-weight: 500;}} /* Player 1 */
        table.dataframe th:nth-child(4), table.dataframe td:nth-child(4) {{ width: 16%; white-space: normal; font-weight: 500;}} /* Player 2 */
        table.dataframe th:nth-child(5), table.dataframe td:nth-child(5) {{ width: 8%; text-align: right;}} /* P1 Prob (Sack) */
        table.dataframe th:nth-child(6), table.dataframe td:nth-child(6) {{ width: 8%; text-align: right;}} /* P2 Prob (Sack) */
        table.dataframe th:nth-child(7), table.dataframe td:nth-child(7) {{ width: 8%; text-align: right;}} /* P1 Odds (Sack) */
        table.dataframe th:nth-child(8), table.dataframe td:nth-child(8) {{ width: 8%; text-align: right;}} /* P2 Odds (Sack) */
        table.dataframe th:nth-child(9), table.dataframe td:nth-child(9) {{ width: 8%; text-align: right; background-color: var(--scooore-header-bg-color);}} /* P1 Odds (Scooore) Header */
        table.dataframe th:nth-child(10), table.dataframe td:nth-child(10) {{ width: 9%; text-align: right; background-color: var(--scooore-header-bg-color);}} /* P2 Odds (Scooore) Header */
        /* Default background for Scooore data cells */
        table.dataframe td:nth-child(9), table.dataframe td:nth-child(10) {{ background-color: var(--scooore-bg-color); }}

        /* Header Styles */
        table.dataframe thead th {{
            background-color: var(--secondary-color); color: var(--white); font-weight: 600;
            border-bottom: 2px solid var(--primary-color); position: sticky; top: 0; z-index: 1;
        }}
        /* Even row background */
        table.dataframe tbody tr:nth-child(even) td {{ background-color: var(--light-gray); }}
        /* Keep Scooore background on even rows */
        table.dataframe tbody tr:nth-child(even) td:nth-child(9),
        table.dataframe tbody tr:nth-child(even) td:nth-child(10) {{ background-color: var(--scooore-bg-color); opacity: 0.9; }} /* Slightly transparent on even rows */

        /* Hover Styles */
        table.dataframe tbody tr:hover td {{ background-color: var(--hover-color); }}
        /* Keep Scooore background on hover, slightly darker */
        table.dataframe tbody tr:hover td:nth-child(9),
        table.dataframe tbody tr:hover td:nth-child(10) {{ background-color: #d4eaff; }}

        /* --- Value Bet Highlighting Styles --- */
        /* Applied by the Styler to specific TD elements */
        table.dataframe td.value-bet-p1,
        table.dataframe td.value-bet-p2 {{
            background-color: var(--value-bet-bg-color) !important; /* Override other backgrounds */
            color: var(--value-bet-text-color);
            font-weight: bold;
        }}
        /* Ensure hover doesn't ruin value bet highlight */
         table.dataframe tbody tr:hover td.value-bet-p1,
         table.dataframe tbody tr:hover td.value-bet-p2 {{
             background-color: #b8dfc1 !important; /* Darker green on hover */
         }}


        .last-updated {{
            margin-top: 25px; padding-top: 15px; border-top: 1px solid var(--medium-gray);
            font-size: 0.9em; color: #6c757d; text-align: center;
        }}
        .{ERROR_MESSAGE_CLASS} {{
            color: #dc3545; font-weight: bold; text-align: center; padding: 20px;
            background-color: #f8d7da; border: 1px solid #f5c6cb; border-radius: 4px;
        }}
        /* Responsive adjustments */
        @media (max-width: 992px) {{
             table.dataframe th, table.dataframe td {{ font-size: 0.85em; padding: 8px 10px; }}
        }}
        @media (max-width: 768px) {{
            body {{ padding: 15px; }}
            h1 {{ font-size: 1.5em; }}
            table.dataframe th, table.dataframe td {{
                white-space: normal; padding: 8px 6px; font-size: 0.8em;
            }}
            table.dataframe th:nth-child(n), table.dataframe td:nth-child(n) {{ width: auto;}}
            table.dataframe th:nth-child(3), table.dataframe td:nth-child(3),
            table.dataframe th:nth-child(4), table.dataframe td:nth-child(4) {{ font-weight: normal;}}
        }}
    </style>
</head>
<body>

    <h1>Upcoming Tennis Match Odds Comparison</h1>
    <p>This page displays probabilities and calculated decimal odds from the Tennis Abstract Sackmann model, alongside betting odds scraped from Scooore.be (if available). Cells highlighted in <span style="background-color: var(--value-bet-bg-color); color: var(--value-bet-text-color); padding: 0 3px; border-radius: 3px;">green</span> indicate potential value bets where Scooore odds are at least {int((VALUE_BET_THRESHOLD-1)*100)}% higher than the model's implied odds.</p>
    <p>Matches involving qualifiers or appearing completed based on Sackmann data are filtered out. Name matching is performed automatically but may not be perfect.</p>

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
    print("Starting page generation process (Sackmann + Scooore Integration + Value Highlight)...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_abs = os.path.join(script_dir, DATA_DIR)
    output_file_abs = os.path.join(script_dir, OUTPUT_HTML_FILE)

    print(f"Script directory: {script_dir}")
    print(f"Looking for latest CSVs in: {data_dir_abs}")
    print(f"Outputting generated HTML to: {output_file_abs}")

    # 1. Find latest data files
    print("\nFinding latest data files...")
    latest_sackmann_csv = find_latest_csv(data_dir_abs, SACKMANN_CSV_PATTERN)
    latest_scooore_csv = find_latest_csv(data_dir_abs, SCOOORE_CSV_PATTERN)

    # 2. Load and Prepare Data
    sackmann_data = None
    scooore_data = None
    error_msg = ""

    if latest_sackmann_csv:
        sackmann_data = load_and_prepare_sackmann_data(latest_sackmann_csv)
        if sackmann_data is None or sackmann_data.empty:
             error_msg += f"Failed to load or prepare valid Sackmann data from {os.path.basename(latest_sackmann_csv)}. "
    else:
        error_msg += f"Could not find latest Sackmann data file ({SACKMANN_CSV_PATTERN}). "
        print(error_msg)

    if latest_scooore_csv:
        scooore_data = load_and_prepare_scooore_data(latest_scooore_csv)
        if scooore_data is None:
             print(f"Warning: Failed to load Scooore data from {os.path.basename(latest_scooore_csv)}. Proceeding without it.")
    else:
        print(f"Warning: No Scooore data file found ({SCOOORE_CSV_PATTERN}). Proceeding without it.")

    # 3. Merge Data
    merged_data = None
    if sackmann_data is not None and not sackmann_data.empty:
        merged_data = merge_data(sackmann_data, scooore_data)
    elif not error_msg:
        error_msg = "No upcoming Sackmann matches found after filtering. "

    # 4. Generate HTML Table
    table_html_content = ""
    if merged_data is not None and not merged_data.empty:
        print(f"\nGenerating HTML table content from merged data (Shape: {merged_data.shape})...")
        table_html_content = generate_html_table(merged_data) # Now uses Styler
    else:
        print(f"\nNo data available for table generation. Using error message: {error_msg}")
        final_error_msg = error_msg if error_msg else "Error: No valid match data found or processed."
        table_html_content = format_error_html_for_table(final_error_msg.strip())

    # 5. Generate Full HTML Page
    update_time = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
    timestamp_str = f"Last updated: {html.escape(update_time)}"
    print("\nGenerating full HTML page content...")
    full_html = generate_full_html_page(table_html_content, timestamp_str) # Includes value bet CSS
    print("Full HTML page content generated.")

    # 6. Write HTML to File
    try:
        print(f"Writing generated HTML content to: {output_file_abs}")
        with open(output_file_abs, 'w', encoding='utf-8') as f:
            f.write(full_html)
        print(f"Successfully wrote generated HTML to {os.path.basename(output_file_abs)}")
    except Exception as e:
        print(f"CRITICAL ERROR writing final HTML file: {e}")
        traceback.print_exc()

    print("\nPage generation process complete.")

