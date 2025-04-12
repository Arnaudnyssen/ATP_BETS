# generate_page.py (Integrates Betcenter odds, calculates spread, highlights value)

import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob
import pytz
import traceback
import re # For name preprocessing
from typing import Optional, List, Tuple, Any
import html

# --- Constants ---
ERROR_MESSAGE_CLASS = "error-message"
DATA_DIR = "data_archive"
SACKMANN_CSV_PATTERN = "sackmann_matchups_*.csv"
# --- Updated for Betcenter ---
BETCENTER_CSV_PATTERN = "betcenter_odds_*.csv" # Pattern for Betcenter files
# ---------------------------
OUTPUT_HTML_FILE = "index.html"
VALUE_BET_THRESHOLD = 1.10 # Highlight if Betcenter odds >= 110% of Sackmann odds

# --- Updated Column order and headers ---
DISPLAY_COLS_ORDERED = [
    'TournamentName', 'Round',
    'Player1Name', 'Player2Name',
    'Player1_Match_Prob', 'Player2_Match_Prob', # Sackmann Probabilities
    'Player1_Match_Odds', 'Player2_Match_Odds', # Sackmann Calculated Odds
    'bc_p1_odds', 'bc_p2_odds',                 # Betcenter Odds
    'p1_spread', 'p2_spread'                    # Odds Spread (BC - Sackmann)
]
DISPLAY_HEADERS = [
    "Tournament", "Round",
    "Player 1", "Player 2",
    "P1 Prob (Sack.)", "P2 Prob (Sack.)",
    "P1 Odds (Sack.)", "P2 Odds (Sack.)",
    "P1 Odds (BC)", "P2 Odds (BC)", # Updated header
    "P1 Spread", "P2 Spread"        # New spread headers
]

# --- Helper Functions ---
def preprocess_player_name(name: str) -> str:
    """Standardizes a single player name string using Title Case."""
    if not isinstance(name, str): return ""
    try:
        # Remove content in parentheses first
        name = re.sub(r'\s*\([^)]*\)', '', name).strip()
        # Remove leading/trailing asterisks if any
        name = re.sub(r'^\*|\*$', '', name).strip()
        # Apply title case for consistency
        name = name.title()
        # Specific replacements if needed (add more as identified)
        # name = name.replace("J.", "Jiri").replace("M.","Martin") # Example
        return name
    except Exception as e:
        print(f"Warning: Could not preprocess name '{name}': {e}")
        return name # Return original on error

def find_latest_csv(directory: str, pattern: str) -> Optional[str]:
    """Finds the most recently modified CSV file matching the pattern."""
    try:
        # Assume DATA_DIR is relative to the script's location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        search_dir = os.path.join(script_dir, directory)
        search_path = os.path.join(search_dir, pattern)
        print(f"Searching for pattern: {search_path}")
        list_of_files = glob.glob(search_path)
        if not list_of_files: print(f"  No files found matching pattern."); return None
        list_of_files = [f for f in list_of_files if os.path.isfile(f)]
        if not list_of_files: print(f"  No *files* found matching pattern (only directories?)."); return None
        latest_file = max(list_of_files, key=os.path.getmtime)
        print(f"Found latest CSV file: {os.path.basename(latest_file)}")
        return latest_file
    except Exception as e:
        print(f"Error finding latest CSV file in '{directory}' with pattern '{pattern}': {e}")
        traceback.print_exc()
        return None

def format_error_html_for_table(message: str) -> str:
    """Formats an error message as an HTML snippet for the table container."""
    print(f"Error generating table: {message}")
    # Ensure the error message is clearly visible within the table container style
    return f'<div class="{ERROR_MESSAGE_CLASS}" style="padding: 20px; text-align: center; color: #dc3545; background-color: #f8d7da; border: 1px solid #f5c6cb; border-radius: 4px;"><strong>Error:</strong> {html.escape(message)} Check logs for details.</div>'

# --- Data Loading Functions ---
def load_and_prepare_sackmann_data(csv_filepath: str) -> Optional[pd.DataFrame]:
    """Loads, preprocesses, and filters Sackmann data."""
    print(f"Loading Sackmann data from: {os.path.basename(csv_filepath)}")
    if not os.path.exists(csv_filepath) or os.path.getsize(csv_filepath) == 0:
        print("  Sackmann file is missing or empty.")
        return None
    try:
        df = pd.read_csv(csv_filepath)
        if df.empty: print("  Sackmann DataFrame is empty after loading."); return None
        print(f"  Read {len(df)} rows initially from Sackmann CSV.")

        # Ensure necessary columns exist
        required_sack_cols = ['Player1Name', 'Player2Name', 'Player1_Match_Prob', 'Player2_Match_Prob']
        if not all(col in df.columns for col in required_sack_cols):
             print(f"  Error: Sackmann DataFrame missing required columns. Found: {df.columns.tolist()}"); return None

        # Convert probabilities early
        df['Player1_Match_Prob'] = pd.to_numeric(df['Player1_Match_Prob'], errors='coerce')
        df['Player2_Match_Prob'] = pd.to_numeric(df['Player2_Match_Prob'], errors='coerce')

        # Filter out invalid probabilities (0, 100, NaN)
        original_count_step1 = len(df)
        df = df[
            (df['Player1_Match_Prob'].notna()) & (df['Player1_Match_Prob'] > 0.0) & (df['Player1_Match_Prob'] < 100.0) &
            (df['Player2_Match_Prob'].notna()) & (df['Player2_Match_Prob'] > 0.0) & (df['Player2_Match_Prob'] < 100.0)
        ].copy()
        print(f"  Filtered Sackmann (Prob = 0%, 100%, NaN): {original_count_step1 - len(df)} rows removed. {len(df)} remain.")

        # Preprocess names using the consistent function (Title Case)
        df['Player1Name'] = df['Player1Name'].astype(str).apply(preprocess_player_name)
        df['Player2Name'] = df['Player2Name'].astype(str).apply(preprocess_player_name)

        # Filter out qualifiers
        original_count_step2 = len(df)
        mask_p1_qualifier = df['Player1Name'].str.contains('Qualifier', case=False, na=False)
        mask_p2_qualifier = df['Player2Name'].str.contains('Qualifier', case=False, na=False)
        df = df[~(mask_p1_qualifier | mask_p2_qualifier)].copy()
        print(f"  Filtered Sackmann (Qualifiers): {original_count_step2 - len(df)} rows removed. {len(df)} remain.")

        if df.empty: print("  Sackmann DataFrame is empty after filtering."); return None

        # Select and ensure correct types for needed columns
        sackmann_cols = ['TournamentName', 'Round', 'Player1Name', 'Player2Name',
                         'Player1_Match_Prob', 'Player2_Match_Prob',
                         'Player1_Match_Odds', 'Player2_Match_Odds']
        df_out = df[[col for col in sackmann_cols if col in df.columns]].copy()
        df_out['Player1_Match_Odds'] = pd.to_numeric(df_out['Player1_Match_Odds'], errors='coerce')
        df_out['Player2_Match_Odds'] = pd.to_numeric(df_out['Player2_Match_Odds'], errors='coerce')

        print(f"  Prepared Sackmann data. Shape: {df_out.shape}")
        return df_out
    except Exception as e:
        print(f"  Error loading/preparing Sackmann data: {e}"); traceback.print_exc(); return None

# --- Updated for Betcenter ---
def load_and_prepare_betcenter_data(csv_filepath: str) -> Optional[pd.DataFrame]:
    """Loads and preprocesses Betcenter odds data."""
    print(f"Loading Betcenter data from: {os.path.basename(csv_filepath)}")
    if not os.path.exists(csv_filepath) or os.path.getsize(csv_filepath) == 0:
        print("  Betcenter file is missing or empty.")
        return None
    try:
        df = pd.read_csv(csv_filepath)
        if df.empty: print("  Betcenter DataFrame is empty after loading."); return None
        print(f"  Read {len(df)} rows initially from Betcenter CSV.")

        # Check for required columns from the scraper output
        required_bc_cols = ['p1_name', 'p2_name', 'p1_odds', 'p2_odds']
        if not all(col in df.columns for col in required_bc_cols):
             print(f"  Error: Betcenter DataFrame missing required columns. Found: {df.columns.tolist()}"); return None

        # Preprocess names using the consistent function (Title Case)
        df['Player1Name'] = df['p1_name'].astype(str).apply(preprocess_player_name)
        df['Player2Name'] = df['p2_name'].astype(str).apply(preprocess_player_name)

        # Select and rename columns for merging, ensure numeric types
        betcenter_cols = ['Player1Name', 'Player2Name', 'p1_odds', 'p2_odds']
        df_out = df[[col for col in betcenter_cols if col in df.columns]].copy()
        df_out.rename(columns={'p1_odds': 'bc_p1_odds', 'p2_odds': 'bc_p2_odds'}, inplace=True)

        df_out['bc_p1_odds'] = pd.to_numeric(df_out['bc_p1_odds'], errors='coerce')
        df_out['bc_p2_odds'] = pd.to_numeric(df_out['bc_p2_odds'], errors='coerce')

        # Drop rows where odds might be missing after conversion
        df_out.dropna(subset=['bc_p1_odds', 'bc_p2_odds'], inplace=True)

        print(f"  Prepared Betcenter data. Shape: {df_out.shape}")
        return df_out
    except Exception as e:
        print(f"  Error loading/preparing Betcenter data: {e}"); traceback.print_exc(); return None
# --- End Update ---

# --- Merge Function (Updated for Betcenter) ---
def merge_data(sackmann_df: pd.DataFrame, betcenter_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Merges Sackmann and Betcenter dataframes based on player names."""
    if betcenter_df is None or betcenter_df.empty:
        print("Betcenter data is missing or empty. Returning only Sackmann data with placeholder columns.")
        # Add placeholder columns if Betcenter data is missing
        for col in ['bc_p1_odds', 'bc_p2_odds', 'p1_spread', 'p2_spread']:
             if col not in sackmann_df.columns: sackmann_df[col] = np.nan
        return sackmann_df

    print("Attempting to merge Sackmann and Betcenter data...")
    try:
        # Ensure consistent Title Case names before merging
        sackmann_df['Player1Name'] = sackmann_df['Player1Name'].astype(str).apply(preprocess_player_name)
        sackmann_df['Player2Name'] = sackmann_df['Player2Name'].astype(str).apply(preprocess_player_name)
        betcenter_df['Player1Name'] = betcenter_df['Player1Name'].astype(str).apply(preprocess_player_name)
        betcenter_df['Player2Name'] = betcenter_df['Player2Name'].astype(str).apply(preprocess_player_name)

        # Outer merge to keep all matches from both sources
        merged_df = pd.merge(
            sackmann_df,
            betcenter_df,
            on=['Player1Name', 'Player2Name'],
            how='outer',
            suffixes=('_sack', '_bc') # Add suffixes in case of other overlapping columns
        )
        print(f"  Merged (P1-P1, P2-P2). Shape: {merged_df.shape}")

        # Attempt swapped merge for matches missed in the first pass
        # Identify rows from Betcenter that didn't match Sackmann
        unmatched_betcenter = merged_df[merged_df['Player1_Match_Prob'].isna() & merged_df['bc_p1_odds'].notna()].copy()
        # Identify rows from Sackmann that didn't match Betcenter
        unmatched_sackmann = merged_df[merged_df['bc_p1_odds'].isna() & merged_df['Player1_Match_Prob'].notna()].copy()

        if not unmatched_betcenter.empty and not unmatched_sackmann.empty:
            print(f"  Found {len(unmatched_betcenter)} unmatched Betcenter rows and {len(unmatched_sackmann)} unmatched Sackmann rows. Attempting swapped merge...")
            # Prepare swapped Betcenter data
            swapped_betcenter = unmatched_betcenter[['Player2Name', 'Player1Name', 'bc_p2_odds', 'bc_p1_odds']].copy()
            swapped_betcenter.columns = ['Player1Name', 'Player2Name', 'bc_p1_odds', 'bc_p2_odds'] # Rename columns to match for merge

            # Merge unmatched Sackmann with swapped Betcenter
            # Use inner merge: only keep rows that match after swapping
            swapped_merge_result = pd.merge(
                unmatched_sackmann.drop(columns=['bc_p1_odds', 'bc_p2_odds']), # Drop empty BC columns from unmatched Sackmann
                swapped_betcenter,
                on=['Player1Name', 'Player2Name'],
                how='inner' # Only keep matches found via swap
            )
            print(f"  Found {len(swapped_merge_result)} matches via swapped merge.")

            # Update the main merged_df with the results from the swapped merge
            if not swapped_merge_result.empty:
                # Use set_index for efficient updating based on player names
                merged_df.set_index(['Player1Name', 'Player2Name'], inplace=True)
                swapped_merge_result.set_index(['Player1Name', 'Player2Name'], inplace=True)
                # Update only the Betcenter odds columns for the swapped matches
                merged_df.update(swapped_merge_result[['bc_p1_odds', 'bc_p2_odds']])
                merged_df.reset_index(inplace=True) # Reset index back to default
                print("  Updated main dataframe with swapped matches.")

        # --- Calculate Spread ---
        print("  Calculating odds spread (Betcenter - Sackmann)...")
        # Ensure odds columns are numeric before calculation
        merged_df['Player1_Match_Odds'] = pd.to_numeric(merged_df['Player1_Match_Odds'], errors='coerce')
        merged_df['Player2_Match_Odds'] = pd.to_numeric(merged_df['Player2_Match_Odds'], errors='coerce')
        merged_df['bc_p1_odds'] = pd.to_numeric(merged_df['bc_p1_odds'], errors='coerce')
        merged_df['bc_p2_odds'] = pd.to_numeric(merged_df['bc_p2_odds'], errors='coerce')

        # Calculate spread, result will be NaN if either operand is NaN
        merged_df['p1_spread'] = merged_df['bc_p1_odds'] - merged_df['Player1_Match_Odds']
        merged_df['p2_spread'] = merged_df['bc_p2_odds'] - merged_df['Player2_Match_Odds']
        print("  Spread calculated.")

        # --- Final Cleanup ---
        # Remove rows where Sackmann data is completely missing (these came only from Betcenter)
        # We only want to display matches present in the Sackmann model predictions
        initial_rows = len(merged_df)
        merged_df.dropna(subset=['Player1_Match_Prob', 'Player1_Match_Odds'], how='all', inplace=True)
        print(f"  Removed {initial_rows - len(merged_df)} rows where Sackmann data was missing.")

        print(f"Final merged data shape: {merged_df.shape}")
        return merged_df

    except Exception as e:
        print(f"Error during data merging or spread calculation: {e}")
        traceback.print_exc()
        # Return Sackmann data with placeholders on error
        for col in ['bc_p1_odds', 'bc_p2_odds', 'p1_spread', 'p2_spread']:
             if col not in sackmann_df.columns: sackmann_df[col] = np.nan
        return sackmann_df

# --- HTML Generation ---

def apply_table_styles(row: pd.Series) -> List[str]:
    """
    Applies CSS classes for value bets and spread highlighting.
    Takes a row of the *numeric* DataFrame as input.
    Returns a list of CSS class strings, one for each column in DISPLAY_COLS_ORDERED.
    """
    # Initialize styles for all display columns
    styles = [''] * len(DISPLAY_COLS_ORDERED)

    # --- Value Bet Highlighting (Betcenter vs Sackmann) ---
    try:
        # Get numeric odds, coerce errors to NaN
        sack_odds_p1 = pd.to_numeric(row.get('Player1_Match_Odds'), errors='coerce')
        bc_odds_p1 = pd.to_numeric(row.get('bc_p1_odds'), errors='coerce')
        sack_odds_p2 = pd.to_numeric(row.get('Player2_Match_Odds'), errors='coerce')
        bc_odds_p2 = pd.to_numeric(row.get('bc_p2_odds'), errors='coerce')

        # Check P1 value bet
        if not pd.isna(sack_odds_p1) and not pd.isna(bc_odds_p1) and bc_odds_p1 >= sack_odds_p1 * VALUE_BET_THRESHOLD:
            try:
                p1_odds_idx = DISPLAY_COLS_ORDERED.index('bc_p1_odds')
                styles[p1_odds_idx] = 'value-bet-p1'
            except ValueError: pass # Column not in display list

        # Check P2 value bet
        if not pd.isna(sack_odds_p2) and not pd.isna(bc_odds_p2) and bc_odds_p2 >= sack_odds_p2 * VALUE_BET_THRESHOLD:
            try:
                p2_odds_idx = DISPLAY_COLS_ORDERED.index('bc_p2_odds')
                styles[p2_odds_idx] = 'value-bet-p2'
            except ValueError: pass # Column not in display list

    except Exception as e_val:
        print(f"Warning: Error during value bet styling: {e_val}")
        # Continue without value bet styling if error occurs

    # --- Spread Highlighting ---
    try:
        p1_spread = pd.to_numeric(row.get('p1_spread'), errors='coerce')
        p2_spread = pd.to_numeric(row.get('p2_spread'), errors='coerce')

        # Style P1 Spread
        if not pd.isna(p1_spread):
            try:
                p1_spread_idx = DISPLAY_COLS_ORDERED.index('p1_spread')
                if p1_spread > 0:
                    styles[p1_spread_idx] = 'spread-positive'
                elif p1_spread < 0:
                    styles[p1_spread_idx] = 'spread-negative'
                # else: no style for zero spread
            except ValueError: pass # Column not in display list

        # Style P2 Spread
        if not pd.isna(p2_spread):
            try:
                p2_spread_idx = DISPLAY_COLS_ORDERED.index('p2_spread')
                if p2_spread > 0:
                    styles[p2_spread_idx] = 'spread-positive'
                elif p2_spread < 0:
                    styles[p2_spread_idx] = 'spread-negative'
            except ValueError: pass # Column not in display list

    except Exception as e_spread:
        print(f"Warning: Error during spread styling: {e_spread}")
        # Continue without spread styling if error occurs

    return styles


def generate_html_table(df: pd.DataFrame) -> str:
    """
    Formats the merged DataFrame, sorts, selects/reorders columns, applies value
    and spread highlighting, and generates an HTML table string using Pandas Styler.
    Returns error HTML string on failure.
    """
    if df is None or df.empty:
         return format_error_html_for_table("No combined match data available to display.")

    try:
        print("Formatting final merged data for display...")
        # Keep a numeric copy for styling logic BEFORE formatting display strings
        df_numeric = df.copy()

        # --- Apply Display Formatting ---
        # Format probabilities
        df['Player1_Match_Prob'] = pd.to_numeric(df['Player1_Match_Prob'], errors='coerce').map('{:.1f}%'.format, na_action='ignore')
        df['Player2_Match_Prob'] = pd.to_numeric(df['Player2_Match_Prob'], errors='coerce').map('{:.1f}%'.format, na_action='ignore')
        # Format Sackmann odds
        df['Player1_Match_Odds'] = pd.to_numeric(df['Player1_Match_Odds'], errors='coerce').map('{:.2f}'.format, na_action='ignore')
        df['Player2_Match_Odds'] = pd.to_numeric(df['Player2_Match_Odds'], errors='coerce').map('{:.2f}'.format, na_action='ignore')
        # Format Betcenter odds
        df['bc_p1_odds'] = pd.to_numeric(df['bc_p1_odds'], errors='coerce').map('{:.2f}'.format, na_action='ignore')
        df['bc_p2_odds'] = pd.to_numeric(df['bc_p2_odds'], errors='coerce').map('{:.2f}'.format, na_action='ignore')
        # Format Spread (show sign explicitly)
        df['p1_spread'] = pd.to_numeric(df['p1_spread'], errors='coerce').map('{:+.2f}'.format, na_action='ignore')
        df['p2_spread'] = pd.to_numeric(df['p2_spread'], errors='coerce').map('{:+.2f}'.format, na_action='ignore')

        # Fill any remaining NaNs with '-' AFTER formatting
        df.fillna('-', inplace=True)
        print("Data formatting complete.")

        # --- Sorting ---
        try:
            # Sort by Tournament and Round (same logic as before)
            round_map = {'R128': 128, 'R64': 64, 'R32': 32, 'R16': 16, 'QF': 8, 'SF': 4, 'F': 2, 'W': 1}
            df['RoundSort'] = df['Round'].map(round_map).fillna(999) # Create numeric sort key for Round
            df.sort_values(by=['TournamentName', 'RoundSort'], inplace=True, na_position='last')
            df.drop(columns=['RoundSort'], inplace=True) # Remove temporary sort column
            # Apply the same sort order to the numeric dataframe used for styling
            df_numeric = df_numeric.loc[df.index]
            print("Sorted matchups by Tournament and Round.")
        except Exception as e:
             print(f"Warning: Error during sorting: {e}")

        # --- Column Selection and Renaming ---
        missing_display_cols = [col for col in DISPLAY_COLS_ORDERED if col not in df.columns]
        if missing_display_cols:
            return format_error_html_for_table(f"Data is missing columns needed for display: {', '.join(missing_display_cols)}. Check merge logic and `DISPLAY_COLS_ORDERED` list.")

        df_display = df[DISPLAY_COLS_ORDERED]
        df_numeric_display = df_numeric[DISPLAY_COLS_ORDERED] # Use this for styling apply
        df_display.columns = DISPLAY_HEADERS # Set display headers

        # --- Generate HTML table string using Pandas Styler ---
        print("Applying styles and generating HTML table string using Styler...")

        # Apply the styling function row by row (axis=1) to the numeric data
        styler = df_numeric_display.style.apply(apply_table_styles, axis=1)

        # Apply table attributes and use the formatted data for display
        styler.set_table_attributes('class="dataframe"')
        styler.set_caption(None)
        styler.data = df_display # Set the display data AFTER applying styles based on numeric data

        # Render HTML
        html_table = styler.to_html(
            index=False, escape=True, na_rep='-', border=0
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
    Constructs the entire HTML page, embedding the table and timestamp.
    CSS updated for 12 columns, Betcenter odds, and spread highlighting.
    """
    # Updated CSS for 12 columns and new styles
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Upcoming Tennis Odds Comparison (Sackmann vs Betcenter)</title>
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
            --betcenter-bg-color: #fff0f5; /* Lavender blush for Betcenter cells */
            --betcenter-header-bg-color: #ffe4e1; /* Misty rose for Betcenter headers */
            --spread-positive-bg-color: #e6ffed; /* Light green for positive spread */
            --spread-positive-text-color: #006400; /* Dark green */
            --spread-negative-bg-color: #ffeeee; /* Light red for negative spread */
            --spread-negative-text-color: #a52a2a; /* Brown */
            --spread-header-bg-color: #f5f5f5; /* Very light gray for spread headers */
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol";
            line-height: 1.6; padding: 20px; max-width: 1400px; /* Increased max-width */
            margin: 20px auto; background-color: var(--light-gray); color: var(--dark-gray);
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
        table.dataframe {{
            width: 100%; border-collapse: collapse; margin: 0; font-size: 0.85em; /* Slightly smaller base font */
        }}
        table.dataframe th, table.dataframe td {{
            border: none; border-bottom: 1px solid var(--medium-gray);
            padding: 8px 10px; /* Adjusted padding */
            text-align: left; vertical-align: middle; white-space: nowrap;
        }}
        table.dataframe tbody tr:last-child td {{ border-bottom: none; }}

        /* --- Column Widths (12 columns) --- */
        table.dataframe th:nth-child(1), table.dataframe td:nth-child(1) {{ width: 12%; white-space: normal;}} /* Tournament */
        table.dataframe th:nth-child(2), table.dataframe td:nth-child(2) {{ width: 4%; }}  /* Round */
        table.dataframe th:nth-child(3), table.dataframe td:nth-child(3) {{ width: 13%; white-space: normal; font-weight: 500;}} /* Player 1 */
        table.dataframe th:nth-child(4), table.dataframe td:nth-child(4) {{ width: 13%; white-space: normal; font-weight: 500;}} /* Player 2 */
        table.dataframe th:nth-child(5), table.dataframe td:nth-child(5) {{ width: 7%; text-align: right;}} /* P1 Prob (Sack) */
        table.dataframe th:nth-child(6), table.dataframe td:nth-child(6) {{ width: 7%; text-align: right;}} /* P2 Prob (Sack) */
        table.dataframe th:nth-child(7), table.dataframe td:nth-child(7) {{ width: 7%; text-align: right;}} /* P1 Odds (Sack) */
        table.dataframe th:nth-child(8), table.dataframe td:nth-child(8) {{ width: 7%; text-align: right;}} /* P2 Odds (Sack) */
        /* Betcenter Columns */
        table.dataframe th:nth-child(9), table.dataframe td:nth-child(9) {{ width: 7%; text-align: right; background-color: var(--betcenter-header-bg-color);}} /* P1 Odds (BC) Header */
        table.dataframe th:nth-child(10), table.dataframe td:nth-child(10) {{ width: 7%; text-align: right; background-color: var(--betcenter-header-bg-color);}} /* P2 Odds (BC) Header */
        /* Spread Columns */
        table.dataframe th:nth-child(11), table.dataframe td:nth-child(11) {{ width: 7%; text-align: right; background-color: var(--spread-header-bg-color);}} /* P1 Spread Header */
        table.dataframe th:nth-child(12), table.dataframe td:nth-child(12) {{ width: 6%; text-align: right; background-color: var(--spread-header-bg-color);}} /* P2 Spread Header */

        /* Default background for Betcenter data cells */
        table.dataframe td:nth-child(9), table.dataframe td:nth-child(10) {{ background-color: var(--betcenter-bg-color); }}
        /* Default background for Spread data cells */
        table.dataframe td:nth-child(11), table.dataframe td:nth-child(12) {{ background-color: var(--spread-header-bg-color); }}


        /* Header Styles */
        table.dataframe thead th {{
            background-color: var(--secondary-color); color: var(--white); font-weight: 600;
            border-bottom: 2px solid var(--primary-color); position: sticky; top: 0; z-index: 1;
        }}
        /* Even row background */
        table.dataframe tbody tr:nth-child(even) td {{ background-color: var(--light-gray); }}
        /* Keep Betcenter/Spread background on even rows */
        table.dataframe tbody tr:nth-child(even) td:nth-child(9),
        table.dataframe tbody tr:nth-child(even) td:nth-child(10) {{ background-color: var(--betcenter-bg-color); opacity: 0.9; }}
        table.dataframe tbody tr:nth-child(even) td:nth-child(11),
        table.dataframe tbody tr:nth-child(even) td:nth-child(12) {{ background-color: var(--spread-header-bg-color); opacity: 0.9; }}

        /* Hover Styles */
        table.dataframe tbody tr:hover td {{ background-color: var(--hover-color); }}
        /* Keep Betcenter/Spread background on hover, slightly darker */
        table.dataframe tbody tr:hover td:nth-child(9),
        table.dataframe tbody tr:hover td:nth-child(10) {{ background-color: #fddde6; }} /* Darker pinkish */
         table.dataframe tbody tr:hover td:nth-child(11),
         table.dataframe tbody tr:hover td:nth-child(12) {{ background-color: #e9e9e9; }} /* Darker gray */

        /* --- Value Bet Highlighting Styles --- */
        table.dataframe td.value-bet-p1,
        table.dataframe td.value-bet-p2 {{
            background-color: var(--value-bet-bg-color) !important; /* Override other backgrounds */
            color: var(--value-bet-text-color); font-weight: bold;
        }}
        table.dataframe tbody tr:hover td.value-bet-p1,
        table.dataframe tbody tr:hover td.value-bet-p2 {{
             background-color: #b8dfc1 !important; /* Darker green on hover */
         }}

        /* --- Spread Highlighting Styles --- */
        table.dataframe td.spread-positive {{
            background-color: var(--spread-positive-bg-color) !important;
            color: var(--spread-positive-text-color); font-weight: 500;
        }}
        table.dataframe td.spread-negative {{
            background-color: var(--spread-negative-bg-color) !important;
            color: var(--spread-negative-text-color); font-weight: 500;
        }}
         table.dataframe tbody tr:hover td.spread-positive {{ background-color: #c8e6c9 !important; }} /* Slightly darker green */
         table.dataframe tbody tr:hover td.spread-negative {{ background-color: #ffcdd2 !important; }} /* Slightly darker red */


        .last-updated {{
            margin-top: 25px; padding-top: 15px; border-top: 1px solid var(--medium-gray);
            font-size: 0.9em; color: #6c757d; text-align: center;
        }}
        .{ERROR_MESSAGE_CLASS} {{ /* Style for error message div */
             /* Styles moved inside format_error_html_for_table for direct application */
        }}
        /* Responsive adjustments */
        @media (max-width: 1200px) {{
             table.dataframe th, table.dataframe td {{ font-size: 0.8em; padding: 7px 8px; }}
        }}
        @media (max-width: 992px) {{
            body {{ padding: 15px; max-width: 100%; }}
            h1 {{ font-size: 1.5em; }}
            table.dataframe th, table.dataframe td {{ font-size: 0.75em; padding: 6px 5px; white-space: normal; }}
            table.dataframe th:nth-child(n), table.dataframe td:nth-child(n) {{ width: auto;}} /* Let columns resize */
            table.dataframe th:nth-child(3), table.dataframe td:nth-child(3),
            table.dataframe th:nth-child(4), table.dataframe td:nth-child(4) {{ font-weight: normal;}}
        }}
        @media (max-width: 768px) {{
             table.dataframe th, table.dataframe td {{ font-size: 0.7em; padding: 5px 4px; }}
        }}
    </style>
</head>
<body>

    <h1>Upcoming Tennis Match Odds Comparison (Sackmann vs Betcenter)</h1>
    <p>Comparison of probabilities and calculated odds from the Tennis Abstract Sackmann model against betting odds scraped from Betcenter.be.
       The 'Spread' columns show the difference between Betcenter odds and Sackmann's calculated odds (Positive means Betcenter odds are higher).
       Cells highlighted in <span style="background-color: var(--value-bet-bg-color); color: var(--value-bet-text-color); padding: 0 3px; border-radius: 3px;">green</span> indicate potential value bets where Betcenter odds are at least {int((VALUE_BET_THRESHOLD-1)*100)}% higher than the model's implied odds.</p>
    <p>Matches involving qualifiers or appearing completed based on Sackmann data are filtered out. Name matching uses Title Case and may not be perfect.</p>

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
    print("Starting page generation process (Sackmann + Betcenter Integration + Spread)...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_abs = os.path.join(script_dir, DATA_DIR)
    output_file_abs = os.path.join(script_dir, OUTPUT_HTML_FILE)

    print(f"Script directory: {script_dir}")
    print(f"Looking for latest CSVs in: {data_dir_abs}")
    print(f"Outputting generated HTML to: {output_file_abs}")

    # 1. Find latest data files
    print("\nFinding latest data files...")
    latest_sackmann_csv = find_latest_csv(data_dir_abs, SACKMANN_CSV_PATTERN)
    latest_betcenter_csv = find_latest_csv(data_dir_abs, BETCENTER_CSV_PATTERN) # Updated pattern

    # 2. Load and Prepare Data
    sackmann_data = None
    betcenter_data = None # Renamed variable
    error_msg = ""

    if latest_sackmann_csv:
        sackmann_data = load_and_prepare_sackmann_data(latest_sackmann_csv)
        if sackmann_data is None or sackmann_data.empty:
             error_msg += f"Failed to load or prepare valid Sackmann data from {os.path.basename(latest_sackmann_csv)}. "
    else:
        error_msg += f"Could not find latest Sackmann data file ({SACKMANN_CSV_PATTERN}). "
        print(error_msg) # Print error immediately if Sackmann fails

    if latest_betcenter_csv:
        betcenter_data = load_and_prepare_betcenter_data(latest_betcenter_csv) # Updated function call
        if betcenter_data is None or betcenter_data.empty:
             print(f"Warning: Failed to load or prepare valid Betcenter data from {os.path.basename(latest_betcenter_csv)}. Proceeding without it.")
    else:
        print(f"Warning: No Betcenter data file found ({BETCENTER_CSV_PATTERN}). Proceeding without it.")

    # 3. Merge Data (Requires Sackmann data at minimum)
    merged_data = None
    if sackmann_data is not None and not sackmann_data.empty:
        merged_data = merge_data(sackmann_data, betcenter_data) # Pass betcenter_data
    elif not error_msg: # If Sackmann loading didn't already report an error
        error_msg = "No upcoming Sackmann matches found after filtering. Cannot generate comparison."
        print(error_msg)

    # 4. Generate HTML Table
    table_html_content = ""
    if merged_data is not None and not merged_data.empty:
        print(f"\nGenerating HTML table content from merged data (Shape: {merged_data.shape})...")
        table_html_content = generate_html_table(merged_data) # Uses Styler with updated styling logic
    else:
        # If merging failed or Sackmann data was missing/empty
        print(f"\nNo data available for table generation. Using error message: {error_msg}")
        final_error_msg = error_msg if error_msg else "Error: No valid match data found or processed."
        table_html_content = format_error_html_for_table(final_error_msg.strip())

    # 5. Generate Full HTML Page
    update_time = datetime.now(pytz.timezone('Europe/Brussels')).strftime('%Y-%m-%d %H:%M:%S %Z') # Use local timezone
    timestamp_str = f"Last updated: {html.escape(update_time)}"
    print("\nGenerating full HTML page content...")
    full_html = generate_full_html_page(table_html_content, timestamp_str)
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
