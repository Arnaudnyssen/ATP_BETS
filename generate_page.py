# generate_page.py (Integrates Betcenter odds, calculates spread - Fix KeyError, Improve Merge)

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
BETCENTER_CSV_PATTERN = "betcenter_odds_*.csv"
OUTPUT_HTML_FILE = "index.html"
VALUE_BET_THRESHOLD = 1.10 # Highlight if Betcenter odds >= 110% of Sackmann odds

# --- Column Definitions ---
MERGE_KEY_COLS = ['TournamentKey', 'Player1NameKey', 'Player2NameKey']
# Define columns to keep from each source (using original Betcenter odds names here)
SACKMANN_COLS_KEEP = ['TournamentName', 'Round', 'Player1Name', 'Player2Name',
                      'Player1_Match_Prob', 'Player2_Match_Prob',
                      'Player1_Match_Odds', 'Player2_Match_Odds'] + MERGE_KEY_COLS
# Keep original odds names from Betcenter CSV for selection, plus keys
BETCENTER_COLS_SELECT = ['TournamentName', 'p1_odds', 'p2_odds'] + MERGE_KEY_COLS

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
# (create_merge_key, preprocess_player_name, find_latest_csv, format_error_html_for_table remain the same)
def create_merge_key(text: str) -> str:
    """Creates a simplified, lowercase, space-removed key for merging."""
    if not isinstance(text, str): return ""
    try:
        key = text.lower()
        key = key.replace("tennis - ", "").replace(", qualifying", "").replace(", spain", "").replace(", germany", "")
        key = re.sub(r'[^\w]', '', key)
        return key
    except Exception: return ""

def preprocess_player_name(name: str) -> Tuple[str, str]:
    """Standardizes player names and creates a merge key."""
    display_name = ""; merge_key_name = ""
    if not isinstance(name, str): return display_name, merge_key_name
    try:
        if ',' in name:
            parts = [part.strip() for part in name.split(',')];
            if len(parts) == 2: name = f"{parts[1]} {parts[0]}"
        display_name = re.sub(r'\s*\([^)]*\)', '', name).strip()
        display_name = re.sub(r'^\*|\*$', '', display_name).strip()
        display_name = display_name.title()
        display_name = re.sub(r'\s+', ' ', display_name).strip()
        merge_key_name = create_merge_key(display_name)
        return display_name, merge_key_name
    except Exception as e: print(f"Warning: Could not preprocess player name '{name}': {e}"); return name.title(), create_merge_key(name)

def find_latest_csv(directory: str, pattern: str) -> Optional[str]:
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

def format_error_html_for_table(message: str) -> str:
    print(f"Error generating table: {message}")
    return f'<div class="{ERROR_MESSAGE_CLASS}" style="padding: 20px; text-align: center; color: #dc3545; background-color: #f8d7da; border: 1px solid #f5c6cb; border-radius: 4px;"><strong>Error:</strong> {html.escape(message)} Check logs for details.</div>'

# --- Data Loading Functions ---
def load_and_prepare_sackmann_data(csv_filepath: str) -> Optional[pd.DataFrame]:
    """Loads, preprocesses, filters, and standardizes Sackmann data."""
    print(f"Loading Sackmann data from: {os.path.basename(csv_filepath)}")
    # (No changes needed here from previous version)
    if not os.path.exists(csv_filepath) or os.path.getsize(csv_filepath) == 0: print("  Sackmann file is missing or empty."); return None
    try:
        df = pd.read_csv(csv_filepath)
        if df.empty: print("  Sackmann DataFrame is empty after loading."); return None
        print(f"  Read {len(df)} rows initially from Sackmann CSV.")
        required_cols = ['TournamentName', 'Player1Name', 'Player2Name', 'Player1_Match_Prob', 'Player2_Match_Prob']
        if not all(col in df.columns for col in required_cols): print(f"  Error: Sackmann DataFrame missing required columns. Found: {df.columns.tolist()}"); return None
        df['Player1_Match_Prob'] = pd.to_numeric(df['Player1_Match_Prob'], errors='coerce')
        df['Player2_Match_Prob'] = pd.to_numeric(df['Player2_Match_Prob'], errors='coerce')
        original_count_step1 = len(df)
        df = df[ (df['Player1_Match_Prob'].notna()) & (df['Player1_Match_Prob'] > 0.0) & (df['Player1_Match_Prob'] < 100.0) & (df['Player2_Match_Prob'].notna()) & (df['Player2_Match_Prob'] > 0.0) & (df['Player2_Match_Prob'] < 100.0) ].copy()
        print(f"  Filtered Sackmann (Prob = 0%, 100%, NaN): {original_count_step1 - len(df)} rows removed. {len(df)} remain.")
        df['TournamentKey'] = df['TournamentName'].astype(str).apply(create_merge_key)
        df['TournamentName'] = df['TournamentName'].astype(str).apply(lambda x: x.title()) # Keep original title cased for display if needed later
        df[['Player1Name', 'Player1NameKey']] = df['Player1Name'].astype(str).apply(lambda x: pd.Series(preprocess_player_name(x)))
        df[['Player2Name', 'Player2NameKey']] = df['Player2Name'].astype(str).apply(lambda x: pd.Series(preprocess_player_name(x)))
        original_count_step2 = len(df)
        mask_p1_qualifier = df['Player1Name'].str.contains('Qualifier', case=False, na=False)
        mask_p2_qualifier = df['Player2Name'].str.contains('Qualifier', case=False, na=False)
        df = df[~(mask_p1_qualifier | mask_p2_qualifier)].copy()
        print(f"  Filtered Sackmann (Qualifiers): {original_count_step2 - len(df)} rows removed. {len(df)} remain.")
        if df.empty: print("  Sackmann DataFrame is empty after filtering."); return None
        df_out = df[SACKMANN_COLS_KEEP].copy()
        df_out['Player1_Match_Odds'] = pd.to_numeric(df_out['Player1_Match_Odds'], errors='coerce')
        df_out['Player2_Match_Odds'] = pd.to_numeric(df_out['Player2_Match_Odds'], errors='coerce')
        print(f"  Prepared Sackmann data. Shape: {df_out.shape}")
        return df_out
    except Exception as e: print(f"  Error loading/preparing Sackmann data: {e}"); traceback.print_exc(); return None


def load_and_prepare_betcenter_data(csv_filepath: str) -> Optional[pd.DataFrame]:
    """Loads, preprocesses, and standardizes Betcenter odds data."""
    print(f"Loading Betcenter data from: {os.path.basename(csv_filepath)}")
    if not os.path.exists(csv_filepath) or os.path.getsize(csv_filepath) == 0: print("  Betcenter file is missing or empty."); return None
    try:
        df = pd.read_csv(csv_filepath)
        if df.empty: print("  Betcenter DataFrame is empty after loading."); return None
        print(f"  Read {len(df)} rows initially from Betcenter CSV.")
        required_bc_cols = ['tournament', 'p1_name', 'p2_name', 'p1_odds', 'p2_odds']
        if not all(col in df.columns for col in required_bc_cols): print(f"  Error: Betcenter DataFrame missing required columns ({required_bc_cols}). Found: {df.columns.tolist()}"); return None

        # Create Display Names and Merge Keys
        df['TournamentKey'] = df['tournament'].astype(str).apply(create_merge_key)
        df['TournamentName'] = df['tournament'].astype(str).apply(lambda x: x.replace("Tennis - ", "").strip().title())
        df[['Player1Name', 'Player1NameKey']] = df['p1_name'].astype(str).apply(lambda x: pd.Series(preprocess_player_name(x)))
        df[['Player2Name', 'Player2NameKey']] = df['p2_name'].astype(str).apply(lambda x: pd.Series(preprocess_player_name(x)))

        # --- *** FIX for KeyError *** ---
        # Select the columns we KNOW exist in df at this point
        cols_to_select = ['TournamentName'] + MERGE_KEY_COLS + ['p1_odds', 'p2_odds']
        # Ensure all needed columns are actually present before selecting
        missing_cols = [c for c in cols_to_select if c not in df.columns]
        if missing_cols:
            print(f"  Error: Intermediate Betcenter DF missing columns needed for selection: {missing_cols}")
            return None
        df_out = df[cols_to_select].copy()

        # NOW rename the odds columns
        df_out.rename(columns={'p1_odds': 'bc_p1_odds', 'p2_odds': 'bc_p2_odds'}, inplace=True)
        # --- *** END FIX *** ---

        # Convert renamed columns to numeric
        df_out['bc_p1_odds'] = pd.to_numeric(df_out['bc_p1_odds'], errors='coerce')
        df_out['bc_p2_odds'] = pd.to_numeric(df_out['bc_p2_odds'], errors='coerce')
        df_out.dropna(subset=['bc_p1_odds', 'bc_p2_odds'], inplace=True) # Drop if odds conversion failed

        print(f"  Prepared Betcenter data. Shape: {df_out.shape}")
        if not df_out.empty: print(f"  Sample Betcenter preprocessed keys:\n{df_out[MERGE_KEY_COLS].head(3)}")
        return df_out
    except Exception as e: print(f"  Error loading/preparing Betcenter data: {e}"); traceback.print_exc(); return None


# --- Merge Function (Using Keys) ---
def merge_data(sackmann_df: pd.DataFrame, betcenter_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Merges Sackmann and Betcenter dataframes based on standardized keys."""
    if betcenter_df is None or betcenter_df.empty:
        print("Betcenter data is missing or empty. Returning only Sackmann data with placeholder columns.")
        for col in ['bc_p1_odds', 'bc_p2_odds', 'p1_spread', 'p2_spread']:
             if col not in sackmann_df.columns: sackmann_df[col] = np.nan
        return sackmann_df.drop(columns=[col for col in MERGE_KEY_COLS if col in sackmann_df.columns], errors='ignore')

    print("Attempting to merge Sackmann and Betcenter data on standardized keys...")
    try:
        # (Merge logic remains the same, including debug prints)
        if not all(key in sackmann_df.columns for key in MERGE_KEY_COLS): print(f"Error: Sackmann DF missing keys ({MERGE_KEY_COLS}). Cols: {sackmann_df.columns.tolist()}"); return sackmann_df.drop(columns=[col for col in MERGE_KEY_COLS if col in sackmann_df.columns], errors='ignore')
        if not all(key in betcenter_df.columns for key in MERGE_KEY_COLS): print(f"Error: Betcenter DF missing keys ({MERGE_KEY_COLS}). Cols: {betcenter_df.columns.tolist()}"); return sackmann_df.drop(columns=[col for col in MERGE_KEY_COLS if col in sackmann_df.columns], errors='ignore')
        print("\n--- Debugging Merge ---"); print(f"Sackmann DF Head (Merge Keys - {len(sackmann_df)} rows):"); print(sackmann_df[MERGE_KEY_COLS].head())
        print(f"\nBetcenter DF Head (Merge Keys - {len(betcenter_df)} rows):"); print(betcenter_df[MERGE_KEY_COLS].head()); print("-----------------------\n")
        # Select only necessary columns from Betcenter before merge
        betcenter_merge_data = betcenter_df[['bc_p1_odds', 'bc_p2_odds'] + MERGE_KEY_COLS] # Use the renamed columns now

        merged_df = pd.merge(sackmann_df, betcenter_merge_data, on=MERGE_KEY_COLS, how='left')
        print(f"  Left Merged (P1-P1, P2-P2) on keys. Shape: {merged_df.shape}"); matches_found_count = merged_df['bc_p1_odds'].notna().sum(); print(f"  Matches found in initial merge: {matches_found_count}")

        unmatched_sackmann = merged_df[merged_df['bc_p1_odds'].isna()].copy()
        if not unmatched_sackmann.empty:
            print(f"  {len(unmatched_sackmann)} Sackmann rows still unmatched. Attempting swapped merge...")
            betcenter_swapped = betcenter_merge_data.rename(columns={'Player1NameKey': 'Player2NameKey', 'Player2NameKey': 'Player1NameKey', 'bc_p1_odds': 'temp_bc_p2_odds', 'bc_p2_odds': 'temp_bc_p1_odds'})
            betcenter_swapped.rename(columns={'temp_bc_p1_odds': 'bc_p1_odds', 'temp_bc_p2_odds': 'bc_p2_odds'}, inplace=True)
            swapped_merge_result = pd.merge(unmatched_sackmann.drop(columns=['bc_p1_odds', 'bc_p2_odds']), betcenter_swapped, on=MERGE_KEY_COLS, how='left')
            print(f"  Swapped merge result shape: {swapped_merge_result.shape}")
            swapped_matches_found = swapped_merge_result[swapped_merge_result['bc_p1_odds'].notna()]
            print(f"  Matches found via swapped merge: {len(swapped_matches_found)}")
            if not swapped_matches_found.empty:
                 merged_df.set_index(MERGE_KEY_COLS, inplace=True); swapped_matches_found.set_index(MERGE_KEY_COLS, inplace=True)
                 merged_df.update(swapped_matches_found[['bc_p1_odds', 'bc_p2_odds']]); merged_df.reset_index(inplace=True)
                 print("  Updated main dataframe with swapped matches.")

        merged_df.drop(columns=[col for col in MERGE_KEY_COLS if col in merged_df.columns], errors='ignore', inplace=True)

        print("  Calculating odds spread (Betcenter - Sackmann)...")
        merged_df['Player1_Match_Odds'] = pd.to_numeric(merged_df['Player1_Match_Odds'], errors='coerce'); merged_df['Player2_Match_Odds'] = pd.to_numeric(merged_df['Player2_Match_Odds'], errors='coerce')
        merged_df['bc_p1_odds'] = pd.to_numeric(merged_df['bc_p1_odds'], errors='coerce'); merged_df['bc_p2_odds'] = pd.to_numeric(merged_df['bc_p2_odds'], errors='coerce')
        merged_df['p1_spread'] = merged_df['bc_p1_odds'] - merged_df['Player1_Match_Odds']; merged_df['p2_spread'] = merged_df['bc_p2_odds'] - merged_df['Player2_Match_Odds']
        print("  Spread calculated.")

        print(f"Final merged data shape before display: {merged_df.shape}")
        print("Sample of merged data (Head):"); print(merged_df[['TournamentName', 'Player1Name', 'Player2Name', 'Player1_Match_Odds', 'bc_p1_odds', 'p1_spread']].head())
        return merged_df
    except Exception as e:
        print(f"Error during data merging or spread calculation: {e}"); traceback.print_exc()
        if 'sackmann_df' in locals():
             for col in ['bc_p1_odds', 'bc_p2_odds', 'p1_spread', 'p2_spread']:
                  if col not in sackmann_df.columns: sackmann_df[col] = np.nan
             return sackmann_df.drop(columns=[col for col in MERGE_KEY_COLS if col in sackmann_df.columns], errors='ignore')
        else: return pd.DataFrame()

# --- HTML Generation ---
# (apply_table_styles and generate_html_table remain the same)
def apply_table_styles(row: pd.Series) -> List[str]:
    """Applies CSS classes for value bets and spread highlighting."""
    styles = [''] * len(DISPLAY_COLS_ORDERED)
    try:
        sack_odds_p1 = pd.to_numeric(row.get('Player1_Match_Odds'), errors='coerce'); bc_odds_p1 = pd.to_numeric(row.get('bc_p1_odds'), errors='coerce')
        sack_odds_p2 = pd.to_numeric(row.get('Player2_Match_Odds'), errors='coerce'); bc_odds_p2 = pd.to_numeric(row.get('bc_p2_odds'), errors='coerce')
        if not pd.isna(sack_odds_p1) and not pd.isna(bc_odds_p1) and bc_odds_p1 >= sack_odds_p1 * VALUE_BET_THRESHOLD:
            try: styles[DISPLAY_COLS_ORDERED.index('bc_p1_odds')] = 'value-bet-p1'
            except ValueError: pass
        if not pd.isna(sack_odds_p2) and not pd.isna(bc_odds_p2) and bc_odds_p2 >= sack_odds_p2 * VALUE_BET_THRESHOLD:
            try: styles[DISPLAY_COLS_ORDERED.index('bc_p2_odds')] = 'value-bet-p2'
            except ValueError: pass
    except Exception as e_val: print(f"Warning: Error during value bet styling: {e_val}")
    try:
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
    """Generates the HTML table using Pandas Styler."""
    if df is None or df.empty: return format_error_html_for_table("No combined match data available to display.")
    try:
        print("Formatting final merged data for display...")
        df_numeric = df.copy(); df_display = df.copy()
        formatters = {
            'Player1_Match_Prob': '{:.1f}%'.format, 'Player2_Match_Prob': '{:.1f}%'.format,
            'Player1_Match_Odds': '{:.2f}'.format, 'Player2_Match_Odds': '{:.2f}'.format,
            'bc_p1_odds': '{:.2f}'.format, 'bc_p2_odds': '{:.2f}'.format,
            'p1_spread': '{:+.2f}'.format, 'p2_spread': '{:+.2f}'.format
        }
        for col, fmt in formatters.items():
            if col in df_display.columns:
                 df_display[col] = pd.to_numeric(df_display[col], errors='coerce').map(fmt, na_action='ignore')
        df_display.fillna('-', inplace=True) # Fill NaNs AFTER formatting
        print("Data formatting complete.")
        try: # Sorting
            round_map = {'R128': 128, 'R64': 64, 'R32': 32, 'R16': 16, 'QF': 8, 'SF': 4, 'F': 2, 'W': 1}
            df_display['RoundSort'] = df_display['Round'].map(round_map).fillna(999)
            df_display.sort_values(by=['TournamentName', 'RoundSort'], inplace=True, na_position='last')
            df_numeric = df_numeric.loc[df_display.index]
            df_display.drop(columns=['RoundSort'], inplace=True)
            print("Sorted matchups by Tournament and Round.")
        except Exception as e: print(f"Warning: Error during sorting: {e}")
        missing_display_cols = [col for col in DISPLAY_COLS_ORDERED if col not in df_display.columns]
        if missing_display_cols: return format_error_html_for_table(f"Data missing columns: {', '.join(missing_display_cols)}.")
        df_display_final = df_display[DISPLAY_COLS_ORDERED]; df_numeric_final = df_numeric[DISPLAY_COLS_ORDERED]
        df_display_final.columns = DISPLAY_HEADERS
        print("Applying styles and generating HTML table string using Styler...")
        styler = df_numeric_final.style.apply(apply_table_styles, axis=1)
        styler.set_table_attributes('class="dataframe"')
        styler.data = df_display_final
        html_table = styler.to_html(index=False, escape=True, na_rep='-', border=0)
        if not html_table or not isinstance(html_table, str): return format_error_html_for_table("Failed to generate HTML table using pandas Styler.")
        print("HTML table string generated successfully via Styler.")
        return html_table
    except KeyError as e: print(f"Error generating HTML table: Missing column {e}"); traceback.print_exc(); return format_error_html_for_table(f"Internal Error: Missing column '{e}'.")
    except Exception as e: print(f"Error generating HTML table: {e}"); traceback.print_exc(); return format_error_html_for_table(f"Unexpected error during HTML table generation: {type(e).__name__}")

def generate_full_html_page(table_content_html: str, timestamp_str: str) -> str:
    """Constructs the entire HTML page, embedding the table and timestamp."""
    # (CSS and HTML structure remain the same)
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
        .{ERROR_MESSAGE_CLASS} {{ /* Error styles applied inline */ }}
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
    print("Starting page generation process (Sackmann + Betcenter Integration + Spread)...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_abs = os.path.join(script_dir, DATA_DIR)
    output_file_abs = os.path.join(script_dir, OUTPUT_HTML_FILE)
    print(f"Script directory: {script_dir}"); print(f"Looking for latest CSVs in: {data_dir_abs}"); print(f"Outputting generated HTML to: {output_file_abs}")

    print("\nFinding latest data files...")
    latest_sackmann_csv = find_latest_csv(data_dir_abs, SACKMANN_CSV_PATTERN)
    latest_betcenter_csv = find_latest_csv(data_dir_abs, BETCENTER_CSV_PATTERN)

    sackmann_data = None; betcenter_data = None; error_msg = ""
    if latest_sackmann_csv:
        sackmann_data = load_and_prepare_sackmann_data(latest_sackmann_csv)
        if sackmann_data is None or sackmann_data.empty: error_msg += f"Failed to load/prepare valid Sackmann data from {os.path.basename(latest_sackmann_csv)}. "
    else: error_msg += f"Could not find latest Sackmann data file ({SACKMANN_CSV_PATTERN}). "; print(error_msg)
    if latest_betcenter_csv:
        betcenter_data = load_and_prepare_betcenter_data(latest_betcenter_csv) # This might return None on KeyError
        if betcenter_data is None or betcenter_data.empty: print(f"Warning: Failed to load/prepare valid Betcenter data from {os.path.basename(latest_betcenter_csv)}. Proceeding without it.")
    else: print(f"Warning: No Betcenter data file found ({BETCENTER_CSV_PATTERN}). Proceeding without it.")

    # Initialize table_html_content here to ensure it's always defined
    table_html_content = ""
    merged_data = None # Initialize merged_data

    # Proceed only if Sackmann data loaded successfully
    if sackmann_data is not None and not sackmann_data.empty:
        try:
            merged_data = merge_data(sackmann_data, betcenter_data) # Betcenter_data might be None here
        except Exception as merge_err:
             print(f"CRITICAL ERROR during merge_data: {merge_err}")
             error_msg += f"Error during data merge: {merge_err}. "
             merged_data = None # Ensure merged_data is None on merge error

        # Generate table ONLY if merge was successful (or if only Sackmann data was used)
        if merged_data is not None and not merged_data.empty:
            print(f"\nGenerating HTML table content from merged data (Shape: {merged_data.shape})...")
            table_html_content = generate_html_table(merged_data)
            if ERROR_MESSAGE_CLASS in table_html_content: print("generate_html_table reported an error.")
        else:
             # This case handles if merge failed OR if Sackmann was fine but Betcenter was missing/failed to load
             print(f"\nNo data available for table generation after merge step. Using error message: {error_msg}")
             final_error_msg = error_msg if error_msg else "Error: No valid data after merge."
             table_html_content = format_error_html_for_table(final_error_msg.strip())

    else:
        # This case handles if Sackmann data failed to load initially
        print(f"\nSackmann data failed to load. Cannot generate table. Using error message: {error_msg}")
        final_error_msg = error_msg if error_msg else "Error: Failed to load primary Sackmann data."
        table_html_content = format_error_html_for_table(final_error_msg.strip())


    update_time = datetime.now(pytz.timezone('Europe/Brussels')).strftime('%Y-%m-%d %H:%M:%S %Z')
    timestamp_str = f"Last updated: {html.escape(update_time)}"
    print("\nGenerating full HTML page content...");
    # table_content_html should always have a value here (table or error message)
    full_html = generate_full_html_page(table_content_html, timestamp_str);
    print("Full HTML page content generated.")

    try:
        print(f"Writing generated HTML content to: {output_file_abs}")
        with open(output_file_abs, 'w', encoding='utf-8') as f: f.write(full_html)
        print(f"Successfully wrote generated HTML to {os.path.basename(output_file_abs)}")
    except Exception as e: print(f"CRITICAL ERROR writing final HTML file: {e}"); traceback.print_exc()

    print("\nPage generation process complete.")

