# process_data.py (v8 - Calculate Relative Spread)
# Loads latest Sackmann and Betcenter data, preprocesses, merges, calculates
# absolute spread, relative spread, normalized Betcenter probabilities,
# and saves the final combined DataFrame to processed_comparison_*.csv.

import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob
import pytz # Keep for potential future use
import traceback
import re
from typing import Optional, List, Tuple, Any

# --- Constants ---
DATA_DIR = "data_archive"
SACKMANN_CSV_PATTERN = "sackmann_matchups_*.csv"
BETCENTER_CSV_PATTERN = "betcenter_odds_*.csv"
PROCESSED_OUTPUT_FILENAME_BASE = "processed_comparison"
DATE_FORMAT = "%Y%m%d"

# --- Column Definitions ---
MERGE_KEY_COLS = ['TournamentKey', 'Player1NameKey', 'Player2NameKey']
# Added relative spread columns
FINAL_COLS = [
    'TournamentName', 'Round', 'Player1Name', 'Player2Name',
    'Player1_Match_Prob', 'Player2_Match_Prob', 'bc_p1_prob', 'bc_p2_prob',
    'Player1_Match_Odds', 'Player2_Match_Odds', 'bc_p1_odds', 'bc_p2_odds',
    'p1_spread', 'p2_spread', 'rel_p1_spread', 'rel_p2_spread' # Added relative spreads
]


# --- Helper Functions ---
# (create_merge_key, preprocess_player_name, find_latest_csv remain the same as v7)
def create_merge_key(text: str) -> str:
    """Creates a simplified, lowercase, space-removed key for merging."""
    if not isinstance(text, str): return ""
    try:
        key = text.lower()
        key = key.replace('barcelone', 'barcelona').replace('hambourg', 'hamburg').replace('genève', 'geneva').replace('tbilissi', 'tbilisi') # Standardize spelling
        prefixes_suffixes_to_remove = [
            "tennis - ", ", qualifying", ", spain", ", germany", "atp", "challenger",
            "qualification","macédoine du nord"
        ]
        for item in prefixes_suffixes_to_remove:
            key = key.replace(item, "")
        key = key.strip()
        key = re.sub(r'\d+$', '', key) # Remove trailing digits
        key = re.sub(r'[^\w]', '', key) # Keep only alphanumeric
        return key
    except Exception as e:
        print(f"Warning: Error creating merge key for '{text}': {e}")
        return ""

def preprocess_player_name(name: str) -> Tuple[str, str]:
    """Standardizes player names (Title Case) and creates a merge key."""
    display_name = ""; merge_key_name = ""
    if not isinstance(name, str): return display_name, merge_key_name
    try:
        if ',' in name:
            parts = [part.strip() for part in name.split(',')];
            if len(parts) == 2:
                 first_name_part = parts[1]
                 if first_name_part.endswith('.') and len(first_name_part) <= 2:
                      first_name_part = first_name_part[:-1]
                 name = f"{first_name_part} {parts[0]}"
            else: name = " ".join(parts)
        display_name = re.sub(r'\s*\([^)]*\)', '', name).strip()
        display_name = re.sub(r'^\*|\*$', '', display_name).strip()
        display_name = re.sub(r'\.$', '', display_name).strip()
        display_name = display_name.title()
        display_name = re.sub(r'\s+', ' ', display_name).strip()
        merge_key_name = create_merge_key(display_name)
        return display_name, merge_key_name
    except Exception as e:
        print(f"Warning: Could not preprocess player name '{name}': {e}")
        return name.title(), create_merge_key(name)

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

# --- Data Loading Functions ---
# (load_and_prepare_sackmann_data, load_and_prepare_betcenter_data remain the same as v7)
def load_and_prepare_sackmann_data(csv_filepath: str) -> Optional[pd.DataFrame]:
    """Loads, preprocesses, filters, and standardizes Sackmann data."""
    print(f"Loading Sackmann data from: {os.path.basename(csv_filepath)}")
    if not os.path.exists(csv_filepath) or os.path.getsize(csv_filepath) == 0: print("  Sackmann file is missing or empty."); return None
    try:
        df = pd.read_csv(csv_filepath)
        if df.empty: print("  Sackmann DataFrame is empty after loading."); return None
        print(f"  Read {len(df)} rows initially from Sackmann CSV.")
        required_cols = ['TournamentName', 'TournamentURL', 'Player1Name', 'Player2Name', 'Player1_Match_Prob', 'Player2_Match_Prob', 'Player1_Match_Odds', 'Player2_Match_Odds'] # Added odds cols
        if not all(col in df.columns for col in required_cols): print(f"  Error: Sackmann DataFrame missing required columns. Found: {df.columns.tolist()}"); return None
        df['Player1_Match_Prob'] = pd.to_numeric(df['Player1_Match_Prob'], errors='coerce')
        df['Player2_Match_Prob'] = pd.to_numeric(df['Player2_Match_Prob'], errors='coerce')
        df['Player1_Match_Odds'] = pd.to_numeric(df['Player1_Match_Odds'], errors='coerce') # Ensure odds are numeric
        df['Player2_Match_Odds'] = pd.to_numeric(df['Player2_Match_Odds'], errors='coerce')
        original_count_step1 = len(df)
        df = df[ (df['Player1_Match_Prob'].notna()) & (df['Player1_Match_Prob'] > 0.0) & (df['Player1_Match_Prob'] < 100.0) & \
                 (df['Player2_Match_Prob'].notna()) & (df['Player2_Match_Prob'] > 0.0) & (df['Player2_Match_Prob'] < 100.0) ].copy()
        print(f"  Filtered Sackmann (Prob = 0%, 100%, NaN): {original_count_step1 - len(df)} rows removed. {len(df)} remain.")
        if df.empty: print("  Sackmann DataFrame is empty after filtering 0/100 probs."); return None

        df['TournamentKey'] = df['TournamentName'].astype(str).apply(create_merge_key)
        df['OrigTournamentName'] = df['TournamentName']
        df['TournamentName'] = df['OrigTournamentName'].astype(str).apply(lambda x: x.title())
        df[['Player1Name', 'Player1NameKey']] = df['Player1Name'].astype(str).apply(lambda x: pd.Series(preprocess_player_name(x)))
        df[['Player2Name', 'Player2NameKey']] = df['Player2Name'].astype(str).apply(lambda x: pd.Series(preprocess_player_name(x)))

        original_count_step2 = len(df)
        mask_p1_qualifier = df['Player1Name'].str.contains('Qualifier', case=False, na=False)
        mask_p2_qualifier = df['Player2Name'].str.contains('Qualifier', case=False, na=False)
        df = df[~(mask_p1_qualifier | mask_p2_qualifier)].copy()
        print(f"  Filtered Sackmann (Qualifiers): {original_count_step2 - len(df)} rows removed. {len(df)} remain.")
        if df.empty: print("  Sackmann DataFrame is empty after filtering qualifiers."); return None

        sackmann_cols_keep = ['TournamentName', 'TournamentURL', 'Round', 'Player1Name', 'Player2Name',
                              'Player1_Match_Prob', 'Player2_Match_Prob',
                              'Player1_Match_Odds', 'Player2_Match_Odds'] + MERGE_KEY_COLS
        df_out = df[[col for col in sackmann_cols_keep if col in df.columns]].copy()
        # Ensure odds are numeric again after selection (redundant but safe)
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

        df['TournamentKey'] = df['tournament'].astype(str).apply(create_merge_key)
        df['TournamentName'] = df['tournament'].astype(str).apply(lambda x: x.replace("Tennis - ", "").strip().title())
        df[['Player1Name', 'Player1NameKey']] = df['p1_name'].astype(str).apply(lambda x: pd.Series(preprocess_player_name(x)))
        df[['Player2Name', 'Player2NameKey']] = df['p2_name'].astype(str).apply(lambda x: pd.Series(preprocess_player_name(x)))

        cols_to_select = ['TournamentName'] + MERGE_KEY_COLS + ['p1_odds', 'p2_odds']
        missing_cols = [c for c in cols_to_select if c not in df.columns]
        if missing_cols: print(f"  Error: Intermediate Betcenter DF missing columns: {missing_cols}"); return None

        df_out = df[cols_to_select].copy()
        df_out.rename(columns={'p1_odds': 'bc_p1_odds', 'p2_odds': 'bc_p2_odds'}, inplace=True)
        df_out['bc_p1_odds'] = pd.to_numeric(df_out['bc_p1_odds'], errors='coerce')
        df_out['bc_p2_odds'] = pd.to_numeric(df_out['bc_p2_odds'], errors='coerce')
        df_out.dropna(subset=['bc_p1_odds', 'bc_p2_odds'], inplace=True)
        print(f"  Prepared Betcenter data. Shape: {df_out.shape}")
        if not df_out.empty: print(f"  Sample Betcenter preprocessed keys:\n{df_out[MERGE_KEY_COLS].head(3)}")
        return df_out
    except Exception as e: print(f"  Error loading/preparing Betcenter data: {e}"); traceback.print_exc(); return None

import pandas as pd
import numpy as np
from typing import Optional

# Placeholder for the merge key columns, ensure this is defined in your actual code
# Example: MERGE_KEY_COLS = ['DateKey', 'TournamentKey', 'Player1NameKey', 'Player2NameKey']
MERGE_KEY_COLS = ['StandardizedDate', 'StandardizedTournament', 'Player1NameKey', 'Player2NameKey'] # Replace with your actual keys

def merge_data(sackmann_df: pd.DataFrame, betcenter_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Merges Sackmann and Betcenter dataframes based on standardized keys, handling swaps."""
    if sackmann_df is None or sackmann_df.empty:
        print("Sackmann data is missing or empty. Cannot merge.")
        return pd.DataFrame()

    # Store original Sackmann index to ensure we can map back to it for updates
    # This is crucial because pd.merge on columns will reset the index for merged_df.
    original_sackmann_index = sackmann_df.index.copy()

    placeholder_cols = ['bc_p1_odds', 'bc_p2_odds', 'bc_p1_prob', 'bc_p2_prob', 
                        'p1_spread', 'p2_spread', 'rel_p1_spread', 'rel_p2_spread']

    if betcenter_df is None or betcenter_df.empty:
        print("Betcenter data is missing or empty. Adding placeholder columns to Sackmann data.")
        final_df = sackmann_df.copy()
        for col in placeholder_cols:
            if col not in final_df.columns:
                final_df[col] = np.nan
        return final_df
    else:
        print("Attempting to merge Sackmann and Betcenter data on standardized keys...")
        try:
            if not all(key in sackmann_df.columns for key in MERGE_KEY_COLS):
                print(f"Error: Sackmann DF missing one or more merge keys from {MERGE_KEY_COLS}. Sackmann columns: {sackmann_df.columns.tolist()}")
                # Return a copy to avoid modifying original if it's passed around, with placeholders
                final_df = sackmann_df.copy()
                for col in placeholder_cols:
                    if col not in final_df.columns:
                        final_df[col] = np.nan
                return final_df
            if not all(key in betcenter_df.columns for key in MERGE_KEY_COLS):
                print(f"Error: Betcenter DF missing one or more merge keys from {MERGE_KEY_COLS}. Betcenter columns: {betcenter_df.columns.tolist()}")
                # If Betcenter is faulty, return Sackmann with placeholders
                final_df = sackmann_df.copy()
                for col in placeholder_cols:
                    if col not in final_df.columns:
                        final_df[col] = np.nan
                return final_df

            print("\n--- Debugging Merge ---")
            print(f"Sackmann DF Head (Keys - {len(sackmann_df)} rows, Index: {sackmann_df.index[:5].tolist()}...):")
            print(sackmann_df[MERGE_KEY_COLS].head())
            print(f"\nBetcenter DF Head (Keys - {len(betcenter_df)} rows, Index: {betcenter_df.index[:5].tolist()}...):")
            print(betcenter_df[MERGE_KEY_COLS].head())
            print("-----------------------\n")

            # Prepare Betcenter data for merge (select necessary columns)
            # Ensure all MERGE_KEY_COLS and the odds columns are present in betcenter_df before selection
            betcenter_cols_to_select = [col for col in ['bc_p1_odds', 'bc_p2_odds'] + MERGE_KEY_COLS if col in betcenter_df.columns]
            if not all(key in betcenter_cols_to_select for key in MERGE_KEY_COLS + ['bc_p1_odds', 'bc_p2_odds']):
                 print(f"Warning: Betcenter DF is missing some expected columns for merge (bc_p1_odds, bc_p2_odds or merge keys). Available: {betcenter_df.columns.tolist()}")
                 # Fallback if essential columns are missing from betcenter after key check
                 final_df = sackmann_df.copy()
                 for col_ph in placeholder_cols:
                     if col_ph not in final_df.columns:
                         final_df[col_ph] = np.nan
                 return final_df

            betcenter_merge_data = betcenter_df[betcenter_cols_to_select].copy()


            # Perform the initial merge (P1-P1, P2-P2)
            # The resulting merged_df will have a new RangeIndex [0, 1, ..., N-1]
            merged_df = pd.merge(
                sackmann_df.reset_index(drop=True), # Reset index to ensure positional alignment
                betcenter_merge_data,
                on=MERGE_KEY_COLS,
                how='left',
                suffixes=('', '_bc_initial') # Suffix for any overlapping columns from betcenter_merge_data not in MERGE_KEY_COLS
            )
            print(f"  Left Merged (P1-P1, P2-P2) on keys. Shape: {merged_df.shape}")
            matches_found_count = merged_df['bc_p1_odds'].notna().sum()
            print(f"  Matches found in initial merge: {matches_found_count}")

            # Identify rows in merged_df that were unmatched (these correspond positionally to original sackmann_df rows)
            unmatched_mask_in_merged_df = merged_df['bc_p1_odds'].isna()

            if unmatched_mask_in_merged_df.any():
                num_unmatched = unmatched_mask_in_merged_df.sum()
                print(f"  {num_unmatched} Sackmann rows (by position in merged_df) still unmatched. Attempting swapped merge...")

                # Prepare Betcenter data for a swapped merge
                betcenter_swapped = betcenter_merge_data.rename(columns={
                    'Player1NameKey': 'Player2NameKey_temp', # Use temp to avoid immediate overwrite if Player2NameKey exists
                    'Player2NameKey': 'Player1NameKey',
                    'bc_p1_odds': 'temp_bc_p2_odds',
                    'bc_p2_odds': 'temp_bc_p1_odds'
                })
                # Now rename Player2NameKey_temp to Player2NameKey
                if 'Player2NameKey_temp' in betcenter_swapped.columns:
                    betcenter_swapped.rename(columns={'Player2NameKey_temp': 'Player2NameKey'}, inplace=True)

                betcenter_swapped.rename(columns={
                    'temp_bc_p1_odds': 'bc_p1_odds',
                    'temp_bc_p2_odds': 'bc_p2_odds'
                }, inplace=True)

                # Get the subset of original sackmann_df that was unmatched.
                # We use the boolean mask derived from merged_df, applied to original sackmann_df.
                # This ensures we are working with the correct rows from sackmann_df and preserving its original index.
                unmatched_sackmann_subset = sackmann_df[unmatched_mask_in_merged_df.values].copy()
                
                # Columns to drop if they exist in unmatched_sackmann_subset (they shouldn't from the initial merge's 'left' nature)
                # This is more of a safeguard.
                cols_to_drop_if_exist_in_subset = ['bc_p1_odds', 'bc_p2_odds'] # Odds cols that might have been added if merge was different
                existing_cols_to_drop = [col for col in cols_to_drop_if_exist_in_subset if col in unmatched_sackmann_subset.columns]
                
                # Perform the swapped merge.
                # The left DataFrame (unmatched_sackmann_subset) has original sackmann_df indices.
                # The result (swapped_merge_result) will also have these original sackmann_df indices.
                swapped_merge_result = pd.merge(
                    unmatched_sackmann_subset.drop(columns=existing_cols_to_drop, errors='ignore'),
                    betcenter_swapped,
                    on=MERGE_KEY_COLS,
                    how='left',
                    suffixes=('', '_bc_swap') # Suffix for any new overlapping columns
                )

                # Identify rows that found a match in the swapped merge
                swapped_matches_found_df = swapped_merge_result[swapped_merge_result['bc_p1_odds'].notna()]
                print(f"  Matches found via swapped merge: {len(swapped_matches_found_df)}")

                if not swapped_matches_found_df.empty:
                    # swapped_matches_found_df.index contains original sackmann_df labels for the rows that got a swapped match.
                    # We need to update 'merged_df' (which has a 0-based RangeIndex).
                    # To do this, find the integer positions in the original_sackmann_index that correspond to these labels.
                    
                    # Get the original sackmann_df labels of the successfully swapped rows
                    sackmann_labels_of_swapped_matches = swapped_matches_found_df.index
                    
                    # Get the integer positions of these labels in the original_sackmann_index
                    # These positions directly map to the 0-based index of merged_df
                    positions_in_merged_df_to_update = original_sackmann_index.get_indexer(sackmann_labels_of_swapped_matches)
                    
                    # Filter out any -1s if a label wasn't found (should not happen if logic is correct)
                    valid_positions = positions_in_merged_df_to_update[positions_in_merged_df_to_update != -1]
                    
                    # Ensure we are selecting data from swapped_matches_found_df using its own index (which are sackmann_labels_of_swapped_matches)
                    # and aligning it for assignment.
                    # We need to ensure that the .values are taken from data indexed by sackmann_labels_of_swapped_matches[valid_positions_mask]
                    # where valid_positions_mask corresponds to the valid original labels.
                    
                    # Get the actual labels that correspond to valid_positions
                    valid_original_labels = original_sackmann_index[valid_positions]
                    valid_swapped_data = swapped_matches_found_df.loc[valid_original_labels]

                    if len(valid_positions) > 0:
                        # Ensure columns exist in merged_df before assignment, though 'left' merge should create them as NaN
                        if 'bc_p1_odds' not in merged_df.columns: merged_df['bc_p1_odds'] = np.nan
                        if 'bc_p2_odds' not in merged_df.columns: merged_df['bc_p2_odds'] = np.nan
                        
                        merged_df.loc[valid_positions, 'bc_p1_odds'] = valid_swapped_data['bc_p1_odds'].values
                        merged_df.loc[valid_positions, 'bc_p2_odds'] = valid_swapped_data['bc_p2_odds'].values
                        print(f"  Updated {len(valid_positions)} rows in main dataframe with swapped matches.")
            
            # Restore the original Sackmann index to the final DataFrame
            merged_df.index = original_sackmann_index
            final_df = merged_df

        except Exception as e:
            print(f"Error during data merging: {e}")
            import traceback
            traceback.print_exc()
            print("Fallback: Returning only Sackmann data due to merge error.")
            final_df = sackmann_df.copy()
            for col in placeholder_cols:
                if col not in final_df.columns:
                    final_df[col] = np.nan
        return final_df

    # --- Calculate Spread ---
    print("Calculating odds spread (Betcenter - Sackmann)...")
    final_df['Player1_Match_Odds'] = pd.to_numeric(final_df['Player1_Match_Odds'], errors='coerce')
    final_df['Player2_Match_Odds'] = pd.to_numeric(final_df['Player2_Match_Odds'], errors='coerce')
    final_df['bc_p1_odds'] = pd.to_numeric(final_df['bc_p1_odds'], errors='coerce')
    final_df['bc_p2_odds'] = pd.to_numeric(final_df['bc_p2_odds'], errors='coerce')
    final_df['p1_spread'] = np.where(final_df['bc_p1_odds'].notna() & final_df['Player1_Match_Odds'].notna(),
                                   final_df['bc_p1_odds'] - final_df['Player1_Match_Odds'], np.nan)
    final_df['p2_spread'] = np.where(final_df['bc_p2_odds'].notna() & final_df['Player2_Match_Odds'].notna(),
                                   final_df['bc_p2_odds'] - final_df['Player2_Match_Odds'], np.nan)
    print("Spread calculated.")

    # --- Calculate Relative Spread ---
    print("Calculating relative spread...")
    # Calculate relative spread: spread / sackmann_odds
    # Handle division by zero or NaN in sackmann_odds
    final_df['rel_p1_spread'] = np.where(final_df['p1_spread'].notna() & final_df['Player1_Match_Odds'].notna() & (final_df['Player1_Match_Odds'] > 0),
                                       final_df['p1_spread'] / final_df['Player1_Match_Odds'], np.nan)
    final_df['rel_p2_spread'] = np.where(final_df['p2_spread'].notna() & final_df['Player2_Match_Odds'].notna() & (final_df['Player2_Match_Odds'] > 0),
                                       final_df['p2_spread'] / final_df['Player2_Match_Odds'], np.nan)
    print("Relative spread calculated.")

    # --- Calculate Normalized Betcenter Probabilities ---
    print("Calculating normalized Betcenter probabilities...")
    raw_p1 = np.where(final_df['bc_p1_odds'] > 0, 1 / final_df['bc_p1_odds'], 0)
    raw_p2 = np.where(final_df['bc_p2_odds'] > 0, 1 / final_df['bc_p2_odds'], 0)
    total_raw_prob = raw_p1 + raw_p2
    final_df['bc_p1_prob'] = np.where(total_raw_prob > 0, (raw_p1 / total_raw_prob) * 100, np.nan)
    final_df['bc_p2_prob'] = np.where(total_raw_prob > 0, (raw_p2 / total_raw_prob) * 100, np.nan)
    print("Betcenter probabilities calculated.")

    # Drop the key columns before returning/saving
    final_df.drop(columns=[col for col in MERGE_KEY_COLS if col in final_df.columns], errors='ignore', inplace=True)

    # Reorder columns for final output
    final_df = final_df[[col for col in FINAL_COLS if col in final_df.columns]]

    print(f"Final processed data shape: {final_df.shape}")
    print("Sample of final processed data (Head):")
    print(final_df[['TournamentName', 'Player1Name', 'Player2Name', 'p1_spread', 'rel_p1_spread']].head())
    return final_df

# --- Main Execution Logic ---
# (Main execution block remains the same)
if __name__ == "__main__":
    print("="*50); print(" Starting Data Processing Script..."); print("="*50)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_abs = os.path.join(script_dir, DATA_DIR)

    print("\nFinding latest input data files...")
    latest_sackmann_csv = find_latest_csv(data_dir_abs, SACKMANN_CSV_PATTERN)
    latest_betcenter_csv = find_latest_csv(data_dir_abs, BETCENTER_CSV_PATTERN)

    sackmann_data = None
    betcenter_data = None

    if latest_sackmann_csv:
        sackmann_data_loaded = load_and_prepare_sackmann_data(latest_sackmann_csv)
        if sackmann_data_loaded is not None:
             sackmann_data = sackmann_data_loaded
    else:
        print("CRITICAL: Sackmann data file not found. Cannot proceed with merge.")
        exit()

    if latest_betcenter_csv:
        betcenter_data = load_and_prepare_betcenter_data(latest_betcenter_csv)
    else:
        print("Warning: Betcenter data file not found. Proceeding with Sackmann data only (no BC odds/probs/spread).")

    final_processed_data = merge_data(sackmann_data, betcenter_data)

    if final_processed_data is not None and not final_processed_data.empty:
        print("\nSaving final processed data...")
        today_date_str = datetime.now().strftime(DATE_FORMAT)
        output_filename = "today.csv"
        output_path = os.path.join(data_dir_abs, output_filename)
        try:
            # Format floats: Use more precision for relative spread
            float_cols = final_processed_data.select_dtypes(include=['float']).columns
            format_dict = {col: '%.4f' if 'rel_' in col else '%.2f' for col in float_cols}
            final_processed_data.to_csv(output_path, index=False, encoding='utf-8', float_format='%.4f') # Use higher precision default
            print(f"Successfully saved processed data to: {output_path}")
        except Exception as e:
            print(f"Error saving processed data to CSV: {e}")
            traceback.print_exc()
    else:
        print("\nNo final data generated or available to save.")

    print("\nData processing script finished.")

