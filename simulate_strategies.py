# simulate_strategies.py (v1)
# Loads processed comparison data and simulates betting strategies.
# Logs intended bets to a CSV file.

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
import traceback
from typing import Optional, List, Dict, Any

# --- Constants ---
DATA_DIR = "data_archive"
PROCESSED_CSV_PATTERN = "processed_comparison_*.csv" # Input data
STRATEGY_LOG_FILENAME = "strategy_log.csv" # Output log file
DATE_FORMAT_INPUT = "%Y%m%d" # For finding input file if needed
DATE_FORMAT_OUTPUT = "%Y-%m-%d" # For logging

# --- Strategy Parameters ---
STRATEGY1_PROB_DIFF_THRESHOLD = 5.0 # Bet if Sack_Prob > BC_Prob by 5 percentage points
STRATEGY3_KELLY_FRACTION = 0.25 # Bet 25% of the calculated Kelly stake
# Initial bankroll simulation is complex without results, skipping for now

# --- Helper Functions ---
def find_latest_csv(directory: str, pattern: str) -> Optional[str]:
    """Finds the most recently modified CSV file matching the pattern."""
    # (Same as in other scripts)
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

# --- Strategy Logic Functions ---

def apply_strategy_1(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Identifies bets for Strategy 1: Prob Diff > Threshold."""
    bets = []
    today_str = datetime.now().strftime(DATE_FORMAT_OUTPUT)
    required_cols = ['TournamentName', 'Player1Name', 'Player2Name',
                     'Player1_Match_Prob', 'bc_p1_prob', 'bc_p1_odds',
                     'Player2_Match_Prob', 'bc_p2_prob', 'bc_p2_odds']
    if not all(col in df.columns for col in required_cols):
        print("Strategy 1 Error: Missing required columns in input data.")
        return []

    for index, row in df.iterrows():
        # Check P1
        prob_diff_p1 = row['Player1_Match_Prob'] - row['bc_p1_prob']
        if not pd.isna(prob_diff_p1) and prob_diff_p1 > STRATEGY1_PROB_DIFF_THRESHOLD:
            bets.append({
                'BetDate': today_str, 'Strategy': 'S1_ProbDiff',
                'Tournament': row['TournamentName'], 'Player1': row['Player1Name'], 'Player2': row['Player2Name'],
                'BetOnPlayer': 'P1', 'BetType': f"ProbDiff>{STRATEGY1_PROB_DIFF_THRESHOLD}%",
                'TriggerValue': round(prob_diff_p1, 2), 'BetAmount': 1.0, 'BetOdds': row['bc_p1_odds'],
                'SackmannProb': row['Player1_Match_Prob'], 'BetcenterProb': row['bc_p1_prob'],
                'MatchResult': None, 'ProfitLoss': None # Cannot determine yet
            })

        # Check P2
        prob_diff_p2 = row['Player2_Match_Prob'] - row['bc_p2_prob']
        if not pd.isna(prob_diff_p2) and prob_diff_p2 > STRATEGY1_PROB_DIFF_THRESHOLD:
             bets.append({
                'BetDate': today_str, 'Strategy': 'S1_ProbDiff',
                'Tournament': row['TournamentName'], 'Player1': row['Player1Name'], 'Player2': row['Player2Name'],
                'BetOnPlayer': 'P2', 'BetType': f"ProbDiff>{STRATEGY1_PROB_DIFF_THRESHOLD}%",
                'TriggerValue': round(prob_diff_p2, 2), 'BetAmount': 1.0, 'BetOdds': row['bc_p2_odds'],
                'SackmannProb': row['Player2_Match_Prob'], 'BetcenterProb': row['bc_p2_prob'],
                'MatchResult': None, 'ProfitLoss': None
            })
    print(f"Strategy 1 identified {len(bets)} potential bets.")
    return bets

def apply_strategy_2(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Identifies bet for Strategy 2: Max Positive Spread."""
    bets = []
    today_str = datetime.now().strftime(DATE_FORMAT_OUTPUT)
    required_cols = ['TournamentName', 'Player1Name', 'Player2Name',
                     'p1_spread', 'bc_p1_odds', 'Player1_Match_Prob', 'bc_p1_prob',
                     'p2_spread', 'bc_p2_odds', 'Player2_Match_Prob', 'bc_p2_prob']
    if not all(col in df.columns for col in required_cols):
        print("Strategy 2 Error: Missing required columns in input data.")
        return []

    # Find max positive spreads, ignore NaNs
    max_p1_spread = df['p1_spread'].dropna().max()
    max_p2_spread = df['p2_spread'].dropna().max()

    best_spread = -np.inf
    best_bet = None

    if max_p1_spread > 0 and max_p1_spread > best_spread:
        best_spread = max_p1_spread
        best_row = df[df['p1_spread'] == max_p1_spread].iloc[0] # Take first if multiple max
        best_bet = {
            'BetDate': today_str, 'Strategy': 'S2_MaxSpread',
            'Tournament': best_row['TournamentName'], 'Player1': best_row['Player1Name'], 'Player2': best_row['Player2Name'],
            'BetOnPlayer': 'P1', 'BetType': "MaxSpread",
            'TriggerValue': round(best_spread, 2), 'BetAmount': 1.0, 'BetOdds': best_row['bc_p1_odds'],
            'SackmannProb': best_row['Player1_Match_Prob'], 'BetcenterProb': best_row['bc_p1_prob'],
            'MatchResult': None, 'ProfitLoss': None
        }

    if max_p2_spread > 0 and max_p2_spread > best_spread: # Check if P2 spread is even better
        best_spread = max_p2_spread
        best_row = df[df['p2_spread'] == max_p2_spread].iloc[0]
        best_bet = {
            'BetDate': today_str, 'Strategy': 'S2_MaxSpread',
            'Tournament': best_row['TournamentName'], 'Player1': best_row['Player1Name'], 'Player2': best_row['Player2Name'],
            'BetOnPlayer': 'P2', 'BetType': "MaxSpread",
            'TriggerValue': round(best_spread, 2), 'BetAmount': 1.0, 'BetOdds': best_row['bc_p2_odds'],
            'SackmannProb': best_row['Player2_Match_Prob'], 'BetcenterProb': best_row['bc_p2_prob'],
            'MatchResult': None, 'ProfitLoss': None
        }

    if best_bet:
        bets.append(best_bet)
        print(f"Strategy 2 identified best bet: {best_bet['BetOnPlayer']} in {best_bet['Tournament']} ({best_bet['Player1']} vs {best_bet['Player2']}) with spread {best_bet['TriggerValue']:.2f}")
    else:
        print("Strategy 2 identified no bets (no positive spread found).")
    return bets

def apply_strategy_3(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Identifies bets for Strategy 3: Fractional Kelly."""
    bets = []
    today_str = datetime.now().strftime(DATE_FORMAT_OUTPUT)
    required_cols = ['TournamentName', 'Player1Name', 'Player2Name',
                     'Player1_Match_Prob', 'bc_p1_odds', 'bc_p1_prob',
                     'Player2_Match_Prob', 'bc_p2_odds', 'bc_p2_prob']
    if not all(col in df.columns for col in required_cols):
        print("Strategy 3 Error: Missing required columns in input data.")
        return []

    for index, row in df.iterrows():
        # Check P1 Kelly Bet
        prob_p1 = row['Player1_Match_Prob'] / 100.0
        odds_p1 = row['bc_p1_odds']
        if not pd.isna(prob_p1) and not pd.isna(odds_p1) and odds_p1 > 1:
            edge_p1 = (prob_p1 * odds_p1) - 1
            if edge_p1 > 0: # Only bet if edge is positive
                kelly_fraction_p1 = edge_p1 / (odds_p1 - 1)
                bet_amount_p1 = STRATEGY3_KELLY_FRACTION * kelly_fraction_p1 # We bet fraction *of fraction* - needs bankroll later
                # For now, just log the fraction itself, assume fixed 1 unit stake scaled by fraction later
                bets.append({
                    'BetDate': today_str, 'Strategy': 'S3_Kelly',
                    'Tournament': row['TournamentName'], 'Player1': row['Player1Name'], 'Player2': row['Player2Name'],
                    'BetOnPlayer': 'P1', 'BetType': f"KellyEdge>{0}%",
                    'TriggerValue': round(kelly_fraction_p1, 4), # Log Kelly fraction
                    'BetAmount': round(bet_amount_p1, 4), # Log intended fractional stake (needs bankroll context)
                    'BetOdds': odds_p1,
                    'SackmannProb': row['Player1_Match_Prob'], 'BetcenterProb': row['bc_p1_prob'],
                    'MatchResult': None, 'ProfitLoss': None
                })

        # Check P2 Kelly Bet
        prob_p2 = row['Player2_Match_Prob'] / 100.0
        odds_p2 = row['bc_p2_odds']
        if not pd.isna(prob_p2) and not pd.isna(odds_p2) and odds_p2 > 1:
            edge_p2 = (prob_p2 * odds_p2) - 1
            if edge_p2 > 0:
                kelly_fraction_p2 = edge_p2 / (odds_p2 - 1)
                bet_amount_p2 = STRATEGY3_KELLY_FRACTION * kelly_fraction_p2
                bets.append({
                    'BetDate': today_str, 'Strategy': 'S3_Kelly',
                    'Tournament': row['TournamentName'], 'Player1': row['Player1Name'], 'Player2': row['Player2Name'],
                    'BetOnPlayer': 'P2', 'BetType': f"KellyEdge>{0}%",
                    'TriggerValue': round(kelly_fraction_p2, 4),
                    'BetAmount': round(bet_amount_p2, 4),
                    'BetOdds': odds_p2,
                    'SackmannProb': row['Player2_Match_Prob'], 'BetcenterProb': row['bc_p2_prob'],
                    'MatchResult': None, 'ProfitLoss': None
                })
    print(f"Strategy 3 identified {len(bets)} potential Kelly bets.")
    return bets

# --- Main Execution Logic ---
if __name__ == "__main__":
    print("="*50); print(" Starting Betting Strategy Simulation..."); print("="*50)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_abs = os.path.join(script_dir, DATA_DIR)
    log_file_path = os.path.join(data_dir_abs, STRATEGY_LOG_FILENAME)

    # 1. Find latest processed comparison file
    print("Finding latest processed comparison data...")
    latest_processed_file = find_latest_csv(data_dir_abs, PROCESSED_CSV_PATTERN)

    if not latest_processed_file:
        print("Error: No processed comparison file found. Cannot simulate strategies.")
        exit()

    # 2. Load data
    print(f"Loading data from: {os.path.basename(latest_processed_file)}")
    try:
        df_comparison = pd.read_csv(latest_processed_file)
        # Ensure necessary columns are numeric
        num_cols = ['Player1_Match_Prob', 'Player2_Match_Prob', 'bc_p1_prob', 'bc_p2_prob',
                    'Player1_Match_Odds', 'Player2_Match_Odds', 'bc_p1_odds', 'bc_p2_odds',
                    'p1_spread', 'p2_spread']
        for col in num_cols:
            if col in df_comparison.columns:
                df_comparison[col] = pd.to_numeric(df_comparison[col], errors='coerce')
            else:
                print(f"Warning: Expected numeric column '{col}' not found in data.")
                # Add missing column as NaN to prevent errors later if needed by strategies
                df_comparison[col] = np.nan

    except Exception as e:
        print(f"Error loading data from {latest_processed_file}: {e}")
        traceback.print_exc()
        exit()

    if df_comparison.empty:
        print("Loaded data is empty. No strategies to simulate.")
        exit()

    # 3. Apply Strategies
    print("\nApplying betting strategies...")
    all_bets = []
    all_bets.extend(apply_strategy_1(df_comparison))
    all_bets.extend(apply_strategy_2(df_comparison))
    all_bets.extend(apply_strategy_3(df_comparison))

    # 4. Save results to log file
    if not all_bets:
        print("\nNo bets identified by any strategy for today.")
    else:
        print(f"\nIdentified {len(all_bets)} total bets to log.")
        new_bets_df = pd.DataFrame(all_bets)

        try:
            # Check if log file exists to append or write header
            file_exists = os.path.isfile(log_file_path)
            print(f"Appending bets to: {log_file_path} (Exists: {file_exists})")
            new_bets_df.to_csv(log_file_path, mode='a', header=not file_exists, index=False, encoding='utf-8', float_format='%.4f')
            print("Successfully logged bets.")
        except Exception as e:
            print(f"Error writing to strategy log file '{log_file_path}': {e}")
            traceback.print_exc()

    print("\nStrategy simulation finished.")
