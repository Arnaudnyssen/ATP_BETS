# generate_page.py (v17 - Apply Styles to Subset)
# Applies comparison table styles only to relevant spread columns
# using the 'subset' argument to potentially avoid Styler KeyError.

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
# INTERESTING_SPREAD_THRESHOLD = 0.50 # Not used for highlighting

# --- Column Definitions ---
COMP_COLS_ORDERED = [
    'TournamentName', 'Round', 'Player1Name', 'Player2Name',
    'Player1_Match_Prob', 'bc_p1_prob', 'Player2_Match_Prob', 'bc_p2_prob',
    'Player1_Match_Odds', 'bc_p1_odds', 'Player2_Match_Odds', 'bc_p2_odds',
    'p1_spread', 'rel_p1_spread', 'p2_spread', 'rel_p2_spread'
]
COMP_HEADERS = [
    "Tournament", "R", "Player 1", "Player 2",
    "P1 Prob (S)", "P1 Prob (BC)", "P2 Prob (S)", "P2 Prob (BC)",
    "P1 Odds (S)", "P1 Odds (BC)", "P2 Odds (S)", "P2 Odds (BC)",
    "P1 Sprd", "Rel Sprd", "P2 Sprd", "Rel Sprd"
]
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
    Calculates styles for spread sign highlighting.
    Returns a list of styles intended for the spread columns.
    NOTE: The Styler will apply these based on the 'subset' argument.
    """
    # Initialize styles for the number of columns *expected* to be styled
    # (p1_spread, rel_p1_spread, p2_spread, rel_p2_spread) -> 4 columns
    num_style_cols = 4
    styles = [''] * num_style_cols # Only n