# generate_page.py (Targets specific empty divs in index.html)
# WARNING: This approach is fragile. Using comment placeholders is recommended.

import pandas as pd
from datetime import datetime
import os
import glob
import pytz
import traceback
from typing import Optional

# --- Constants ---
# WARNING: These placeholders target specific empty divs. Fragile!
TABLE_PLACEHOLDER = '<div class="table-container">\n    </div>'
TIMESTAMP_PLACEHOLDER = '<div class="last-updated">\n    </div>'

ERROR_MESSAGE_CLASS = "error-message"
DATA_DIR = "data_archive"
CSV_PATTERN = "sackmann_data_*.csv"
TEMPLATE_FILE = "index.html"
OUTPUT_HTML_FILE = "index.html"


# --- Helper Functions ---

def find_latest_csv(directory: str, pattern: str) -> Optional[str]:
    """Finds the most recently modified CSV file matching the pattern."""
    try:
        search_path = os.path.join(directory, pattern)
        list_of_files = glob.glob(search_path)
        if not list_of_files:
            print(f"No files found: {directory}/{pattern}")
            return None
        latest_file = max(list_of_files, key=os.path.getmtime)
        print(f"Found latest CSV: {latest_file}")
        return latest_file
    except Exception as e:
        print(f"Error finding latest CSV: {e}")
        traceback.print_exc()
        return None

def format_error_html(message: str) -> str:
    """Formats an error message as an HTML snippet."""
    print(f"Error generating table: {message}")
    return f'<div class="{ERROR_MESSAGE_CLASS}">{message} Check logs.</div>'

def generate_html_table(csv_filepath: str) -> str:
    """Reads CSV, processes data, and generates HTML table string or error HTML."""
    print(f"Generating table from: {csv_filepath}")
    if not os.path.exists(csv_filepath):
         return format_error_html(f"Data file not found: {csv_filepath}")
    try:
        if os.path.getsize(csv_filepath) == 0:
            return format_error_html(f"Data file is empty: {csv_filepath}")

        df = pd.read_csv(csv_filepath)
        if df.empty:
            return format_error_html("Data file read but DataFrame is empty.")

        # Prepare DataFrame for display
        if 'ScrapeTimestampUTC' in df.columns:
             df_display = df.drop(columns=['ScrapeTimestampUTC']).copy()
        else: df_display = df.copy()

        columns_to_display = ['Player', 'Probability (%)', 'Decimal_Odds', 'Round', 'Tournament_URL']
        existing_columns = [col for col in columns_to_display if col in df_display.columns]
        if not existing_columns or 'Player' not in existing_columns:
             return format_error_html("Required columns ('Player', etc.) not found in CSV.")
        df_display = df_display[existing_columns].copy()

        # Rename and Format Columns
        rename_map = {'Probability (%)': 'Probability','Decimal_Odds': 'Odds','Tournament_URL': 'Tournament'}
        df_display.rename(columns={k: v for k, v in rename_map.items() if k in df_display.columns}, inplace=True)

        if 'Tournament' in df_display.columns:
            try:
                 df_display['Tournament'] = df_display['Tournament'].apply(
                     lambda url: url.split('/')[-2].replace('-', ' ').title()
                     if isinstance(url, str) and '/' in url and len(url.split('/')) > 2 else 'Unknown'
                 )
            except Exception: df_display['Tournament'] = 'Unknown' # Handle potential errors

        if 'Probability' in df_display.columns:
            try:
                df_display['Probability'] = pd.to_numeric(df_display['Probability'], errors='coerce')
                if pd.api.types.is_numeric_dtype(df_display['Probability']):
                     df_display.sort_values(by='Probability', ascending=False, inplace=True, na_position='last')
                df_display['Probability'] = df_display['Probability'].map('{:.1f}%'.format, na_action='ignore')
            except Exception as e: print(f"Warning: Formatting Probability failed - {e}")

        if 'Odds' in df_display.columns:
             try:
                df_display['Odds'] = pd.to_numeric(df_display['Odds'], errors='coerce')
                df_display['Odds'] = df_display['Odds'].map('{:.2f}'.format, na_action='ignore')
             except Exception as e: print(f"Warning: Formatting Odds failed - {e}")

        # Generate HTML
        html_table = df_display.to_html(index=False, border=0, classes='dataframe', escape=True, na_rep='-')
        if "<table" not in html_table: return format_error_html("Failed to generate HTML table tags.")

        print("HTML table generated successfully.")
        # Wrap table in the container div structure for replacement
        return f'<div class="table-container">{html_table}</div>'

    except Exception as e:
        print(f"Error processing CSV or generating table: {e}")
        traceback.print_exc()
        return format_error_html(f"Unexpected error during table generation: {type(e).__name__}")


def update_index_html(template_path: str, output_path: str, table_html_content: str):
    """Reads template, injects content into placeholders, writes output."""
    print(f"Updating '{output_path}' using template '{template_path}'...")
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()

        final_content = template_content # Start with original

        # Replace Table Div
        if TABLE_PLACEHOLDER in final_content:
            final_content = final_content.replace(TABLE_PLACEHOLDER, table_html_content, 1)
            print("Table placeholder replaced.")
        else:
            print(f"ERROR: Table placeholder not found in template.")

        # Replace Timestamp Div
        if TIMESTAMP_PLACEHOLDER in final_content:
            update_time = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
            last_updated_html = f'<div class="last-updated">Last updated: {update_time}</div>'
            final_content = final_content.replace(TIMESTAMP_PLACEHOLDER, last_updated_html, 1)
            print("Timestamp placeholder replaced.")
        else:
             print(f"ERROR: Timestamp placeholder not found in template.")

        # Write the result
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(final_content)
        print(f"Successfully wrote updated content to {output_path}")

    except FileNotFoundError:
        print(f"Error: Template file not found at {template_path}")
    except Exception as e:
        print(f"Error updating index.html: {e}")
        traceback.print_exc()


# --- Main Execution Logic ---
if __name__ == "__main__":
    print("Starting page generation process...")

    latest_csv = find_latest_csv(DATA_DIR, CSV_PATTERN)

    if latest_csv:
        table_html_content = generate_html_table(latest_csv)
    else:
        error_message = format_error_html(f"Error: Could not find latest data file ({CSV_PATTERN}) in {DATA_DIR}.")
        # Wrap error message in container div to replace the placeholder structure
        table_html_content = f'<div class="table-container">{error_message}</div>'

    if not isinstance(table_html_content, str): # Should always be string now
        error_message = format_error_html("Internal Error: Failed to generate valid HTML content.")
        table_html_content = f'<div class="table-container">{error_message}</div>'

    update_index_html(TEMPLATE_FILE, OUTPUT_HTML_FILE, table_html_content)

    print("Page generation process complete.")
