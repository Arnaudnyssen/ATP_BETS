# generate_page.py (Corrected Replace Calls)

import pandas as pd
from datetime import datetime
import os
import glob
import pytz
import traceback
from typing import Optional

# --- find_latest_csv function remains the same as in generate_page_py_04 ---
def find_latest_csv(directory: str, pattern: str = "sackmann_data_*.csv") -> Optional[str]:
    """Finds the most recently created CSV file matching the pattern in a directory."""
    try:
        search_path = os.path.join(directory, pattern)
        list_of_files = glob.glob(search_path)
        if not list_of_files:
            print(f"No files found matching pattern '{pattern}' in directory '{directory}'.")
            return None
        try:
             latest_file = max(list_of_files, key=os.path.getctime)
        except AttributeError:
             latest_file = max(list_of_files, key=os.path.getmtime)
        except ValueError:
             print("Error finding latest file: No files match.")
             return None
        print(f"Found latest CSV file: {latest_file}")
        return latest_file
    except Exception as e:
        print(f"Error finding latest CSV file: {e}")
        return None

# --- generate_html_table function remains the same as in generate_page_py_04 ---
def generate_html_table(csv_filepath: str) -> str:
    """Reads the CSV and generates an HTML table string with robust error handling."""
    error_prefix = "<p><strong>Error generating table:</strong> "
    error_suffix = " Check workflow logs for details.</p>"
    if csv_filepath is None or not os.path.exists(csv_filepath):
         print("Error: CSV filepath is missing or file does not exist.")
         return f"{error_prefix}Data file not found.{error_suffix}"
    try:
        if os.path.getsize(csv_filepath) == 0:
            print(f"Error: CSV file at {csv_filepath} is empty.")
            return "<p>No match data available (empty file).</p>"
        df = pd.read_csv(csv_filepath)
        if df.empty:
            print(f"Warning: DataFrame is empty after reading {csv_filepath}.")
            return "<p>No match data available (DataFrame empty).</p>"
        print("Preparing data for HTML table...")
        if 'ScrapeTimestampUTC' in df.columns:
             df_display = df.drop(columns=['ScrapeTimestampUTC']).copy()
        else: df_display = df.copy()
        columns_to_display = ['Player', 'Probability (%)', 'Decimal_Odds', 'Round', 'Tournament_URL']
        existing_columns = [col for col in columns_to_display if col in df_display.columns]
        if not existing_columns:
             print("Error: No relevant columns found in DataFrame.")
             return f"{error_prefix}No relevant columns found.{error_suffix}"
        if 'Player' not in existing_columns:
             print("Error: 'Player' column missing.")
             return f"{error_prefix}'Player' column missing.{error_suffix}"
        df_display = df_display[existing_columns].copy()
        print(f"Selected columns for display: {existing_columns}")
        rename_map = {'Probability (%)': 'Probability','Decimal_Odds': 'Odds','Tournament_URL': 'Tournament'}
        rename_map_existing = {k: v for k, v in rename_map.items() if k in df_display.columns}
        df_display.rename(columns=rename_map_existing, inplace=True)
        print(f"Renamed columns: {rename_map_existing}")
        if 'Tournament' in df_display.columns:
            try:
                 df_display['Tournament'] = df_display['Tournament'].apply(
                     lambda url: url.split('/')[-2].replace('-', ' ').title() if isinstance(url, str) and '/' in url else 'Unknown')
            except Exception as e:
                 print(f"Warning: Could not process Tournament URLs cleanly - {e}")
                 df_display['Tournament'] = 'Unknown'
        if 'Probability' in df_display.columns:
            try:
                df_display['Probability'] = pd.to_numeric(df_display['Probability'], errors='coerce')
                if pd.api.types.is_numeric_dtype(df_display['Probability']) and not df_display['Probability'].isna().all():
                     df_display.sort_values(by='Probability', ascending=False, inplace=True, na_position='last')
                df_display['Probability'] = df_display['Probability'].map('{:.1f}%'.format, na_action='ignore')
            except Exception as e: print(f"Warning: Could not sort/format Probability column - {e}")
        if 'Odds' in df_display.columns:
             try:
                df_display['Odds'] = pd.to_numeric(df_display['Odds'], errors='coerce')
                df_display['Odds'] = df_display['Odds'].map('{:.2f}'.format, na_action='ignore')
             except Exception as e: print(f"Warning: Could not format Odds column - {e}")
        print("Generating HTML string...")
        try:
            html_table = df_display.to_html(index=False, border=0, classes='dataframe', escape=True, na_rep='-')
            if "<table" in html_table and "</table>" in html_table:
                 html_table = f'<table class="dataframe">{html_table.split("<table")[1].split("</table>")[0]}</table>'
            else:
                 print("Warning: pandas to_html did not return expected table tags.")
                 return "<p>Could not format data into a table.</p>"
            print("HTML table generated successfully.")
            return html_table
        except Exception as e_html:
             print(f"Error during pandas to_html conversion: {e_html}")
             traceback.print_exc()
             return f"{error_prefix}Failed during HTML conversion ({type(e_html).__name__}).{error_suffix}"
    except pd.errors.EmptyDataError:
        print(f"Error: CSV file at {csv_filepath} is empty or unreadable.")
        return "<p>No match data available (file empty or unreadable).</p>"
    except Exception as e:
        print(f"Error processing CSV or generating HTML table: {e}")
        traceback.print_exc()
        return f"{error_prefix}{type(e).__name__} occurred.{error_suffix}"

# --- update_index_html function MODIFIED ---
def update_index_html(template_path: str, output_path: str, table_html: str):
    """Reads the template, injects the table and timestamp, and writes the output HTML."""
    print(f"Updating {output_path}...")
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()

        update_time = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
        last_updated_text = f"Last updated: {update_time}"

        # *** CORRECTED .replace() CALLS ***
        content_with_table = template_content.replace('', table_html)
        final_content = content_with_table.replace('', last_updated_text)
        # *** END CORRECTION ***

        # Check if replacements happened (useful for debugging)
        if '' in final_content:
            print("ERROR: Odds table placeholder WAS NOT REPLACED in final content!")
        else:
            print("Odds table placeholder replaced.")

        if '' in final_content:
             print("ERROR: Last updated placeholder WAS NOT REPLACED in final content!")
        else:
             print("Last updated placeholder replaced.")

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(final_content)
        print(f"Successfully wrote updated content to {output_path}")

    except FileNotFoundError:
        print(f"Error: Template file not found at {template_path}")
    except Exception as e:
        print(f"Error updating index.html: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    DATA_DIR = "data_archive"
    TEMPLATE_FILE = "index.html"
    OUTPUT_HTML_FILE = "index.html"

    print("Finding latest data file...")
    latest_csv = find_latest_csv(DATA_DIR)

    table_html_content = "<p><strong>Error: Could not find the latest data file to generate the table. Check workflow logs.</strong></p>" # Updated default message

    if latest_csv:
        print("Generating HTML table from latest CSV...")
        table_html_content = generate_html_table(latest_csv)
    else:
        # Explicitly log that we are using the error message because no CSV was found
        print(f"No CSV file found in {DATA_DIR}. Using default error message for HTML content.")

    print(f"Updating {OUTPUT_HTML_FILE}...")
    update_index_html(TEMPLATE_FILE, OUTPUT_HTML_FILE, table_html_content)

    print("Page generation process complete.")

