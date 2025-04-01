# generate_page.py

import pandas as pd
from datetime import datetime
import os
import pytz # For timezone handling

def generate_html_table(csv_filepath: str) -> str:
    """Reads the CSV and generates an HTML table string."""
    try:
        df = pd.read_csv(csv_filepath)
        if df.empty:
            return "<p>No match data available.</p>"

        # --- Data Preparation ---
        # Select and rename columns for display
        columns_to_display = ['Player', 'Probability (%)', 'Decimal_Odds', 'Round', 'Tournament_URL']
        # Ensure all expected columns exist, handle missing ones gracefully
        existing_columns = [col for col in columns_to_display if col in df.columns]
        if not existing_columns:
             return "<p>Error: No relevant columns found in the data file.</p>"
        if 'Player' not in existing_columns:
             return "<p>Error: 'Player' column is missing.</p>"

        df_display = df[existing_columns].copy()

        # Rename for better readability in the table header
        rename_map = {
            'Probability (%)': 'Probability',
            'Decimal_Odds': 'Odds',
            'Tournament_URL': 'Tournament'
        }
        df_display.rename(columns=rename_map, inplace=True)

        # Optional: Clean up Tournament URL for display (e.g., extract name)
        if 'Tournament' in df_display.columns:
            try:
                 # Example: Extract part of the URL path as a name
                 df_display['Tournament'] = df_display['Tournament'].apply(
                     lambda url: url.split('/')[-2].replace('-', ' ').title() if isinstance(url, str) and '/' in url else 'Unknown'
                 )
            except Exception as e:
                 print(f"Warning: Could not process Tournament URLs cleanly - {e}")
                 df_display['Tournament'] = 'Unknown' # Fallback


        # Sort by Probability (descending)
        if 'Probability' in df_display.columns:
            # Ensure probability is numeric before sorting
            df_display['Probability'] = pd.to_numeric(df_display['Probability'], errors='coerce')
            df_display.sort_values(by='Probability', ascending=False, inplace=True, na_position='last')
            # Format probability and odds nicely
            df_display['Probability'] = df_display['Probability'].map('{:.1f}%'.format, na_action='ignore')
        if 'Odds' in df_display.columns:
            df_display['Odds'] = pd.to_numeric(df_display['Odds'], errors='coerce')
            df_display['Odds'] = df_display['Odds'].map('{:.2f}'.format, na_action='ignore')


        # --- HTML Generation ---
        # Convert DataFrame to HTML table
        html_table = df_display.to_html(
            index=False,            # Don't include DataFrame index
            border=0,               # We use CSS borders
            classes='dataframe',    # Add a CSS class for styling
            escape=True,            # Escape HTML characters in data
            na_rep='-'              # Representation for missing values
        )
        # Wrap the generated table for better styling control if needed
        html_table = f'<table class="dataframe">{html_table.split("<table border")[1].split("</table>")[0]}</table>'


        return html_table

    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_filepath}")
        return "<p>Error: Could not load match data file.</p>"
    except pd.errors.EmptyDataError:
        print(f"Error: CSV file at {csv_filepath} is empty.")
        return "<p>No match data available (empty file).</p>"
    except Exception as e:
        print(f"Error generating HTML table: {e}")
        import traceback
        traceback.print_exc()
        return f"<p>Error generating table: {e}</p>"

def update_index_html(template_path: str, output_path: str, table_html: str):
    """Reads the template, injects the table and timestamp, and writes the output HTML."""
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()

        # Get current timestamp in UTC for consistency
        update_time = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
        last_updated_text = f"Last updated: {update_time}"

        # Replace placeholders using the exact comment strings
        output_content = template_content.replace('', table_html)
        output_content = output_content.replace('', last_updated_text)

        # Ensure placeholders were found and replaced
        if '' in output_content:
            print("Warning: Odds table placeholder not found or replaced in template.")
        if '' in output_content:
             print("Warning: Last updated placeholder not found or replaced in template.")


        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(output_content)
        print(f"Successfully updated {output_path}")

    except FileNotFoundError:
        print(f"Error: Template file not found at {template_path}")
    except Exception as e:
        print(f"Error updating index.html: {e}")

if __name__ == "__main__":
    CSV_FILE = "sackmann_data.csv"        # Input CSV file (expected in root)
    TEMPLATE_FILE = "index.html"          # Input HTML template (expected in root)
    OUTPUT_HTML_FILE = "index.html"       # Output HTML file (overwrite the template)

    # Basic check if CSV exists before proceeding
    if not os.path.exists(CSV_FILE):
        print(f"Error: Input file '{CSV_FILE}' not found. Please run the scraper first.")
    else:
        print("Generating HTML table from CSV...")
        odds_table = generate_html_table(CSV_FILE)

        print(f"Updating {OUTPUT_HTML_FILE}...")
        update_index_html(TEMPLATE_FILE, OUTPUT_HTML_FILE, odds_table)

        print("Page generation complete.")
