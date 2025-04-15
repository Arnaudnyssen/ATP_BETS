# ATP_BETS: Automated Tennis Odds Comparison & Strategy Simulation

## Introduction

This project automates the daily process of scraping tennis match probabilities and betting odds, comparing them to identify potential value, simulating betting strategies based on this comparison, tracking results, and presenting the findings on a static website deployed via GitHub Pages.

The primary goal is to explore potential discrepancies between a statistical model  and market odds  and to backtest simple betting strategies based on these discrepancies.

## Features

* **Daily Data Scraping:** Automatically fetches upcoming match probabilities (Tennis Abstract) and corresponding betting odds (Betcenter.be) using Selenium.
* **Results Scraping:** Automatically scrapes completed match results from Tennis Abstract.
* **Data Processing:** Standardizes player and tournament names, merges probability and odds data, calculates implied market probabilities, and computes the spread (difference) between market odds and model-implied odds.
* **Strategy Simulation:** Identifies hypothetical bets based on predefined strategies (e.g., probability difference threshold, max positive spread, fractional Kelly).
* **Performance Tracking:** Logs intended bets and calculates Profit/Loss based on scraped match results. Generates a daily performance summary.
* **Web Interface:** Generates a static HTML page (`index.html`) displaying the odds comparison data and strategy log in separate tabs.
* **Automation:** Orchestrated via GitHub Actions for daily execution.
* **Deployment:** Automatically deploys the generated `index.html` and associated data files to GitHub Pages.

## Technology Stack

* **Language:** Python 3.10+
* **Core Libraries:** Pandas, NumPy
* **Web Scraping:** Selenium, Requests, BeautifulSoup4, webdriver-manager
* **Web Development:** HTML, CSS, JavaScript (for tabs)
* **Automation:** GitHub Actions
* **Deployment:** GitHub Pages

## Setup & Installation

1.  **Prerequisites:**
    * Python 3.10 or higher installed.
    * Git installed.
    * Google Chrome browser installed (required by Selenium and webdriver-manager/chromedriver).
2.  **Clone Repository:**
    ```bash
    git clone <your-repository-url>
    cd ATP_BETS
    ```
3.  **Create Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
4.  **Install Dependencies:**
    ```bash
    pip install --upgrade pip
    pip install -r requirements.txt
    # Install requests and beautifulsoup4 if not added to requirements yet
    pip install requests beautifulsoup4
    ```
5.  **Chromedriver:** `webdriver-manager` (included in `requirements.txt`) should automatically download and manage the correct Chromedriver version. If you encounter issues, ensure Google Chrome is up-to-date or install `chromedriver` manually appropriate for your system and Chrome version.

## Usage

### Automated Workflow (GitHub Actions)

* The primary way to run the project is via the GitHub Actions workflow defined in `.github/workflows/main.yml`.
* **Scheduled Runs:** The workflow is configured to run automatically on a daily schedule (e.g., 06:00 UTC - check the `cron` schedule in the YAML file).
* **Manual Runs:** You can also trigger the workflow manually from the "Actions" tab in your GitHub repository. Select the "Scrape Tennis Data & Update Page" workflow and click "Run workflow".

### Accessing the Output

* The generated comparison website is automatically deployed to GitHub Pages. Access it via the URL provided in your repository's settings under "Pages" (usually `https://<your-username>.github.io/ATP_BETS/`).
* The generated data files (raw scrapes, processed data, results, logs) are committed back to the `data_archive/` directory in the repository.

## Project Structure

ATP_BETS/│├── .github/│   └── workflows/│       └── main.yml           # GitHub Actions workflow definition├── data_archive/              # Stores all generated CSV data files│   ├── sackmann_matchups_.csv│   ├── betcenter_odds_.csv│   ├── processed_comparison_.csv│   ├── match_results_.csv│   ├── strategy_log.csv│   └── daily_results_summary.csv├── .gitignore                 # Files/directories ignored by Git├── generate_page.py           # Generates index.html website├── process_data.py            # Cleans, merges, calculates data├── save_sackmann_data.py      # Orchestrates Sackmann scraping/saving├── tennis_abstract_scraper.py # Scrapes Sackmann probs & results├── betcenter_odds_scraper.py  # Scrapes Betcenter odds├── simulate_strategies.py     # Identifies hypothetical bets├── calculate_results.py       # Calculates P/L based on results├── requirements.txt           # Python package dependencies├── README.md                  # This file└── index.html                 # Generated website (committed by Actions)
## Workflow Overview

1.  **Scrape Sackmann:** `save_sackmann_data.py` calls `p_sack_preproc.py` which uses `tennis_abstract_scraper.py` to fetch probabilities and results. Saves `sackmann_matchups_*.csv` and `match_results_*.csv`.
2.  **Scrape Betcenter:** `betcenter_odds_scraper.py` fetches odds. Saves `betcenter_odds_*.csv`.
3.  **Process Data:** `process_data.py` loads the latest Sackmann and Betcenter files, standardizes names/tournaments, merges them, calculates spreads and implied probabilities. Saves `processed_comparison_*.csv`.
4.  **Simulate Strategies:** `simulate_strategies.py` loads the processed data, identifies bets based on defined rules, and appends them to `strategy_log.csv`.
5.  **Calculate Results:** `calculate_results.py` loads the strategy log and match results, calculates P/L for completed bets, and updates `strategy_log.csv`. It also creates `daily_results_summary.csv`.
6.  **Generate Page:** `generate_page.py` loads the processed comparison data and the strategy log, generating `index.html` with tabs for display.
7.  **Commit & Deploy:** The GitHub Action commits all generated data files and `index.html` back to the repository and deploys the site to GitHub Pages.

## Data Files (`data_archive/`)

* `sackmann_matchups_YYYYMMDD.csv`: Raw(ish) upcoming match data scraped from Tennis Abstract (probabilities, calculated odds).
* `betcenter_odds_YYYYMMDD.csv`: Raw odds data scraped from Betcenter.
* `match_results_YYYYMMDD.csv`: Completed match results scraped from Tennis Abstract.
* `processed_comparison_YYYYMMDD.csv`: Merged, cleaned data combining Sackmann and Betcenter info, including spreads and implied probabilities. Used for display and strategy simulation.
* `strategy_log.csv`: Persistent log of all hypothetical bets identified by the simulation scripts over time, updated with results and P/L when available.
* `daily_results_summary.csv`: Summary statistics (Daily P/L, Cumulative P/L, Number of Bets) per strategy per day.

## Future Enhancements

* Implement graphing of strategy performance (Cumulative P/L) on the website.
* Add contextual data (Surface, Player Ranks, H2H) to the analysis.
* Refine strategy simulation (e.g., proper bankroll management for Kelly).
* Implement more sophisticated strategy triggers (e.g., relative spread).
* Add advanced table interactivity (sorting, filtering, column toggling).
* Track data latency more precisely (separate timestamps for sources).
* Incorporate model uncertainty/calibration analysis.
* Add support for more odds providers.

