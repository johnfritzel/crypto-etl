# Cryptocurrency Data Pipeline

This is an Extract, Transform, and Load (ETL) pipeline to gather cryptocurrency data from CoinMarketCap via web scraping. The clean data is then stored in a PostgreSQL database, readily available for analysis and visualization through an interactive dashboard.

&nbsp;
## Functionality
- Extracts data: Uses Playwright to launch a browser, navigate to CoinMarketCap, and scrape relevant data (name, symbol, price, percentage changes, market cap, volume).
- Transforms data: Cleans the extracted data by removing currency symbols, converting values to appropriate data types (float for prices and volumes, percentages divided by 100), and handling missing values.
- Loads data: Establishes a connection to a PostgreSQL database using credentials stored securely in a .env file. Drops existing table (if present) and creates a new table cryptocurrency with appropriate columns. Inserts the cleaned data into the database table.

&nbsp;
## Libraries
The script utilizes the following libraries:

- pandas: Data manipulation and DataFrame creation.
- BeautifulSoup: HTML content parsing.
- playwright: Browser automation for interacting with CoinMarketCap.
- psycopg2: Connection and interaction with PostgreSQL database.
- logging: Recording informational and error messages.
- dotenv: Loading environment variables from a .env file (securely stores database credentials).
- os: Operating system interaction (accessing environment variables).

&nbsp;
## Prerequisites
- Python 3.12.2

&nbsp;
## Installation
1. Clone this repository.
```
git clone https://github.com/johnfritzel/crypto-etl.git
```

2. Create a virtual environment.
```
python -m venv venv
```

2. Activate the virtual environment.
```
venv\Scripts\activate # Windows
source venv/bin/activate  # Linux/macOS
```

4. Install required dependencies.
```
pip install -r requirements.txt
```

5. Run the script:
```
python main.py
```  
