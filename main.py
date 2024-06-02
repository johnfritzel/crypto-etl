# Import libraries
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import psycopg2
import logging
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get database credentials from environment variables
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Data extraction function
def extract_data():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto("https://coinmarketcap.com/")

        # Automate scrolling to load more content
        total_height = page.evaluate("document.body.scrollHeight")

        # Divide the height into 10 parts so that it loads the page thoroughly
        part_height = total_height // 10

        # Loop to scroll to each part and wait for it to load
        for i in range(1, 11):
            # Scroll to the current part
            scroll_height = i * part_height
            page.evaluate(f"window.scrollTo(0, {scroll_height})")

            # Wait for the content to load
            page.wait_for_timeout(2000) # Set to 2000 for internet speed consideration

        # Fetch page content after loading
        source = page.content()

    # Parse the HTML with BeautifulSoup
    soup = BeautifulSoup(source, 'html.parser')

    currency_table = soup.find('div', class_='sc-ae0cff98-2 tLNRm')

    # Data storage
    data = {
        'Name': [],
        'Symbol': [],
        'Price': [],
        '1h %': [],
        '24h %': [],
        '7d %': [],
        'Market Cap': [],
        'Volume (24h)': []
    }

    # Extract data from the currency table
    for tr in currency_table.find_all('tr')[1:]:  # Skip the header row
        try:
            name = tr.find('p', class_='sc-71024e3e-0 ehyBa-d').text
            symbol = tr.find('p', class_='sc-71024e3e-0 OqPKt coin-item-symbol').text
            price = tr.find('div', class_='sc-a093f09c-0 gPTgRa').text
            
            percent_changes = tr.find_all('span', class_='sc-4ed47bb1-0')
            percent_1h = percent_changes[0].text if len(percent_changes) > 0 else ''
            percent_24h = percent_changes[1].text if len(percent_changes) > 1 else ''
            percent_7d = percent_changes[2].text if len(percent_changes) > 2 else ''

            market_cap = tr.find('span', class_='sc-baf034bc-1 fTNuyx').text
            volume_24h = tr.find('p', class_='sc-71024e3e-0 bbHOdE font_weight_500').text

            # Append extracted data to the data storage
            data['Name'].append(name)
            data['Symbol'].append(symbol)
            data['Price'].append(price)
            data['1h %'].append(percent_1h)
            data['24h %'].append(percent_24h)
            data['7d %'].append(percent_7d)
            data['Market Cap'].append(market_cap)
            data['Volume (24h)'].append(volume_24h)

        except Exception as e:
            logging.error(f"Error scraping row: {e}")

    # Convert to DataFrame
    df = pd.DataFrame(data)
    return df

# Data cleaning function
def clean_data(df):
    df['Price'] = df['Price'].str.replace('[$,]', '', regex=True).astype(float)
    df['1h %'] = df['1h %'].str.replace('%', '').astype(float) / 100
    df['24h %'] = df['24h %'].str.replace('%', '').astype(float) / 100
    df['7d %'] = df['7d %'].str.replace('%', '').astype(float) / 100
    df['Market Cap'] = df['Market Cap'].str.replace('[$,]', '', regex=True).astype(float)
    df['Volume (24h)'] = df['Volume (24h)'].str.replace('[$,]', '', regex=True).astype(float)
    df.fillna('', inplace=True)
    return df

# Data loading to PostgreSQL function
def load_data(df, conn):
    cur = conn.cursor()

    # Drop existing table if it exists
    cur.execute("DROP TABLE IF EXISTS cryptocurrency")

    # Recreate the table structure
    cur.execute("""
        CREATE TABLE cryptocurrency (
            name VARCHAR(255),
            symbol VARCHAR(20),
            price FLOAT,
            percent_change_1h FLOAT,
            percent_change_24h FLOAT,
            percent_change_7d FLOAT,
            market_cap FLOAT,
            volume_24h FLOAT
        )
    """)

    # Insert data into PostgreSQL table
    for index, row in df.iterrows():
        cur.execute("""
            INSERT INTO cryptocurrency(name, symbol, price, percent_change_1h, percent_change_24h, percent_change_7d, market_cap, volume_24h)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['Name'], row['Symbol'], row['Price'], row['1h %'], row['24h %'], row['7d %'], row['Market Cap'], row['Volume (24h)']))
    
    conn.commit()
    cur.close()


# Main function
def main():
    logging.info("Starting ETL process")

    # Extract data 
    logging.info("Extract data")
    df = extract_data()

    # Clean data
    logging.info("Cleaning data")
    clean_df = clean_data(df)

    # Load data into PostgreSQL
    logging.info("Loading data into PostgreSQL")
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    load_data(clean_df, conn)
    conn.close()
    
    logging.info("ETL process completed successfully")

if __name__ == "__main__":
    main()