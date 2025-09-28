import requests
import json
import pandas as pd  # NEW: Import pandas for data manipulation
from sqlalchemy import create_engine, text  # NEW: Import SQLAlchemy to talk to the database
from datetime import datetime  # NEW: To handle timestamps

# --- Configuration ---
ETHERSCAN_API_KEY = 'HG5V7TT2Q1ZNK7UZYV6XU27Y2DD36R2I55'
ETHERSCAN_API_URL = 'https://api.etherscan.io/api'

# NEW: Add your database connection string from Neon here
# Make sure it's inside quotes
DATABASE_URL = 'postgresql://neondb_owner:npg_6WliVj7Ybuaf@ep-winter-waterfall-ad3k32vz-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'

# --- NEW: Database Setup ---
# Create a database engine. This is the main entry point to our database.
engine = create_engine(DATABASE_URL)

def setup_database():
    """Create the transactions table if it doesn't exist."""
    print("Setting up database...")
    try:
        with engine.connect() as connection:
            # We use a TEXT data type for hash, from_address, and to_address because they are strings.
            # We use NUMERIC for the value_eth as it's a decimal number.
            # We use TIMESTAMPTZ for the timestamp to include time zone information.
            # "hash" is the primary key, meaning each transaction hash must be unique.
            create_table_query = """
            CREATE TABLE IF NOT EXISTS transactions (
                hash TEXT PRIMARY KEY,
                from_address TEXT,
                to_address TEXT,
                value_eth NUMERIC,
                timestamp TIMESTAMPTZ
            );
            """
            # The text() function is used to execute a raw SQL string safely.
            connection.execute(text(create_table_query))
            # The connection.commit() is needed to save the changes (like creating a table).
            connection.commit()
        print("Database table 'transactions' is ready.")
    except Exception as e:
        print(f"An error occurred during database setup: {e}")


# --- Function to Get Whale Transactions from Etherscan ---
def get_whale_transactions():
    """Fetches the most recent large Ethereum transactions."""
    print("Fetching whale transactions from Etherscan...")
    params = {
        'module': 'account',
        'action': 'txlist',
        'address': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', # WETH Contract Address
        'page': 1,
        'offset': 50,  # Let's get more transactions
        'sort': 'desc',
        'apikey': ETHERSCAN_API_KEY
    }
    try:
        response = requests.get(ETHERSCAN_API_URL, params=params)
        response.raise_for_status()
        data = response.json()
        return data['result']
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Etherscan: {e}")
        return None

# --- NEW: Function to Process and Save Data ---
def process_and_save_data(transactions):
    """Cleans the transaction data and saves it to the database."""
    if not transactions:
        print("No transactions to process.")
        return

    print(f"Processing {len(transactions)} transactions...")

    # Use pandas to create a DataFrame (a table) from our list of transactions
    df = pd.DataFrame(transactions)

    # --- Data Cleaning and Transformation ---
    # 1. Select only the columns we care about
    df = df[['hash', 'from', 'to', 'value', 'timeStamp']]

    # 2. Rename columns for clarity ('from' is a reserved word in SQL)
    df.rename(columns={'from': 'from_address', 'to': 'to_address'}, inplace=True)

    # 3. Convert 'value' from Wei (string) to Ether (numeric)
    # pd.to_numeric handles potential errors by turning them into 'NaN' (Not a Number)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df['value_eth'] = df['value'] / 1e18 # 1 Ether = 10^18 Wei

    # 4. Convert Unix timestamp (string) to a proper datetime object
    df['timestamp'] = pd.to_datetime(df['timeStamp'], unit='s')
    
    # 5. Drop the original, unconverted columns
    df = df.drop(columns=['value', 'timeStamp'])
    
    # Remove any rows that had conversion errors
    df.dropna(inplace=True)

    # --- Save to Database ---
    try:
        # The .to_sql() function is a powerful feature of pandas.
        # 'transactions': The name of the table in our database.
        # engine: The database connection we created earlier.
        # if_exists='append': If the table exists, add the new data.
        # index=False: We don't want to save the pandas DataFrame index as a column.
        # We need a way to handle duplicates. We'll use a trick.
        # First, we get all the hashes that are already in the database.
        with engine.connect() as connection:
            existing_hashes = pd.read_sql("SELECT hash FROM transactions", connection)['hash'].tolist()
        
        # Then, we filter our DataFrame to only include transactions with new hashes.
        new_transactions_df = df[~df['hash'].isin(existing_hashes)]

        if not new_transactions_df.empty:
            new_transactions_df.to_sql('transactions', engine, if_exists='append', index=False)
            print(f"Successfully saved {len(new_transactions_df)} new transactions to the database.")
        else:
            print("No new transactions to save.")

    except Exception as e:
        print(f"An error occurred while saving to the database: {e}")

# --- Main part of the script ---
if __name__ == "__main__":
    # 1. Set up the database and table
    setup_database()
    
    # 2. Get the raw data from the API
    raw_transactions = get_whale_transactions()
    
    # 3. Process the raw data and save the clean version to our database
    process_and_save_data(raw_transactions)