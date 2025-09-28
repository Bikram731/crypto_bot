import yfinance as yf
from datetime import datetime

# Define the cryptocurrency ticker and the start date
ticker = 'BTC-USD'
start_date = '2023-01-01'

# Automatically get today's date as the end date
end_date = datetime.now().strftime('%Y-%m-%d')

print(f"Downloading latest historical data for {ticker} up to {end_date}...")

# Download the data from Yahoo Finance
try:
    data = yf.download(ticker, start=start_date, end=end_date)
    
    if data.empty:
        print("No new data downloaded.")
    else:
        # Save the data to a CSV file, overwriting the old one
        data.to_csv('btc_data.csv')
        print(f"Success! Your data file 'btc_data.csv' is now up to date.")

except Exception as e:
    print(f"An error occurred: {e}")