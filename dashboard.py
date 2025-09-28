import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import requests # We'll need this to call the new APIs

# --- Configuration ---
# Make sure this is your correct Neon database connection string
DATABASE_URL = 'postgresql://neondb_owner:npg_6WliVj7Ybuaf@ep-winter-waterfall-ad3k32vz-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'

# --- Database Connection ---
engine = create_engine(DATABASE_URL)

# --- Data Fetching Functions ---

def fetch_transactions():
    """
    Fetches transaction data and joins it with wallet profiles.
    """
    try:
        with engine.connect() as connection:
            # This is a SQL LEFT JOIN. It combines the two tables.
            # It takes every transaction and adds the 'profile' column from the wallet_profiles table.
            query = """
            SELECT
                t.hash,
                t.from_address,
                t.to_address,
                t.value_eth,
                t.timestamp,
                wp.profile
            FROM
                transactions t
            LEFT JOIN
                wallet_profiles wp ON t.from_address = wp.from_address
            ORDER BY
                t.timestamp DESC
            LIMIT 100;
            """
            df = pd.read_sql(query, connection)
            # Fill any missing profiles with 'Not Profiled'
            df['profile'] = df['profile'].fillna('Not Profiled')
            return df
    except Exception as e:
        # This will happen if 'wallet_profiles' doesn't exist yet
        st.warning(f"Could not fetch wallet profiles. Running in basic mode. Run ml_profiler.py to generate profiles.")
        # Fallback to fetching just transactions if the join fails
        return pd.read_sql("SELECT * FROM transactions ORDER BY timestamp DESC LIMIT 100", engine)

#hype score function
def fetch_hype_scores():
    """Fetches the last 24 hours of hype scores."""
    try:
        with engine.connect() as connection:
            # Fetch scores from the last 24 hours for BTC
            query = """
            SELECT * FROM hype_scores
            WHERE timestamp >= NOW() - INTERVAL '24 hours' AND symbol = 'BTC'
            ORDER BY timestamp ASC;
            """
            df = pd.read_sql(query, connection)
            return df
    except Exception as e:
        st.warning("Could not fetch hype score data.")
        return pd.DataFrame()






# NEW FUNCTION: Fetches prices from multiple exchanges for arbitrage
def fetch_arbitrage_opportunities():
    """Fetches prices from Binance and KuCoin to find arbitrage opportunities."""
    # A list of popular crypto pairs to check
    pairs = ['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'XRP-USDT']
    all_opportunities = []

    st.write("Fetching live prices from Binance and KuCoin...")

    for pair in pairs:
        try:
            # --- Fetch from Binance ---
            binance_symbol = pair.replace('-', '') # Binance uses 'BTCUSDT' format
            binance_url = f"https://api.binance.com/api/v3/ticker/price?symbol={binance_symbol}"
            binance_res = requests.get(binance_url)
            binance_price = float(binance_res.json()['price'])

            # --- Fetch from KuCoin ---
            # KuCoin uses 'BTC-USDT' format
            kucoin_url = f"https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={pair}"
            kucoin_res = requests.get(kucoin_url)
            kucoin_price = float(kucoin_res.json()['data']['price'])

            # --- Calculate Spread ---
            spread = abs(binance_price - kucoin_price)
            spread_pct = (spread / min(binance_price, kucoin_price)) * 100

            # --- Determine where to Buy and Sell ---
            if binance_price < kucoin_price:
                buy_on = 'Binance'
                sell_on = 'KuCoin'
            else:
                buy_on = 'KuCoin'
                sell_on = 'Binance'

            all_opportunities.append({
                'Pair': pair,
                'Binance Price': binance_price,
                'KuCoin Price': kucoin_price,
                'Spread (%)': spread_pct,
                'Buy On': buy_on,
                'Sell On': sell_on
            })
        except Exception as e:
            # This handles cases where an API might fail or a pair isn't listed
            st.warning(f"Could not fetch data for {pair}. It might not be listed on both exchanges.")
            continue
            
    return pd.DataFrame(all_opportunities)

# --- Streamlit App ---

st.set_page_config(page_title="Crypto Intelligence Hub", layout="wide")

st.title("📈 Crypto Intelligence Hub")

# --- Arbitrage Section ---
st.subheader("🤑 Arbitrage Opportunities")
st.markdown("Find real-time price differences across major exchanges.")

# A button to trigger the arbitrage search
if st.button("Find Arbitrage Now"):
    # When the button is clicked, fetch the data
    arbitrage_df = fetch_arbitrage_opportunities()
    
    if not arbitrage_df.empty:
        # Sort by the highest spread first
        arbitrage_df = arbitrage_df.sort_values(by='Spread (%)', ascending=False)
        
        # Display the formatted table
        st.dataframe(arbitrage_df.style.format({
            'Binance Price': '${:,.2f}',
            'KuCoin Price': '${:,.2f}',
            'Spread (%)': '{:.4f}%'
        }), use_container_width=True)
    else:
        st.warning("Could not fetch any arbitrage data at the moment.")

st.markdown("---") # Visual separator

# --- Sentiment Fusion Chart ---
st.subheader("📈 Social Sentiment (BTC)")
st.markdown("Real-time market sentiment based on X (Twitter) data.")
hype_df = fetch_hype_scores()

if not hype_df.empty:
    # Use st.line_chart to plot the hype score over time
    # We rename the columns for the chart to understand which column is the index (x-axis)
    st.line_chart(hype_df.rename(columns={'timestamp':'index'}).set_index('index'))
else:
    st.info("Sentiment data for the last 24 hours is not yet available. Run the sentiment_analyzer.py script.")

st.markdown("---") # Add another separator for clean layout

# --- Whale Transactions Section ---
st.subheader("🐋 Whale Transactions")
st.markdown("The latest transactions collected from the Ethereum blockchain.")

# Fetch and display the transaction data
df_transactions = fetch_transactions()
if not df_transactions.empty:
    st.dataframe(df_transactions.style.format({"value_eth": "{:.6f}"}), use_container_width=True)
else:
    st.warning("No transaction data found. Please run the `data_collector.py` script.")