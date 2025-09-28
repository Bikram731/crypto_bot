import tweepy
from transformers import pipeline
import time
import pandas as pd
from sqlalchemy import create_engine, text

# --- Configuration ---
# Your key is working, so keep it!
X_BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAJ643wEAAAAAmRpSOGw7jxcyx3N1XupwgxjTHpQ%3DFJlzDrQrlNIS56Hx1Qr1ubW1FydQeAkMqwz3053mdwmsDcUqV8'
DATABASE_URL = 'postgresql://neondb_owner:npg_6WliVj7Ybuaf@ep-winter-waterfall-ad3k32vz-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'

# --- Setup ---
sentiment_pipeline = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
engine = create_engine(DATABASE_URL)
client = tweepy.Client(X_BEARER_TOKEN)

def setup_hype_table():
    try:
        with engine.connect() as connection:
            connection.execute(text("""
            CREATE TABLE IF NOT EXISTS hype_scores (
                timestamp TIMESTPTZ PRIMARY KEY,
                symbol TEXT,
                score INT,
                positive_tweets INT,
                negative_tweets INT
            );
            """))
            connection.commit()
        print("Database table 'hype_scores' is ready.")
    except Exception as e:
        print(f"Error setting up database table: {e}")

def run_sentiment_poll():
    """
    This function will run in a loop, fetching recent tweets every 15 minutes.
    """
    print("Starting sentiment polling... Will fetch tweets every 15 minutes.")
    while True:
        try:
            print(f"[{pd.to_datetime('now', utc=True)}] Fetching recent tweets for #Bitcoin...")
            
            # Use the simple search function
            response = client.search_recent_tweets("#Bitcoin", max_results=100)
            
            if not response.data:
                print("No new tweets found in the last batch.")
                time.sleep(900) # Sleep for 15 minutes (900 seconds)
                continue

            # Process the tweets we found
            tweet_buffer = []
            for tweet in response.data:
                sentiment = sentiment_pipeline(tweet.text)[0]
                score = 1 if sentiment['label'] == 'POSITIVE' else -1
                tweet_buffer.append(score)

            positive_tweets = tweet_buffer.count(1)
            negative_tweets = tweet_buffer.count(-1)
            hype_score = positive_tweets - negative_tweets
            timestamp = pd.to_datetime('now', utc=True)

            print(f"[{timestamp}] Saving batch: {len(tweet_buffer)} tweets. Hype Score: {hype_score}")

            # Save to database
            with engine.connect() as connection:
                query = text("""
                INSERT INTO hype_scores (timestamp, symbol, score, positive_tweets, negative_tweets)
                VALUES (:ts, :sym, :scr, :pos, :neg)
                ON CONFLICT (timestamp) DO NOTHING;
                """)
                connection.execute(query, {'ts': timestamp, 'sym': 'BTC', 'scr': hype_score, 'pos': positive_tweets, 'neg': negative_tweets})
                connection.commit()

        except Exception as e:
            print(f"An error occurred during polling: {e}")
        
        # Wait for 15 minutes before the next poll
        print("Sleeping for 15 minutes...")
        time.sleep(900)

# --- Main execution block ---
if __name__ == "__main__":
    setup_hype_table()
    run_sentiment_poll()