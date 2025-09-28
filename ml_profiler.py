import pandas as pd
from sqlalchemy import create_engine
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import warnings

# Suppress warnings from KMeans for cleaner output
warnings.filterwarnings('ignore', category=FutureWarning)

# --- Configuration ---
DATABASE_URL = 'postgresql://neondb_owner:npg_6WliVj7Ybuaf@ep-winter-waterfall-ad3k32vz-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
engine = create_engine(DATABASE_URL)

def profile_wallets():
    """Fetches data, engineers features, clusters wallets, and saves profiles."""
    print("Starting wallet profiling process...")
    
    try:
        # 1. Fetch data from the database
        print("Fetching transactions from database...")
        df = pd.read_sql("SELECT * FROM transactions", engine)
        
        if df.empty or len(df) < 10: # Need enough data to profile
            print("Not enough transaction data to create profiles.")
            return

        # 2. Feature Engineering: Create behavioral stats for each wallet
        print("Engineering features for each wallet...")
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        wallet_features = df.groupby('from_address').agg(
            total_transactions=('hash', 'count'),
            avg_eth_value=('value_eth', 'mean'),
            wallet_age_days=('timestamp', lambda x: (x.max() - x.min()).days)
        ).reset_index()

        wallet_features['wallet_age_days'] = wallet_features['wallet_age_days'].fillna(0)

        # 3. Prepare data for clustering
        features_for_clustering = wallet_features[['total_transactions', 'avg_eth_value', 'wallet_age_days']]
        
        # Scale features so that no single feature dominates the clustering
        scaler = StandardScaler()
        scaled_features = scaler.fit_transform(features_for_clustering)

        # 4. Apply K-Means Clustering
        print("Running K-Means clustering...")
        # We'll create 4 clusters (you can experiment with this number)
        kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
        wallet_features['cluster'] = kmeans.fit_predict(scaled_features)

        # 5. Assign meaningful profile names to the clusters
        # NOTE: These names are interpretations. You might analyze the clusters to find better names.
        # For example, find the cluster with the highest avg_eth_value, etc.
        profile_map = {
            0: 'High-Frequency Trader',
            1: 'Occasional User',
            2: 'Old Hodler',
            3: 'New Whale'
        }
        wallet_features['profile'] = wallet_features['cluster'].map(profile_map).fillna('General')

        # 6. Save the profiles to a new database table
        print("Saving profiles to 'wallet_profiles' table...")
        wallet_features[['from_address', 'profile']].to_sql('wallet_profiles', engine, if_exists='replace', index=False)
        
        print("\nWallet profiling complete!")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    profile_wallets()