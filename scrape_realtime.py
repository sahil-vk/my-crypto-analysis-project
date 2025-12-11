import requests
import pandas as pd
from datetime import datetime
import time
import os
import glob

# correct folder for realtime CSVs
DATA_DIR = "data/realtime"
os.makedirs(DATA_DIR, exist_ok=True)

# correct folder for top_10 file (for historical scraper)
TOP10_DIR = "data/historical"
os.makedirs(TOP10_DIR, exist_ok=True)

# Function to delete old CSV files
def delete_old_csv_files():
    csv_pattern = os.path.join(DATA_DIR, 'crypto_data_*.csv')
    old_files = glob.glob(csv_pattern)
    for file in old_files:
        try:
            os.remove(file)
            print(f"🗑️ Deleted old file: {file}")
        except Exception as e:
            print(f"⚠️ Error deleting {file}: {e}")

delete_old_csv_files()

# API information
url = 'https://api.coingecko.com/api/v3/coins/markets'
param = {
    'vs_currency': 'usd',
    'order': 'market_cap_desc',
    'per_page': 250,
    'page': 1
}

# Request
response = requests.get(url, params=param)

if response.status_code == 200:
    print('Connection Successful! \nGetting the data...')
    
    data = response.json()
    df = pd.DataFrame(data)
    
    df = df[['id', 'symbol', 'current_price', 'market_cap', 'total_volume', 
             'high_24h', 'low_24h', 'price_change_24h', 'price_change_percentage_24h', 'ath', 'atl']]
    
    now = datetime.now()
    df.loc[:, 'date'] = now.strftime('%Y-%m-%d')
    df.loc[:, 'time'] = now.strftime('%H:%M:%S')

    column_order = ['id', 'symbol', 'date', 'time', 'current_price', 'market_cap', 'total_volume', 
                    'high_24h', 'low_24h', 'price_change_24h', 'price_change_percentage_24h', 'ath', 'atl']
    df = df[column_order]

    # SAVE MAIN REALTIME CSV
    filename = f'crypto_data_{now.strftime("%Y-%m-%d_%H-%M-%S")}.csv'
    df.to_csv(os.path.join(DATA_DIR, filename), index=False)
    print(f"Data saved successfully as {filename}!")

    # 🚀 NEW: Generate top_10_coins.txt
    top_10 = df.head(10)['id'].tolist()
    top10_file = os.path.join(TOP10_DIR, "top_10_coins.txt")

    # delete old file if exists
    if os.path.exists(top10_file):
        os.remove(top10_file)

    # write new file
    with open(top10_file, "w") as f:
        for coin in top_10:
            f.write(coin + "\n")

    print("📌 Top 10 coin IDs saved successfully to data/historical/top_10_coins.txt!")

else:
    print(f"Connection Failed! Error Code {response.status_code}")
