import os
import glob
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import random

# Define the correct directory for historical data
historical_data_dir = "data/historical"

# Function to delete old CSV files from the correct directory
def delete_old_csv_files():
    csv_pattern = os.path.join(historical_data_dir, 'top_10_crypto_365days_data_*.csv')
    old_files = glob.glob(csv_pattern)
    for file in old_files:
        try:
            os.remove(file)
            print(f"🗑️ Deleted old file: {file}")
        except Exception as e:
            print(f"⚠️ Error deleting {file}: {e}")

delete_old_csv_files()  # Call function to delete old CSV files

# Read top 10 coins dynamically from the file
with open("data/historical/top_10_coins.txt", "r") as f:

    top_10_coins = [line.strip() for line in f.readlines()]

currency = 'usd'
end_date = datetime.now()
start_date = end_date - timedelta(days=364)

start_timestamp = int(time.mktime(start_date.timetuple()))
end_timestamp = int(time.mktime(end_date.timetuple()))

# Function to fetch historical data including ATH & ATL with retry mechanism
def fetch_coin_data(coin_id, start_timestamp, end_timestamp, currency='usd', max_retries=5):
    print(f"📢 Fetching historical data (with ATH & ATL) for {coin_id}...")
    
    url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range'
    params = {'vs_currency': currency, 'from': start_timestamp, 'to': end_timestamp}
    
    retry_count = 0
    wait_time = 10  # Initial wait time in seconds
    
    while retry_count < max_retries:
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            prices = data.get('prices', [])
            market_caps = data.get('market_caps', [])
            total_volumes = data.get('total_volumes', [])
            
            if prices:
                df_prices = pd.DataFrame(prices, columns=['timestamp', 'price'])
                df_market_caps = pd.DataFrame(market_caps, columns=['timestamp', 'market_cap'])
                df_total_volumes = pd.DataFrame(total_volumes, columns=['timestamp', 'total_volume'])
                
                df = df_prices.merge(df_market_caps, on='timestamp').merge(df_total_volumes, on='timestamp')
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df['date'] = df['timestamp'].dt.date
                
                # Compute OHLC
                ohlc = df.groupby('date')['price'].agg(open='first', high='max', low='min', close='last').reset_index()
                df = df.merge(ohlc, on='date', how='left').drop(columns=['date'])
                
                # Fetch ATH & ATL within the same request
                url_coin_info = f'https://api.coingecko.com/api/v3/coins/{coin_id}'
                response_coin = requests.get(url_coin_info)
                
                if response_coin.status_code == 200:
                    coin_data = response_coin.json()
                    ath = coin_data.get('market_data', {}).get('ath', {}).get(currency, None)
                    atl = coin_data.get('market_data', {}).get('atl', {}).get(currency, None)
                else:
                    print(f"⚠️ Failed to fetch ATH & ATL for {coin_id}. Error: {response_coin.status_code}")
                    ath, atl = None, None
                
                # Add ATH & ATL columns
                df['ath'] = ath
                df['atl'] = atl
                
                # Insert coin ID column at the beginning
                df.insert(0, 'id', coin_id)
                
                print(f"✅ Data for {coin_id} fetched successfully!\n")
                return df
            
            else:
                print(f"⚠️ No historical data found for {coin_id}\n")
                return None
        
        elif response.status_code == 429:  # Too Many Requests
            print(f"⏳ Rate limit hit for {coin_id}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            wait_time *= 2  # Exponential backoff
            retry_count += 1
        else:
            print(f"❌ Failed to fetch data for {coin_id}. Error: {response.status_code}\n")
            return None
    
    print(f"🚨 Max retries reached for {coin_id}. Skipping...\n")
    return None

# Initialize merged DataFrame
merged_df = pd.DataFrame()
failed_coins = []

# Fetch historical data with ATH & ATL
for coin in top_10_coins:
    data = fetch_coin_data(coin, start_timestamp, end_timestamp, currency)
    if data is not None:
        merged_df = pd.concat([merged_df, data], ignore_index=True)
    else:
        failed_coins.append(coin)
    
    time.sleep(1 + random.random())
  # ✅ Wait time reduced to 30 seconds

# Retry failed coins with backoff
if failed_coins:
    print(f"\n🔄 Retrying failed coins with backoff: {failed_coins}...\n")
    for coin in failed_coins:
        data = fetch_coin_data(coin, start_timestamp, end_timestamp, currency)
        if data is not None:
            merged_df = pd.concat([merged_df, data], ignore_index=True)
        time.sleep(1 + random.random())
  # ✅ Retry wait time also set to 30 seconds

# Define column order
column_order = ['id', 'timestamp', 'price', 'market_cap', 'total_volume', 'ath', 'atl', 'open', 'high', 'low', 'close']
merged_df = merged_df[column_order]  # Reorder columns

# Ensure the directory exists before saving
if not os.path.exists(historical_data_dir):
    os.makedirs(historical_data_dir)

# Save final dataset in the correct directory
csv_filename = os.path.join(historical_data_dir, f'top_10_crypto_365days_data_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv')
merged_df.to_csv(csv_filename, index=False)

print(f"\n🎉 All data (historical + ATH + ATL) saved successfully to {csv_filename}!\n")
