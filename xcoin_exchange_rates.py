import requests
import boto3
import pandas as pd
from datetime import datetime, timedelta

# Base URL for historical data
base_url = "https://rest.coinapi.io/v1/exchangerate"

# Cryptocurrencies to fetch
assets = ["BTC", "ETH", "ADA", "SOL"]
base_currency = "GBP"  # Target currency

# Calculate yesterday's date
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%dT00:00:00")

# Query parameters for historical data
query_params = {
    "period_id": "7DAY",  # Weekly data
    "time_start": "2019-01-01T00:00:00",  # Start date
    "time_end": yesterday_str,  # End date
    "limit": 100000  # Maximum data points
}

# Your API key
api_key = ''

# Headers
headers = {
    'Accept': 'application/json',
    'X-CoinAPI-Key': api_key
}

# Dictionary to store results
historical_data = {}

# Fetch historical data for each asset
for asset in assets:
    url = f"{base_url}/{asset}/{base_currency}/history"
    response = requests.get(url, headers=headers, params=query_params)

    if response.status_code == 200:
        data = response.json()
        historical_data[asset] = {
            entry["time_period_start"][:10]: entry["rate_close"]
            for entry in data
        }  # Extract date and closing rate
    else:
        print(f"Failed to fetch historical data for {asset}: {response.status_code} - {response.text}")

# Create a pandas DataFrame
dates = list(historical_data[assets[0]].keys())  # Get dates from the first asset
df_data = {
    "date": pd.to_datetime(dates),  # Convert dates to datetime
    "BTC_to_GBP_rate": [historical_data["BTC"].get(date, None) for date in dates],
    "ETH_to_GBP_rate": [historical_data["ETH"].get(date, None) for date in dates],
    "ADA_to_GBP_rate": [historical_data["ADA"].get(date, None) for date in dates],
    "SOL_to_GBP_rate": [historical_data["SOL"].get(date, None) for date in dates]
}

df = pd.DataFrame(df_data)

# Save the DataFrame as CSV and Parquet
csv_file = "exchange_rates.csv"
parquet_file = "exchange_rates.parquet"

# Save DataFrame to CSV
df.to_csv(csv_file, index=False)
print(f"DataFrame saved as CSV file: {csv_file}")

# Save DataFrame to Parquet
df.to_parquet(parquet_file, index=False, engine="pyarrow")
print(f"DataFrame saved as Parquet file: {parquet_file}")

# S3 Configuration
s3_bucket = "xcoinexchangerate"
s3_csv_key = "data/exchange_rates.csv"  # Path in the bucket
s3_parquet_key = "data/exchange_rates.parquet"

# AWS Credentials (Make sure to configure your AWS credentials securely)
aws_access_key_id = ""
aws_secret_access_key = ""
aws_region = "eu-west-1"  # Example: 'us-east-1'

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region,
)

# Upload CSV to S3
s3_client.upload_file(csv_file, s3_bucket, s3_csv_key)
print(f"CSV file uploaded to S3: s3://{s3_bucket}/{s3_csv_key}")

# Upload Parquet to S3
s3_client.upload_file(parquet_file, s3_bucket, s3_parquet_key)
print(f"Parquet file uploaded to S3: s3://{s3_bucket}/{s3_parquet_key}")