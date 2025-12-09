import requests
import json
import os
from dotenv import load_dotenv

# ---------------------------
# LOAD CONFIGURATION
# ---------------------------
load_dotenv()  # load variables from .env

API_KEY = os.getenv("API_KEY")

if not API_KEY:
    raise ValueError("Please set your CoinMarketCap API key.")


# The API endpoint for the latest quotes
url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"

# The headers required for API authentication
headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': API_KEY,
}

# The parameters to specify the cryptocurrencies you want. 
# You can use 'symbol' or 'id'
# We will use symbols for this exercise
parameters = {
    'symbol': 'BTC,ETH,SOL' 
}


# --- Step 1 & 2: Make the API call and parse the JSON response ---
try:
    response = requests.get(url, headers=headers, params=parameters)
    response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
    data = response.json()
    
    # Check for API-level errors
    if data['status']['error_code'] != 0:
        print(f"API Error: {data['status']['error_message']}")
        exit()

except requests.exceptions.RequestException as e:
    print(f"An error occurred with the API request: {e}")
    exit()

except json.JSONDecodeError:
    print("Failed to decode JSON from the response. The API might have returned a non-JSON error.")
    exit()



# --- Step 3: Extract and store the relevant data ---
crypto_data = []
for symbol in parameters['symbol'].split(','):
    # The 'data' key holds a dictionary where keys are the symbols
    # We access the 'quote' dictionary to get the USD data
    # We then use a .get() method to handle potential missing keys gracefully
    crypto_info = data['data'].get(symbol)
    if crypto_info:
        name = crypto_info['name']
        price = crypto_info['quote']['USD']['price']
        percent_change_24h = crypto_info['quote']['USD']['percent_change_24h']
        
        crypto_data.append({
            'name': name,
            'price_usd': price,
            'percent_change_24h': percent_change_24h
        })


# --- Step 4: Analyze the data to find the biggest gainer ---
# Use a lambda function to find the max based on a specific key
if crypto_data:
    highest_gainer = max(crypto_data, key=lambda x: x['percent_change_24h'])
else:
    highest_gainer = None

# --- Step 5: Save the data to a file ---
file_path = "crypto_summary.txt"

try:
    with open(file_path, "w") as file:
        file.write("--- Cryptocurrency Data Summary ---\n\n")
        
        for item in crypto_data:
            file.write(f"Name: {item['name']}\n")
            file.write(f"Price (USD): ${item['price_usd']:.2f}\n")
            file.write(f"24h Change: {item['percent_change_24h']:.2f}%\n")
            file.write("-" * 25 + "\n")
            
        if highest_gainer:
            file.write(f"\nToday's Biggest Gainer: {highest_gainer['name']} with a {highest_gainer['percent_change_24h']:.2f}% increase.\n")
        else:
            file.write("\nNo data available to determine the biggest gainer.\n")
            
    print(f"Successfully saved cryptocurrency data to {file_path}")

except IOError as e:
    print(f"An error occurred while writing to the file: {e}")
    