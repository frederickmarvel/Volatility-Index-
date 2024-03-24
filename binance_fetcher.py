import pandas as pd
import numpy as np
import requests
import schedule
import time
import pymysql
from pymysql.err import MySQLError
import os
from dotenv import load_dotenv
from fastapi import FastAPI
load_dotenv()
app = FastAPI()
# Database connection parameters
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

binance_api_key = os.getenv('API_KEY')  # Ensure this matches your .env file
binance_secret_key = os.getenv('SECRET_KEY')  # Ensure this matches your .env file
max_server_timestamp = 0
base_endpoint = "https://fapi.binance.com"

def fetch_and_update_btc():
    global base_endpoint
    global max_server_timestamp
    try:
        response = requests.get(f"{base_endpoint}/fapi/v1/marketKlines?symbol=iBTCBVOLUSDT&interval=1d&limit=100")
        response_json = response.json()
        
        # Check if the response contains an error
        if 'code' in response_json and 'msg' in response_json:
            print(f"Error from API: {response_json['msg']}")
            return
        
        # Assuming the actual data is under the 'data' key
        # Replace 'data' with the correct key based on your inspection
        data = response_json['data'] if 'data' in response_json else response_json
        
        if not isinstance(data, list):
            print("Unexpected data format received from API.")
            return

    except Exception as e:
        print(f"Request failed: {e}")
        return

    data.reverse()  # Reorder the array to ascending

    # Filter data
    new_data = [d for d in data if int(d[0]) > max_server_timestamp]

    if not new_data:
        return
    try:
        conn = pymysql.connect(host=db_host, user=db_user, password=db_password, db=db_name)
        cursor = conn.cursor()
        
        # Prepare bulk insert query
        insert_query = "INSERT INTO btc_volatility_index (btc_volatility_index, server_time) VALUES (%s, %s)"
        values = [(d[4], d[0]/1000) for d in new_data]

        # Execute bulk insert
        cursor.executemany(insert_query, values)
        conn.commit()

        # Update max_server_time in memory
        max_server_timestamp = int(new_data[-1][0])

        cursor.close()
        conn.close()
    except MySQLError as e:
        print(f"Database error: {e}")

def fetch_and_update_eth():
    global base_endpoint
    global max_server_timestamp
    try:
        response = requests.get(f"{base_endpoint}/fapi/v1/marketKlines?symbol=iETHBVOLUSDT&interval=1d&limit=100")
        response_json = response.json()
        
        # Check if the response contains an error
        if 'code' in response_json and 'msg' in response_json:
            print(f"Error from API: {response_json['msg']}")
            return
        
        # Assuming the actual data is under the 'data' key
        # Replace 'data' with the correct key based on your inspection
        data = response_json['data'] if 'data' in response_json else response_json
        
        if not isinstance(data, list):
            print("Unexpected data format received from API.")
            return

    except Exception as e:
        print(f"Request failed: {e}")
        return

    data.reverse()  # Reorder the array to ascending

    # Filter data
    new_data = [d for d in data if int(d[0]) > max_server_timestamp]

    if not new_data:
        return
    try:
        conn = pymysql.connect(host=db_host, user=db_user, password=db_password, db=db_name)
        cursor = conn.cursor()
        
        # Prepare bulk insert query
        insert_query = "INSERT INTO eth_volatility_index (eth_volatility_index, server_time) VALUES (%s, %s)"
        values = [(d[4], d[0]/1000) for d in new_data]

        # Execute bulk insert
        cursor.executemany(insert_query, values)
        conn.commit()

        # Update max_server_time in memory
        max_server_timestamp = int(new_data[-1][0])

        cursor.close()
        conn.close()
    except MySQLError as e:
        print(f"Database error: {e}")

fetch_and_update_eth()
fetch_and_update_btc()