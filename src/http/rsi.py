import os
import requests

POLYGON_IO_API_KEY = os.environ.get("POLYGON_IO_API_KEY")

def get_rsi_data(symbol: str, start_date: str, end_date: str, limit: int):
    '''Function to request rsi data from polygon api'''
    url = f'https://api.polygon.io/v1/indicators/rsi/{symbol}/range/1/minute/{start_date}/{end_date}?sort=desc&limit={limit}&apiKey={POLYGON_IO_API_KEY}'
    response = requests.get(url, timeout=5)

    data = response.json()

    return data
