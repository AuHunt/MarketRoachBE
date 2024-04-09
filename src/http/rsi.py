'''
Module that handles the polygon.io API requests for relative strength index (RSI) indicator data
'''

import os
import textwrap
from dataclasses import dataclass
import requests

POLYGON_IO_API_KEY = os.environ.get("POLYGON_IO_API_KEY")

@dataclass
class RsiIndicatorParams:
    '''
    get_rsi_data parameters:
        symbol - ticker symbol
        date - date to retrieve rsi data (YYYY-MM-DD)
        interval - aggregate time interval (second, minute, hour, day, etc.)
        window - window size
        order - result order (asc, desc)
        limit - number of retrieved aggregate bars
    '''
    symbol: str
    date: str
    interval: str
    window: int
    order: str
    limit: int

def get_rsi_data(params: RsiIndicatorParams):
    '''Function to request relative strength index (RSI) data from polygon api'''
    url = textwrap.dedent(f'''
        https://api.polygon.io/v1/indicators/rsi/{params.symbol}?timestamp={params.date}&timespan={params.interval}&window={params.window}&series_type=close
        &order={params.order}&limit={params.limit}&apiKey={POLYGON_IO_API_KEY}
    ''')
    response = requests.get(url, timeout=5)

    data = response.json()

    return data
