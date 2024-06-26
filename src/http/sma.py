'''
Module that handles the polygon.io API requests for simple moving average (SMA) indicator data
'''

import os
import textwrap
from dataclasses import dataclass
import aiohttp

POLYGON_IO_API_KEY = os.environ.get("POLYGON_IO_API_KEY")

@dataclass
class SmaIndicatorParams:
    '''
    get_sma_data parameters:
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
    window: str
    order: str
    limit: int

async def get_sma_data(params: SmaIndicatorParams):
    '''Function to request simple moving average (SMA) data from polygon api'''
    url = textwrap.dedent(f'''
        https://api.polygon.io/v1/indicators/sma/{params.symbol}?timestamp={params.date}&timespan={params.interval}&window={params.window}
        &series_type=close&order={params.order}&limit={params.limit}&apiKey={POLYGON_IO_API_KEY}
    ''').replace('\n', '')

    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=5) as response:
            data = await response.json()

    return data
