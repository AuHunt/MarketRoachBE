'''
Module that handles polygon.io API requests for market data
'''

import os
import textwrap
from dataclasses import dataclass
import aiohttp

POLYGON_IO_API_KEY = os.environ.get("POLYGON_IO_API_KEY")

@dataclass
class BarAggregatesParams:
    '''
    get_bar_aggregates parameters:
        symbol - ticker symbol
        window - interval window size
        interval - aggregate time interval (second, minute, hour, day, etc.)
        start_date - start of aggregate time window (YYYY-MM-DD)
        end_date - end of aggregate time window (YYYY-MM-DD)
        order - result order (asc, desc)
        limit - number of retrieved aggregate bars
    '''
    symbol: str
    window: int
    interval: str
    start_date: str
    end_date: str
    order: str
    limit: int

async def get_bar_aggregates(params: BarAggregatesParams):
    '''
    Poligon.io request to retrieve market data as bar aggregates (candlebars)
    '''
    url = textwrap.dedent(f'''
        https://api.polygon.io/v2/aggs/ticker/{params.symbol}/range/{params.window}/{params.interval}/{params.start_date}/
        {params.end_date}?sort={params.order}&limit={params.limit}&apiKey={POLYGON_IO_API_KEY}
    ''').replace('\n', '')

    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=5) as response:
            data = await response.json()

    return data
