'''
Module defining background task that fetches market data for insertion
Additionally:
- Sleep manager, which sets the sleep cycle for the task
'''

import json
import asyncio
import textwrap

from datetime import datetime, timedelta, timezone, time
from src.db.errors import insert_error
from src.db.aggregate_logs import upsert_aggregate_logs
from src.http.aggregates import get_bar_aggregates, BarAggregatesParams
from src.http.rsi import get_rsi_data, RsiIndicatorParams
from src.http.sma import get_sma_data, SmaIndicatorParams

PRECISION = 4

async def analyze_price_data(
    symbol: str, interval: str, batch_size: int, request_interval: int
):
    '''Function that processes market data stored in the db to find useful analytics'''
    while True:
        try:
            # TODO++: Remove " - timedelta(days=2)" once you have full API access
            today = datetime.now(timezone.utc) - timedelta(days=2)
            unix_today = str(int(today.timestamp() * 1000))
            afterhours_close = time(0, 0, 0)
            premarket_open = time(8, 0, 0)
            is_market_closed = afterhours_close <= today.time() <= premarket_open
            if today.weekday() < 5:
                await sleep_manager(is_market_closed, request_interval)
        except Exception as e:
            print(f'An error occurred in db/aggregate_logs.py: {e}')
            error_time = int((datetime.now(timezone.utc)).timestamp() * 1000)
            await insert_error({
                'time': str(error_time),
                'description': 'Error processing minute market data',
                'source': 'src/processes/market_data.py - process_market_data',
                'details': str(e)
            })
            await sleep_manager(is_market_closed, request_interval)

async def sleep_manager(is_market_closed: bool, request_interval: int):
    '''Function for setting background task sleep time'''
    if is_market_closed:
        print('Market closed - Next scan set to premarket open')
        now = datetime.now(timezone.utc)
        market_open = datetime(now.year, now.month, now.day + 1, 4, 0, tzinfo=timezone.utc)
        seconds_to_open = (market_open - now).total_seconds()
        await asyncio.sleep(seconds_to_open)
    else:
        await asyncio.sleep(request_interval)
