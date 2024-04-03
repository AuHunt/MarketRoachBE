'''
Module handling all intradayLogs collection operations
- Background task fetching of market data for insertion
- Intraday Logs insertion
- Intraday Logs retrieval
- Mocked logs retrieval
- Sleep function for background task
'''

import textwrap
import os
import asyncio
import json
from datetime import datetime, timedelta, timezone, time
from src.http.intraday import get_minute_data
from src.db.mongo_client import MongoClient

IS_MOCKED = os.environ.get('IS_MOCKED', 'false')
REQUEST_INTERVAL = 30

async def fetch_intraday_data(symbol: str):
    '''Function that uses polygon api to retrieve intraday market data'''
    while True:
        try:
            today = datetime.now(timezone.utc)
            yesterday = today - timedelta(days=1)

            afterhours_close = time(0, 0, 0)
            premarket_open = time(8, 0, 0)
            is_market_closed = afterhours_close <= today.time() <= premarket_open

            if today.weekday() < 5:
                minute_data = get_minute_data(symbol, yesterday.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d'), 15 if not is_market_closed else 960)
                unix_time = int(today.timestamp() * 1000)
                logs = []

                if 'results' in minute_data:
                    total_results = len(minute_data['results'])
                    for minute in minute_data['results']:
                        logs.append({
                            'symbol': symbol,
                            'open': minute['o'],
                            'close': minute['c'],
                            'highest': minute['h'],
                            'lowest': minute['l'],
                            'time': str(minute['t']),
                            'fetchTime': str(unix_time),
                            'number': minute['n'],
                            'volume': minute['v'],
                            'vwAveragePrice': minute['vw'],
                            'options': json.dumps({ 
                                "requestId": minute_data['request_id'],
                                "adjusted": minute_data['adjusted'],
                                "status": minute_data['status'],
                                "is_market_closed": is_market_closed
                            }),
                            'details': ''
                        })
                else:
                    logs.append({
                        'symbol': symbol,
                        'open': 0.0,
                        'close': 0.0,
                        'highest': 0.0,
                        'lowest': 0.0,
                        'time': str(0),
                        'fetchTime': str(unix_time),
                        'number': 0,
                        'volume': 0,
                        'vwAveragePrice': 0.0,
                        'options': json.dumps({
                            "requestId": minute_data['request_id'],
                            "status": minute_data['status'],
                            "is_market_closed": is_market_closed
                        }),
                        'details': f'{{ "response": {minute_data} }}'
                    })

                await insert_intraday_logs(logs)

                print(textwrap.dedent(f'''
                    {symbol}: {today}
                    Status: {minute_data['status']}
                    Request Id: {minute_data['request_id']}
                '''))

                await sleep_manager(is_market_closed)
        except Exception as e:
            print(f'An error occurred in db/intraday_logs.py: {e}')
            await sleep_manager(is_market_closed)

async def insert_intraday_logs(logs):
    '''Function that adds logs to intradayLogs collection in MongoDB'''
    intraday_logs = MongoClient.getCollection('intradayLogs')
    result = await intraday_logs.insert_many(logs)
    print(result)

async def get_intraday_logs(symbol: str, start: str, end: str):
    '''Function that retrieves intraday data within specified range for symbol'''
    if IS_MOCKED.lower() == "true":
        await asyncio.sleep(0.5)
        return get_mocked_intraday_logs(symbol, start, end)
    else:
        intraday_logs = MongoClient.getCollection('intradayLogs')
        skip = 0
        batch_size = 125
        logs = []

        while True:
            cursor = intraday_logs.find({ 'time': { '$gte': start, '$lte': end }}, { '_id': 0 }).skip(skip).limit(batch_size)
            log_batch = await cursor.to_list(length=batch_size)
            if not log_batch:
                break
            logs.extend(log_batch)
            skip += batch_size
        
        return logs

def get_mocked_intraday_logs(symbol: str, start: int, end: int):
    '''Function that returns mock intraday log values to avoid using mongodb while testing'''
    print(f'test {symbol} {start} {end}')
    return [{
        'symbol': symbol,
        'open': 1.0,
        'close': 2.0,
        'highest': 2.5,
        'lowest': 0.5,
        'time': start,
        'fetchTime': end,
        'number': 10,
        'volume': 100,
        'vwAveragePrice': 1.5,
        'options': textwrap.dedent(f'''{{ 
            "requestId": 28719c5e0f9221512d8aafe643889753,
            "adjusted": {True},
            "status": 'MOCKED',
            "is_market_closed": {True} }}
        '''),
        'details': ''
    }]

async def sleep_manager(is_market_closed: bool):
    '''Function for setting background task sleep time'''
    if is_market_closed:
        print('Market closed - Next scan set to premarket open')
        now = datetime.now(timezone.utc)
        market_open = datetime(now.year, now.month, now.day + 1, 4, 0, tzinfo=timezone.utc)
        seconds_to_open = (market_open - now).total_seconds()
        await asyncio.sleep(seconds_to_open)
    else:
        await asyncio.sleep(REQUEST_INTERVAL)
