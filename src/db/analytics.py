'''
Module handling all aggregateLogs collection operations
- Analytics data insertion
- Analytics data retrieval
- Mocked data retrieval
'''

import asyncio
import random
from datetime import datetime, timezone
from pymongo import UpdateOne
from src.db.mongo_client import MongoClient
from src.utils.interval_to_ms import interval_to_ms

async def insert_analytics_data(data):
    '''Function that adds datapoints to analytics collection in MongoDB'''
    analytics = MongoClient.get_collection('analytics')
    result = await analytics.insert_many(data)
    print(result)

async def upsert_analytics_data(data):
    '''Function for inserting/editing multiple analytics data entries'''
    analytics = MongoClient.get_collection('analytics')
    operations = []
    for item in data:
        query = {
            'time': item['time'],
            'type': item['type']
        }
        update = {'$set': item}
        operations.append(UpdateOne(query, update, upsert=True))

    result = await analytics.bulk_write(operations)
    print(result)

async def get_analytics(symbol: str, start: str, end: str, interval: str, is_mocked: bool):
    '''Function that retrieves analytics data within specified range for symbol'''
    if is_mocked is True:
        await asyncio.sleep(0.5)
        return get_mocked_analytics(symbol, start, end, interval)
    else:
        analytics = MongoClient.get_collection('analytics')
        skip = 0
        batch_size = 125
        logs = []

        while True:
            pipeline = [
                {
                    '$match': { 
                        'time': {'$gte': start, '$lte': end}, 
                        'interval': { '$eq': interval } 
                    }
                },
                { '$project': { '_id': 0 } },
                { '$sort': { 'time': -1 } },
                { '$skip': skip },
                { '$limit': batch_size }
            ]

            cursor = analytics.aggregate(pipeline)
            log_batch = await cursor.to_list(length=batch_size)

            if not log_batch:
                break

            logs.extend(log_batch)
            skip += batch_size

        return logs

def get_mocked_analytics(symbol: str, start: str, end: str, interval: str):
    '''Function that returns mock aggregate log values to avoid mongodb operations while testing'''
    now = int((datetime.now(timezone.utc)).timestamp() * 1000)
    start_ms = int(round(int(start) / 1000.0 / 60) * 60 * 1000)
    end_ms = int(round(int(end) / 1000.0 / 60) * 60 * 1000)
    interval_ms = interval_to_ms(interval)
    entries_num = (end_ms - start_ms) // interval_ms

    open_price = random.uniform(499.5555, 500.5555)
    close_price = random.uniform(499.5555, 500.5555)
    highest = open_price if open_price > close_price else close_price
    lowest = open_price if open_price < close_price else close_price
    entries = []

    for i in range(entries_num if entries_num <= 960 else 960):
        entries.append({
            'time': str(start_ms + interval_ms * i),
            'close': round(close_price, 2),
            'details': '',
            'fetchTime': str(now),
            'highest': round(highest + random.uniform(0, 1), 2),
            'interval': interval,
            'lowest': round(lowest - random.uniform(0, 1), 2),
            'number': random.randint(10, 5000),
            'open': round(open_price, 2),
            'options': '{}',
            'rsi14': round(random.uniform(25, 75), 2),
            'sma5': round(close_price + random.uniform(-2, 2), 2),
            'symbol': symbol,
            'volume': random.randint(50000, 200000),
            'vwap': round(close_price + random.uniform(-4, 4), 4)
        })

    return entries
