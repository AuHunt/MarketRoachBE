'''
Module handling all aggregateLogs collection operations
- Analytics data insertion
- Analytics data retrieval
- Mocked data retrieval
'''

import asyncio
import random
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
    start_ms = int(round(int(start) / 1000.0 / 60) * 60 * 1000)
    end_ms = int(round(int(end) / 1000.0 / 60) * 60 * 1000)
    interval_ms = interval_to_ms(interval)
    entries_num = (end_ms - start_ms) // interval_ms

    analytic_entries = []
    analytics_types = ['RSI', 'VWAP', 'BOLL', 'MA', 'VOL', 'VI?']

    for i in range(entries_num if entries_num <= 960 else 960):
        type_index = random.randint(0, len(analytics_types))
        analytic_entries.append({
            'time': str(start_ms + interval_ms * i),
            'expiration': str(start_ms + interval_ms * (i + 5)),
            'type': analytics_types[type_index],
            'interval': interval,
            'details': 'Test description',
            'symbol': symbol
        })

    return analytic_entries
