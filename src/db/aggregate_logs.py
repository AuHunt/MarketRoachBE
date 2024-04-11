'''
Module handling all aggregateLogs collection operations
- Aggregate Logs insertion
- Aggregate Logs retrieval
- Mocked logs retrieval
'''

import asyncio
import json
from pymongo import UpdateOne
from src.db.mongo_client import MongoClient

async def insert_aggregate_logs(logs):
    '''Function that adds logs to aggregateLogs collection in MongoDB'''
    aggregate_logs = MongoClient.get_collection('aggregateLogs')
    result = await aggregate_logs.insert_many(logs)
    print(result)

async def upsert_aggregate_logs(logs):
    '''Function for mass-inserting/editing aggregation logs'''
    aggregate_logs = MongoClient.get_collection('aggregateLogs')
    operations = []
    for item in logs:
        query = {"time": item["time"]}
        update = {"$set": item}
        operations.append(UpdateOne(query, update, upsert=True))

    result = await aggregate_logs.bulk_write(operations)
    print(result)

async def get_aggregate_logs(symbol: str, start: str, end: str, interval: str, is_mocked: bool):
    '''Function that retrieves aggregate data within specified range for symbol'''
    if is_mocked is True:
        await asyncio.sleep(0.5)
        return get_mocked_aggregate_logs(symbol, start, end)
    else:
        aggregate_logs = MongoClient.get_collection('aggregateLogs')
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

            cursor = aggregate_logs.aggregate(pipeline)
            log_batch = await cursor.to_list(length=batch_size)

            if not log_batch:
                break

            logs.extend(log_batch)
            skip += batch_size

        return logs

# TODO++: Update and move to its own file
def get_mocked_aggregate_logs(symbol: str, start: int, end: int):
    '''Function that returns mock aggregate log values to avoid using mongodb while testing'''
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
        'vwap': 1.5,
        'options': json.dumps({
            "requestId": "28719c5e0f9221512d8aafe643889753",
            "adjusted": {True},
            "status": 'MOCKED',
            "is_market_closed": {True} 
        }),
        'details': ''
    }]
