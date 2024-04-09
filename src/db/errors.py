'''
Module handling all errors collection operations
- Errors insertion
'''

from src.db.mongo_client import MongoClient

async def insert_errors(errors):
    '''Function that adds errors to errors collection in MongoDB'''
    errors_collection = MongoClient.get_collection('errors')
    result = await errors_collection.insert_many(errors)
    print(result)
