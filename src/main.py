'''
Entry point to MarketRoach backend service
Database connection, FastAPI task, GQL server, and other things are initialized here
'''

import asyncio
import urllib.parse
import os
from dotenv import load_dotenv
load_dotenv()

# pylint: disable=wrong-import-position
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
from src.db.mongo_client import MongoClient
from src.processes.minute_data import process_aggregate_data
from src.gql.schema import schema

# ENV CONF INIT
MONGO_USER = os.environ.get("MONGO_USER")
MONGO_PWD = urllib.parse.quote_plus(os.environ.get("MONGO_PWD"))

# MODULE INIT
app = FastAPI()

# ASYNC TASK INIT
@app.on_event("startup")
async def startup_event():
    '''Background tasks startup event'''
    try:
        await MongoClient.connect_to_mongodb(
            f'mongodb://{MONGO_USER}:{MONGO_PWD}@localhost:27017/marketRoach')
        asyncio.create_task(process_aggregate_data('SPY', 'minute', 15, 30))
        print("Connected to the database successfully.")
    except Exception as e:
        print(f"An error occurred while connecting to the database: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    '''Background tasks shutdown event'''
    try:
        await MongoClient.close_mongodb_connection()
        print("Database connection closed successfully.")
    except Exception as e:
        print(f"An error occurred while closing the database connection: {e}")

# GRAPHQL INIT
graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")
