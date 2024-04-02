# ENV CONF INIT
from dotenv import load_dotenv
import os
load_dotenv()

MONGO_USER = os.environ.get("MONGO_USER")
MONGO_PWD = os.environ.get("MONGO_PWD")

# MODULE INIT
from fastapi import FastAPI
app = FastAPI()

# ASYNC TASK INIT
from motor.motor_asyncio import AsyncIOMotorClient
import urllib.parse
from .db.intradayLogs import getIntradayData
from .db.mongoClient import MongoClient
import asyncio

@app.on_event("startup")
async def startup_event():
    try:
        mrPwd = urllib.parse.quote_plus(MONGO_PWD)
        await MongoClient.connectToMongoDb(f'mongodb://{MONGO_USER}:{mrPwd}@localhost:27017/marketRoach')
        asyncio.create_task(getIntradayData("SPY"))
        print("Connected to the database successfully.")
    except Exception as e:
        print(f"An error occurred while connecting to the database: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    try:
        await MongoClient.closeMongoDbConnection()
        print("Database connection closed successfully.")
    except Exception as e:
        print(f"An error occurred while closing the database connection: {e}")

# GRAPHQL INIT
from strawberry.fastapi import GraphQLRouter
from .gql.schema import schema

graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")
