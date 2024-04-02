from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
import asyncio

class MongoClient:
    _instance = None
    dbClient: AsyncIOMotorClient = None
    database: AsyncIOMotorDatabase = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DataBase, cls).__new__(cls)
        return cls._instance

    @classmethod
    async def connectToMongoDb(cls, connectionUrl):
        if not cls.dbClient:
            cls.dbClient = AsyncIOMotorClient(connectionUrl)
            cls.database = cls.dbClient.get_default_database()

    @classmethod
    def getCollection(cls, collectionName):
        return cls.database[collectionName]

    @classmethod
    async def closeMongoDbConnection(cls):
        cls.dbClient.close()
        cls._instance = None
        cls.dbClient = None
