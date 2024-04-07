'''
Module defining class for handling MongoDb connections
Uses singleton pattern, therefore should only be instantiating one connection at a time
'''

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

class MongoClient:
    '''
    Class handling MongoDb single connection
    - Connect to MongoDb through url
    - Get MongoDb motor client collection
    - Close MongoDb connection
    '''
    _instance = None
    dbClient: AsyncIOMotorClient = None
    database: AsyncIOMotorDatabase = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoClient, cls).__new__(cls)
        return cls._instance

    @classmethod
    async def connect_to_mongodb(cls, connection_url):
        '''
        Function that connects motor client to MongoDb through url
        Url example: mongodb://{MONGO_USER}:{MONGO_PWD}@localhost:27017/{MONGO_DB_NAME}
        '''
        if not cls.dbClient:
            cls.dbClient = AsyncIOMotorClient(connection_url)
            cls.database = cls.dbClient.get_default_database()

    @classmethod
    def get_collection(cls, collection_name):
        '''
        Function for obtaining async motor client with a specific collection
        '''
        # pylint: disable=unsubscriptable-object
        return cls.database[collection_name]

    @classmethod
    async def close_mongodb_connection(cls):
        '''
        Function to close motor client connection and reset class instance
        '''
        cls.dbClient.close()
        cls._instance = None
        cls.dbClient = None
