'''
Module that defines query fields for stock information for GQL schema
'''

from typing import List
import strawberry
from src.db.aggregate_logs import get_aggregate_logs

@strawberry.type
class MarketData:
    '''
    Market data return object:
        symbol - ticker symbol
        open - open price
        close - close price
        highest - highest price
        lowest - lowest price
        volume - trading volume
        vwap - volume weighed average price
        rsi6 - relative strength index (window size = 6)
        rsi14 - relative strength index (window size = 14)
        rsi24 - relative strength index (window size = 24)
        sma5 - simple moving avereage (window size = 5)
        sma100 - simple moving avereage (window size = 100)
        sma200 - simple moving avereage (window size = 200)
        time - unix millisecond timestamp
        fetchTime - time when datapoint was retrieved
        number - number of transactions in aggregate window
        options - request/response metadata
        details - Relevant information about datapoint
    '''
    symbol: str
    open: float
    close: float
    highest: float
    lowest: float
    volume: float
    vwap: float
    rsi6: float
    rsi14: float
    rsi24: float
    sma5: float
    sma100: float
    sma200: float
    time: str
    fetchTime: str
    number: int
    options: str
    details: str

@strawberry.type
class StockQuery:
    '''
    Class defining all query operations for stock data
    '''
    @strawberry.field
    async def get_market_data(
        self, symbol: str, start: str, end: str, interval: str
    ) -> List[MarketData]:
        '''
        Query field for market data
        Resolver retrieves data from MongoDb query
        '''
        data = await get_aggregate_logs(symbol, start, end, interval)

        market_data = [MarketData(**item) for item in data]

        return market_data
