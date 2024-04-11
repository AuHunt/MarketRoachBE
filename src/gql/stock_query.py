'''
Module that defines query fields for stock information for GQL schema
'''

import os
from typing import List, Optional
import strawberry
from src.db.aggregate_logs import get_aggregate_logs

IS_MOCKED = os.environ.get('IS_MOCKED', 'false')

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
    interval: str
    open: Optional[float] = None
    close: Optional[float] = None
    highest: Optional[float] = None
    lowest: Optional[float] = None
    volume: Optional[float] = None
    vwap: Optional[float] = None
    # rsi6: Optional[float] = None
    rsi14: Optional[float] = None
    # rsi24: Optional[float] = None
    sma5: Optional[float] = None
    # sma100: Optional[float] = None
    # sma200: Optional[float] = None
    time: str
    fetchTime: str
    number: Optional[int] = None
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
        data = await get_aggregate_logs(symbol, start, end, interval, IS_MOCKED)

        market_data = [MarketData(**item) for item in data]

        return market_data
