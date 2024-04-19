'''
Module that defines query fields for stock information for GQL schema
'''

import os
from typing import List, Optional
import strawberry
from src.db.aggregate_logs import get_aggregate_logs
from src.db.analytics import get_analytics

IS_MOCKED = os.environ.get('IS_MOCKED', 'false')

@strawberry.type
class Datum:
    '''
    Market data return object:
        symbol - ticker symbol
        interval - interval/timeframe for when market data was retrieved
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
class Analytics:
    '''
    Market analytics return object:
        time - unix millisecond timestamp
        expiration - millisecond expiration timestamp of active pattern
        type - indicator used in pattern analysis
        interval - interval/timeframe during which the analysis took place
        details - details about the analytics/pattern discovered
        symbol - ticker symbol
    '''
    time: str
    expiration: str
    type: str
    interval: str
    details: str
    symbol: str

@strawberry.type
class StockQuery:
    '''
    Class defining all query operations for stock data
    '''
    @strawberry.field
    async def get_stock_data(
        self, symbol: str, start: str, end: str, interval: str
    ) -> List[Datum]:
        '''
        Query field for stock data
        Resolver retrieves market data from MongoDb query
        '''
        data = await get_aggregate_logs(symbol, start, end, interval, bool(IS_MOCKED))

        stock_data = [Datum(**item) for item in data]

        return stock_data

    @strawberry.field
    async def get_stock_analytics(
        self, symbol: str, start: str, end: str, interval: str
    ) -> List[Analytics]:
        '''
        Query field for stock analytics
        Resolver retrieves analytics from MongoDb query
        '''
        data = await get_analytics(symbol, start, end, interval, bool(IS_MOCKED))

        analytics = [Analytics(**item) for item in data]

        return analytics
