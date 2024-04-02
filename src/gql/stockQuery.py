import strawberry
from typing import List, Optional
from ..db.intradayLogs import getIntradayLogs

@strawberry.type
class StockMinute:
    symbol: str
    open: float
    close: float
    highest: float
    lowest: float
    volume: float
    vwAveragePrice: float
    time: int
    fetchTime: int
    number: int
    options: str
    details: str

@strawberry.type
class StockQuery:
    @strawberry.field
    def getMinuteData(self, symbol: str, start: int, end: int) -> List[StockMinute]:
        data = getIntradayLogs(symbol, start, end)

        return data
