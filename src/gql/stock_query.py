import strawberry
from typing import List, Optional
from src.db.intraday_logs import get_intraday_logs

@strawberry.type
class Minute:
    symbol: str
    open: float
    close: float
    highest: float
    lowest: float
    volume: float
    vwAveragePrice: float
    time: str
    fetchTime: str
    number: int
    options: str
    details: str

@strawberry.type
class StockQuery:
    @strawberry.field
    async def get_minutes(self, symbol: str, start: str, end: str) -> List[Minute]:
        data = await get_intraday_logs(symbol, start, end)

        minutes = [Minute(**item) for item in data]

        return minutes
