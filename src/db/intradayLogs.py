import asyncio
from datetime import datetime, timedelta, timezone, time
from ..http.intraday import getMinuteData
from .mongoClient import MongoClient
import os
from zoneinfo import ZoneInfo

IS_MOCKED = os.environ.get("IS_MOCKED")
REQUEST_INTERVAL = 30

async def getIntradayData(ticker: str):
    while True:
        try:
            today = datetime.now(timezone.utc)
            yesterday = today - timedelta(days=1)

            afterHoursClose = time(0, 0, 0)
            preMarketOpen = time(8, 0, 0)
            isMarketClosed = afterHoursClose <= today.time() <= preMarketOpen

            if today.weekday() < 5:
                minuteData = getMinuteData(ticker, yesterday.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d'), 15 if not isMarketClosed else 960)
                unixTime = int(today.timestamp() * 1000)
                logs = []
                resultNum = len(minuteData['results'])

                for minute in minuteData['results']:
                    logs.append({
                        'symbol': minuteData['ticker'],
                        'open': minute['o'],
                        'close': minute['c'],
                        'highest': minute['h'],
                        'lowest': minute['l'],
                        'time': minute['t'],
                        'fetchTime': unixTime,
                        'number': minute['n'],
                        'volume': minute['v'],
                        'vwAveragePrice': minute['vw'],
                        'options': f'{{ "requestId": {minuteData['request_id']}, "adjusted": {minuteData['adjusted']}, "isMarketClosed": {isMarketClosed} }}',
                        'details': ''
                    })

                await addIntradayLogs(logs)

                print(f'-----------------\n{ticker}: {today}\nNumber of Results: {resultNum}\nRequest Id: {minuteData['request_id']}\n-----------------')

                if isMarketClosed:
                    print(f'Market closed - Next scan set to premarket open')
                    marketOpen = datetime(now.year, now.month, now.day + 1, 4, 0, tzinfo=timezone.utc)
                    secondsToMarketOpen = (marketOpen - today).total_seconds()
                    await asyncio.sleep(secondsToMarketOpen)
                else:
                    await asyncio.sleep(REQUEST_INTERVAL)
        except Exception as e:
            print(f'An error occurred in db/getIntradayData: {e}')
            await asyncio.sleep(REQUEST_INTERVAL)

async def addIntradayLogs(logs):
    intradayLogs = MongoClient.getCollection('intradayLogs')
    result = await intradayLogs.insert_many(logs)

async def getIntradayLogs(symbol: str, start: int, end: int):
    if IS_MOCKED.lower() == "true":
        await asyncio.sleep(0.5)
        return getMockedIntradayLogs()

    intradayLogs = MongoClient.getCollection('intradayLogs')
    print(intradayLogs.find({ 'time': { '$gte': start, '$lte': end }}))

    return []

def getMockedIntradayLogs(symbol: str, start: int, end: int):
    print('test')