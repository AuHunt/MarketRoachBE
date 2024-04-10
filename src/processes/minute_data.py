'''
Module defining background task that fetches market data for insertion
Additionally:
- Sleep manager, which sets the sleep cycle for the task
'''

import json
import asyncio
import textwrap
from datetime import datetime, timedelta, timezone, time
from src.db.aggregate_logs import upsert_aggregate_logs
from src.http.aggregates import get_bar_aggregates, BarAggregatesParams
from src.http.rsi import get_rsi_data, RsiIndicatorParams
from src.http.sma import get_sma_data, SmaIndicatorParams

async def process_aggregate_data(
    symbol: str, interval: str, batch_size: int, request_interval: int
):
    '''Function that uses polygon api to retrieve aggregate market data'''
    precision = 4
    while True:
        try:
            # TODO++: Remove " - timedelta(days=2)" once you have full API access
            today = datetime.now(timezone.utc) - timedelta(days=2)
            today_unix_time = int(today.timestamp() * 1000)
            afterhours_close = time(0, 0, 0)
            premarket_open = time(8, 0, 0)
            is_market_closed = afterhours_close <= today.time() <= premarket_open

            if today.weekday() < 5:
                results = await fetch_market_data(symbol, today, interval, batch_size)
                minute_aggs = results[0]
                minute_rsi = results[1]
                minute_sma = results[2]
                results_per_time = {}

                if 'results' in minute_aggs:
                    for minute in minute_aggs['results']:
                        results_per_time[minute['t']] = {
                            'symbol': symbol,
                            'interval': interval,
                            'open': minute['o'],
                            'close': minute['c'],
                            'highest': minute['h'],
                            'lowest': minute['l'],
                            'volume': minute['v'],
                            'vwap': minute['vw'],
                            'time': str(minute['t']),
                            'fetchTime': str(today_unix_time),
                            'number': minute['n'],
                            'options': json.dumps({
                                # TODO++: figure out how to have other requests add their options here
                                # "requestId": minute_aggs['request_id'],
                                # "adjusted": minute_aggs['adjusted'],
                                # "status": minute_aggs['status'],
                            }),
                            'details': ''
                        }
                else:
                    # TODO++: INSERT ERROR TO ERROR COLLECTION IN MONGODB
                    print('TBD')

                if 'results' in minute_rsi and 'values' in minute_rsi['results']:
                    for minute in minute_rsi['results']['values']:
                        rsi14 = round(minute['value'], precision)
                        if minute['timestamp'] in results_per_time:
                            results_per_time[minute['timestamp']]['rsi14'] = rsi14
                        else:
                            results_per_time[minute['timestamp']] = {
                                'symbol': symbol,
                                'interval': interval,
                                'time': str(minute['timestamp']),
                                'fetchTime': str(today_unix_time),
                                'rsi14': rsi14
                            }
                else:
                    # TODO++: INSERT ERROR TO ERROR COLLECTION IN MONGODB
                    print('TBD')

                if 'results' in minute_sma and 'values' in minute_sma['results']:
                    for minute in minute_sma['results']['values']:
                        results_per_time[minute['timestamp']]['sma5'] = round(minute['value'], precision)
                else:
                    # TODO++: INSERT ERROR TO ERROR COLLECTION IN MONGODB
                    print('TBD')

                await upsert_aggregate_logs(results_per_time.values())

                print(textwrap.dedent(f'''
                    {symbol} - {today}
                    Status: {minute_aggs['status']}
                    Request Id: {minute_aggs['request_id']}
                '''))

                await sleep_manager(is_market_closed, request_interval)
        except Exception as e:
            print(f'An error occurred in db/aggregate_logs.py: {e}')
            # TODO++: INSERT ERROR TO ERROR COLLECTION IN MONGODB
            await sleep_manager(is_market_closed, request_interval)

async def sleep_manager(is_market_closed: bool, request_interval: int):
    '''Function for setting background task sleep time'''
    if is_market_closed:
        print('Market closed - Next scan set to premarket open')
        now = datetime.now(timezone.utc)
        market_open = datetime(now.year, now.month, now.day + 1, 4, 0, tzinfo=timezone.utc)
        seconds_to_open = (market_open - now).total_seconds()
        await asyncio.sleep(seconds_to_open)
    else:
        await asyncio.sleep(request_interval)

async def fetch_market_data(symbol: str, today: datetime, interval: str, batch_size: int):
    '''Function for sending out multiple requests concurrently of market data'''
    yesterday = today - timedelta(days=1)

    results = await asyncio.gather(
        get_bar_aggregates(
            BarAggregatesParams(
                symbol=symbol,
                window=1,
                interval=interval,
                start_date=yesterday.strftime('%Y-%m-%d'),
                end_date=today.strftime('%Y-%m-%d'),
                order='desc',
                limit=batch_size
            )
        ),
        get_rsi_data(
            RsiIndicatorParams(
                symbol=symbol,
                date=today.strftime('%Y-%m-%d'),
                interval=interval,
                window=14,
                order='desc',
                limit=batch_size
            )
        ),
        get_sma_data(
            SmaIndicatorParams(
                symbol=symbol,
                date=today.strftime('%Y-%m-%d'),
                interval=interval,
                window=5,
                order='desc',
                limit=batch_size
            )
        ),
    )

    return results
