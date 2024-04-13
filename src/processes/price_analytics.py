'''
Module defining background task that fetches market data for insertion
Additionally:
- Sleep manager, which sets the sleep cycle for the task
'''

import json
import asyncio
import textwrap

from datetime import datetime, timedelta, timezone, time
from src.db.errors import insert_error
from src.db.aggregate_logs import upsert_aggregate_logs
from src.http.aggregates import get_bar_aggregates, BarAggregatesParams
from src.http.rsi import get_rsi_data, RsiIndicatorParams
from src.http.sma import get_sma_data, SmaIndicatorParams

PRECISION = 4

async def analyze_price_data(
    symbol: str, interval: str, batch_size: int, request_interval: int
):
    '''Function that uses polygon api to retrieve aggregate market data'''
    while True:
        try:
            # TODO++: Remove " - timedelta(days=2)" once you have full API access
            today = datetime.now(timezone.utc) - timedelta(days=2)
            unix_today = str(int(today.timestamp() * 1000))
            afterhours_close = time(0, 0, 0)
            premarket_open = time(8, 0, 0)
            is_market_closed = afterhours_close <= today.time() <= premarket_open
            if today.weekday() < 5:
                results = await fetch_market_data(symbol, today, interval, batch_size)
                market_aggs = results[0]
                market_rsi = results[1]
                market_sma = results[2]
                results_per_time = await parse_aggregate_data(
                    symbol, interval, unix_today, market_aggs
                )
                await parse_rsi_data(symbol, interval, unix_today, market_rsi, results_per_time)
                await parse_sma_data(market_sma, results_per_time)

                await upsert_aggregate_logs(results_per_time.values())

                print(textwrap.dedent(f'''
                    {symbol} - {today}
                    Task complete - market data uploaded
                '''))

                await sleep_manager(is_market_closed, request_interval)
        except Exception as e:
            print(f'An error occurred in db/aggregate_logs.py: {e}')
            error_time = int((datetime.now(timezone.utc)).timestamp() * 1000)
            await insert_error({
                'time': str(error_time),
                'description': 'Error processing minute market data',
                'source': 'src/processes/market_data.py - process_market_data',
                'details': str(e)
            })
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

async def parse_aggregate_data(symbol: str, interval: str, fetch_time: str, agg_json: dict) -> dict:
    '''Function that specifically processes market price data based on interval and symbol'''
    results_per_time = {}

    if 'results' in agg_json:
        for minute in agg_json['results']:
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
                'fetchTime': fetch_time,
                'number': minute['n'],
                'options': json.dumps({
                    'requestIds': {
                        'aggregate': agg_json['request_id'],
                    }
                }),
                'details': ''
            }
    else:
        await insert_error({
            'time': fetch_time,
            'description': f'Error processing {interval} aggregate data on {symbol}',
            'source': 'src/processes/market_data.py - parse_aggregate_data',
            'details': json.dumps(agg_json)
        })

    return results_per_time

async def parse_rsi_data(
    symbol: str, interval: str, fetch_time: str, rsi_json: dict, results_per_time: dict
):
    '''Function that specifically processes rsi data based on interval and symbol'''
    if 'results' in rsi_json and 'values' in rsi_json['results']:
        for minute in rsi_json['results']['values']:
            rsi14 = round(minute['value'], PRECISION)
            if minute['timestamp'] in results_per_time:
                results_per_time[minute['timestamp']]['rsi14'] = rsi14
                options = json.loads(results_per_time[minute['timestamp']]['options'])
                if 'requestIds' in options:
                    options['requestIds']['rsi14'] = rsi_json['request_id']
                    results_per_time[minute['timestamp']]['options'] = json.dumps(options)
            else:
                results_per_time[minute['timestamp']] = {
                    'symbol': symbol,
                    'interval': interval,
                    'time': str(minute['timestamp']),
                    'fetchTime': fetch_time,
                    'rsi14': rsi14,
                    'options': json.dumps({
                        'requestIds': {
                            'rsi14': rsi_json['request_id'],
                        }
                    }),
                    'details': ''
                }
    else:
        error_time = int((datetime.now(timezone.utc)).timestamp() * 1000)
        await insert_error({
            'time': str(error_time),
            'description': 'Error processing minute rsi14 data',
            'source': 'src/processes/market_data.py - parse_rsi_data',
            'details': json.dumps(rsi_json)
        })

async def parse_sma_data(sma_json: dict, results_per_time: dict):
    '''Function that specifically processes sma data based on interval and symbol'''
    if 'results' in sma_json and 'values' in sma_json['results']:
        for minute in sma_json['results']['values']:
            sma5 = round(minute['value'], PRECISION)
            results_per_time[minute['timestamp']]['sma5'] = sma5
            options = json.loads(results_per_time[minute['timestamp']]['options'])
            if 'requestIds' in options:
                options['requestIds']['sma5'] = sma_json['request_id']
                results_per_time[minute['timestamp']]['options'] = json.dumps(options)
    else:
        error_time = int((datetime.now(timezone.utc)).timestamp() * 1000)
        await insert_error({
            'time': str(error_time),
            'description': 'Error processing minute sma data',
            'source': 'src/processes/market_data.py - parse_sma_data',
            'details': json.dumps(sma_json)
        })
