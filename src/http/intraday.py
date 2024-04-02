import requests
import os
from datetime import datetime, timedelta
import random
import json

POLYGON_IO_API_KEY = os.environ.get("POLYGON_IO_API_KEY")

def getMinuteData(ticker: str, startDay: str, endDay: str, limit: int):
    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{startDay}/{endDay}?adjusted=true&sort=desc&limit={limit}&apiKey={POLYGON_IO_API_KEY}'
    response = requests.get(url)
    data = response.json()

    # with open('src/http/mocks/IntradayMinuteAgg.json', 'w') as f:
    #     json.dump(data, f)

    return data