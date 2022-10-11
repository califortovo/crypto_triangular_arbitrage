# def print_hi(name):
#
# if __name__ == '__main__':
#     print_hi('PyCharm')

import ccxt
import pandas as pd
import time
from datetime import datetime
from config import myconfig
import math

exchange = ccxt.wazirx({
    "apiKey": ,
    "secret":
})

markets = exchange.fetchMarkets()
market_symbols = [market['symbol'] for market in markets]
print(f'No. of market symbols: {len(market_symbols)}')
print(f'Sample:{market_symbols[0:5]}')