# def print_hi(name):
#
# if __name__ == '__main__':
#     print_hi('PyCharm')

import ccxt
import pandas as pd
import time
from datetime import datetime, timezone

import psycopg2
from config import myconfig
import math

from psycopg2 import sql

# https://khashtamov.com/ru/postgresql-python-psycopg2/
conn = psycopg2.connect(dbname='cryptotrading', user='postgres',
                        password='379245chs37', host='localhost')
cursor = conn.cursor()

# INITIALIZE

exchange = ccxt.wazirx({
    "apiKey": "xakFgqYBzS6qGmJHTj0OdkVbbMOhJvPpR27G7oqMgd58j91WHxsiSSpKCks5XqEY",
    "secret": "knTqNJLaLLkVRaBC6m9Ikg3dvLqCCZgMayBqe6WKkU8cKbOr6ASrfF6cXNwqStTd"
})

markets = exchange.fetchMarkets()
market_symbols = [market['symbol'] for market in markets]
market_symbols_length = len(market_symbols)
crypto_combinations = []
# TESTING

if market_symbols_length > 0:
    print("TEST: OK No. of market symbols: " + market_symbols_length.__str__());
    print(f'-- Sample:{market_symbols[0:5]}')
    print("-" * 100)

# GET ALL THE CRYPTO COMBINATIONS FOR USDT
def get_crypto_combinations(symbols, base):
    combinations = []
    for sym1 in symbols:

        sym1_token1 = sym1.split('/')[0]
        sym1_token2 = sym1.split('/')[1]

        if (sym1_token2 == base):
            for sym2 in symbols:
                sym2_token1 = sym2.split('/')[0]
                sym2_token2 = sym2.split('/')[1]
                if (sym1_token1 == sym2_token2):
                    for sym3 in symbols:
                        sym3_token1 = sym3.split('/')[0]
                        sym3_token2 = sym3.split('/')[1]
                        if ((sym2_token1 == sym3_token1) and (sym3_token2 == sym1_token2)):
                            combination = {
                                'base': sym1_token2,
                                'intermediate': sym1_token1,
                                'ticker': sym2_token1,
                            }
                            combinations.append(combination)

    return combinations


wx_combinations_usdt = get_crypto_combinations(market_symbols, 'USDT')

print(f'No. of crypto combinations for USDT: {len(wx_combinations_usdt)}')

cominations_df = pd.DataFrame(wx_combinations_usdt)
cominations_df.head()


# STEP 2: PERFORM TRIANGULAR ARBITRAGE
# Utility method to fetch the current ticker price

def fetch_current_ticker_price(ticker):
    current_ticker_details = exchange.fetch_ticker(ticker)
    ticker_price = current_ticker_details['close'] if current_ticker_details is not None else None
    return ticker_price


def check_if_float_zero(value):
    return math.isclose(value, 0.0, abs_tol=1e-3)


# Triangular Arbitrage

def check_buy_buy_sell(scrip1, scrip2, scrip3, initial_investment):
    ## SCRIP1
    investment_amount1 = initial_investment
    current_price1 = fetch_current_ticker_price(scrip1)
    final_price = 0
    scrip_prices = {}

    if current_price1 is not None and not check_if_float_zero(current_price1):
        buy_quantity1 = round(investment_amount1 / current_price1, 8)

        # TRY WITHOUT SLEEP IF THE EXCHANGE DOES NOT THROW RATE LIMIT EXCEPTIONS
        time.sleep(1)
        ## SCRIP2
        investment_amount2 = buy_quantity1
        current_price2 = fetch_current_ticker_price(scrip2)
        if current_price2 is not None and not check_if_float_zero(current_price2):
            buy_quantity2 = round(investment_amount2 / current_price2, 8)

            # TRY WITHOUT SLEEP IF THE EXCHANGE DOES NOT THROW RATE LIMIT EXCEPTIONS
            time.sleep(1)
            ## SCRIP3
            investment_amount3 = buy_quantity2
            current_price3 = fetch_current_ticker_price(scrip3)
            if current_price3 is not None and not check_if_float_zero(current_price3):
                sell_quantity3 = buy_quantity2
                final_price = round(sell_quantity3 * current_price3, 3)
                scrip_prices = {scrip1: current_price1, scrip2: current_price2, scrip3: current_price3}

    return final_price, scrip_prices


def check_buy_sell_sell(scrip1, scrip2, scrip3, initial_investment):
    ## SCRIP1
    investment_amount1 = initial_investment
    current_price1 = fetch_current_ticker_price(scrip1)
    final_price = 0
    scrip_prices = {}
    if current_price1 is not None and not check_if_float_zero(current_price1):
        buy_quantity1 = round(investment_amount1 / current_price1, 8)

        # TRY WITHOUT SLEEP IF THE EXCHANGE DOES NOT THROW RATE LIMIT EXCEPTIONS
        time.sleep(1)
        ## SCRIP2
        investment_amount2 = buy_quantity1
        current_price2 = fetch_current_ticker_price(scrip2)
        if current_price2 is not None and not check_if_float_zero(current_price2):
            sell_quantity2 = buy_quantity1
            sell_price2 = round(sell_quantity2 * current_price2, 8)

            # TRY WITHOUT SLEEP IF THE EXCHANGE DOES NOT THROW RATE LIMIT EXCEPTIONS
            time.sleep(1)
            ## SCRIP1
            investment_amount3 = sell_price2
            current_price3 = fetch_current_ticker_price(scrip3)
            if current_price3 is not None and not check_if_float_zero(current_price3):
                sell_quantity3 = sell_price2
                final_price = round(sell_quantity3 * current_price3, 3)
                scrip_prices = {scrip1: current_price1, scrip2: current_price2, scrip3: current_price3}
    return final_price, scrip_prices


def check_profit_loss(total_price_after_sell, initial_investment, transaction_brokerage, min_profit):
    apprx_brokerage = transaction_brokerage * initial_investment / 100 * 3
    min_profitable_price = initial_investment + apprx_brokerage + min_profit
    profit_loss = round(total_price_after_sell - min_profitable_price, 3)
    return profit_loss


# STEP 3: PLACE THE TRADE ORDERS
def place_buy_order(scrip, quantity, limit):
    order = exchange.create_limit_buy_order(scrip, quantity, limit)
    return order


def place_sell_order(scrip, quantity, limit):
    order = exchange.create_limit_sell_order(scrip, quantity, limit)
    return order


def place_trade_orders(type, scrip1, scrip2, scrip3, initial_amount, scrip_prices):
    final_amount = 0.0
    if type == 'BUY_BUY_SELL':
        s1_quantity = initial_amount / scrip_prices[scrip1]
        place_buy_order(scrip1, s1_quantity, scrip_prices[scrip1])

        s2_quantity = s1_quantity / scrip_prices[scrip2]
        place_buy_order(scrip2, s2_quantity, scrip_prices[scrip2])

        s3_quantity = s2_quantity
        place_sell_order(scrip3, s3_quantity, scrip_prices[scrip3])

    elif type == 'BUY_SELL_SELL':
        s1_quantity = initial_amount / scrip_prices[scrip1]
        place_buy_order(scrip1, s1_quantity, scrip_prices[scrip1])

        s2_quantity = s1_quantity
        place_sell_order(scrip2, s2_quantity, scrip_prices[scrip2])

        s3_quantity = s2_quantity * scrip_prices[scrip2]
        place_sell_order(scrip3, s3_quantity, scrip_prices[scrip3])

    return final_amount


# STEP 4: WRAPPING IT TOGETHER
def perform_triangular_arbitrage(scrip1, scrip2, scrip3, arbitrage_type, initial_investment,
                                 transaction_brokerage, min_profit):
    final_price = 0.0
    if (arbitrage_type == 'BUY_BUY_SELL'):
        # Check this combination for triangular arbitrage: scrip1 - BUY, scrip2 - BUY, scrip3 - SELL
        final_price, scrip_prices = check_buy_buy_sell(scrip1, scrip2, scrip3, initial_investment)

    elif (arbitrage_type == 'BUY_SELL_SELL'):
        # Check this combination for triangular arbitrage: scrip1 - BUY, scrip2 - SELL, scrip3 - SELL
        final_price, scrip_prices = check_buy_sell_sell(scrip1, scrip2, scrip3, initial_investment)

    profit_loss = check_profit_loss(final_price, initial_investment, transaction_brokerage, min_profit)

    if profit_loss > 0:
        time_utc = datetime.now().replace(tzinfo=timezone.utc)
        time_local = datetime.now()

        print(f"PROFIT-{time_local.strftime('%H:%M:%S')}:" \
              f"{arbitrage_type}, {scrip1},{scrip2},{scrip3}, Profit/Loss: {round(final_price - initial_investment, 3)} ")
        with conn.cursor() as cursor:
            conn.autocommit = True
            values = [
                (time_utc, arbitrage_type, round(final_price - initial_investment, 3))
            ]
            insert = sql.SQL(
                'INSERT INTO triangular_arbitrage_signals (timestamp, arbitrage_type, profit_loss)'
                'VALUES {}').format(sql.SQL(',').join(map(sql.Literal, values))
                                    )
            cursor.execute(insert)

        # UNCOMMENT THIS LINE TO PLACE THE ORDERS
        # place_trade_orders(arbitrage_type, scrip1, scrip2, scrip3, initial_investment, scrip_pri


INVESTMENT_AMOUNT_DOLLARS = 100
MIN_PROFIT_DOLLARS = 0.5
BROKERAGE_PER_TRANSACTION_PERCENT = 0.2

# UNCOMMENT THE WHILE LOOP TO RUN THIS LOOP CONTINUOUSLY
# while(1):
for combination in wx_combinations_usdt:
    base = combination['base']
    intermediate = combination['intermediate']
    ticker = combination['ticker']

    s1 = f'{intermediate}/{base}'  # Eg: BTC/USDT
    s2 = f'{ticker}/{intermediate}'  # Eg: ETH/BTC
    s3 = f'{ticker}/{base}'  # Eg: ETH/USDT

    # Check triangular arbitrage for buy-buy-sell
    perform_triangular_arbitrage(s1, s2, s3, 'BUY_BUY_SELL', INVESTMENT_AMOUNT_DOLLARS,
                                 BROKERAGE_PER_TRANSACTION_PERCENT, MIN_PROFIT_DOLLARS)
    # Sleep to avoid rate limit on api calls (RateLimitExceeded exception)
    time.sleep(1)
    # Check triangular arbitrage for buy-sell-sell
    perform_triangular_arbitrage(s3, s2, s1, 'BUY_SELL_SELL', INVESTMENT_AMOUNT_DOLLARS,
                                 BROKERAGE_PER_TRANSACTION_PERCENT, MIN_PROFIT_DOLLARS)
    time.sleep(1)
