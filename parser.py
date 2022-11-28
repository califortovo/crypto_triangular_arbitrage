import asyncio
import time

import ccxt
import psycopg2
from psycopg2 import sql

# https://khashtamov.com/ru/postgresql-python-psycopg2/
db_conn = psycopg2.connect(dbname='cryptotrading', user='postgres',
                           password='379245chs37', host='localhost')
db_conn.autocommit = True
cursor = db_conn.cursor()

ccxt_conn = ccxt.wazirx({
    "apiKey": "xakFgqYBzS6qGmJHTj0OdkVbbMOhJvPpR27G7oqMgd58j91WHxsiSSpKCks5XqEY",
    "secret": "knTqNJLaLLkVRaBC6m9Ikg3dvLqCCZgMayBqe6WKkU8cKbOr6ASrfF6cXNwqStTd"
})

exchange_names = ["binance", "bitfinex"]
exchanges_in_db = []  # Биржи, которые есть в БД

# Получаем биржи с БД
cursor.execute('SELECT name FROM exchange')
for t in cursor.fetchall():
    exchanges_in_db.append(t[0])


def saveTickers():
    exchanges = {}

    for exchange_name in exchange_names:
        exchange = getattr(ccxt, exchange_name)
        exchanges[exchange_name] = exchange({
            'enableRateLimit': True
        })

    loop = asyncio.get_event_loop()
    while True:
        start_time = time.time()
        input_coroutines = [fetch_ticker(exchanges, name) for name in exchanges]
        exchange_tickers = loop.run_until_complete(asyncio.gather(*input_coroutines, return_exceptions=True))

        count_exchange = 0

        delta = time.time() - start_time
        for tickers in exchange_tickers:
            if tickers is not None:
                count_exchange += 1

        inserted_start = time.time()
        # db_insert("ticker", [exchange_tickers], "")
        inserted_time = time.time()
        print(count_exchange, " ", delta, ' ', inserted_start - inserted_time, tickers.__str__())


async def fetch_ticker(exchanges, name):
    item = exchanges[name]

    try:
        ticker = await item.fetchTickers()
        return {name: ticker}
    finally:
        pass


def db_insert(tableName, columns, values):
    with db_conn.cursor() as db:
        db_conn.autocommit = True
        insert = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(
            sql.Identifier(tableName),
            # sql.SQL(', ').join(map(sql.Identifier, columns)),
            # sql.SQL(', ').join(map(sql.Literal, values)),
        );
        db.execute(insert)
        print("Объект " + "\"" + columns.__str__() + "\" " + "добавлен в таблицу " + tableName)


def add_exchanges_and_markets_to_db():
    exchanges = {}
    markets = {}

    for exchange_name in exchange_names:
        exchange = getattr(ccxt, exchange_name)
        exchanges[exchange_name] = exchange()
        add_exchange_to_db_if_not_exist(exchange_name)
    print("Обнаружены " + str(len(exchange_names)) + " биржи: " + ", ".join(str(x) for x in exchange_names))

    for exchange_name in exchange_names:
        markets = exchanges[exchange_name].load_markets()
        market_names = list(markets.keys())
        for mark_name in market_names:
            add_market_to_db_if_not_exist(mark_name, market_names, exchange_name)

    print("Обнаружены " + str(len(markets)) + " валютные пары: " + ", ".join(str(x) for x in exchange_names))


def add_exchange_to_db_if_not_exist(exchange_name):
    if exchange_name not in exchanges_in_db:
        db_insert('exchange', [exchange_name, ], ["name", ])


def add_market_to_db_if_not_exist(market_name, market_names, exchange_name):
    cursor.execute('SELECT id FROM exchange WHERE name LIKE %s', (exchange_name,))
    exchange_id = cursor.fetchone()[0]

    cursor.execute('SELECT name FROM market')
    if market_name not in cursor.fetchall():
        db_insert('market', [str(market_name), exchange_id], ["name", "id_exchange"])
        print(market_name)


def start_parser():
    print("=" * 100)
    print("Подготовка БД к парсингу")
    print("-" * 100)
    add_exchanges_and_markets_to_db()
    print("-" * 100)

    # saveTickers()


start_parser()
