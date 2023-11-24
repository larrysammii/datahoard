from polygon import WebSocketClient, RESTClient
from polygon.websocket.models import WebSocketMessage
from typing import List, Dict, Tuple, cast
from dataclasses import dataclass
from polygon.rest.models import Agg
import config
from urllib3.response import HTTPResponse
from datetime import datetime, timedelta
import alpaca_trade_api as tradeapi
import psycopg2
import psycopg2.extras

# TODO:
# *** CHECK WHICH DATA I NEED TO STORE IN DB ***
#
# Pipeline for Polygon API:
#
# [x] 1. Get all tickers from exchange
#          -> [] Choose whether save as csv or store in DB
# [] 2. (DO IT IN rt_stock_price.py) Websocket Client: Stream live data for each ticker
#          -> [] Check data requirements, definte columns, create table in DB
# [x] 3. Get last 5 years historical 1 hour data for each ticker
#          -> [] Check data requirements, definte columns, create table in DB
# [] 4. Get stock financials for each ticker
#          -> [] Check data requirements, definte columns, create table in DB
# [] 5. Get ticker details v3 for each ticker
#          -> [] Check data requirements, definte columns, create table in DB
# [] 6. Get Universal Snapshot for each ticker
#          -> [] Check data requirements, definte columns, create table in DB
#
# NEXT:
# FMP API for financial statements, ratios and key metrics
# Alpha Vantage API for commodities and economic data
# TwelveData API for forex and indices?


# Polygon.io REST Client
def poly_rest() -> RESTClient:
    return RESTClient(api_key=config.POLYGON_API)


# Get exchanges' mic and name.
def exchange_mic(
    asset: str = "stocks",
    locale: str = "us",
) -> Dict[str, List[Tuple[int, str]]]:
    """
    Filter and get only exchanges, excluding TRF & SIP.
    """
    exchanges = poly_rest().get_exchanges(asset, locale)
    mic_name_dict = {}

    for exchange in exchanges:
        if exchange.type == "TRF" or exchange.type == "SIP":
            continue
        mic = exchange.mic
        name = exchange.name
        polygon_id = exchange.id
        mic_name_dict[mic] = [polygon_id, name]
    return mic_name_dict


# Find mic by name. If no name is given, return all mics.
def find_mic(name: str = None) -> str or List[str]:
    mic_name_dict = exchange_mic()
    mic_list = []

    for mic, value in mic_name_dict.items():
        if name == None:
            mic_list.append(mic)
            polygon_id = value[0]
            exchange = value[1]
            print(f"MIC Code: {mic}\nExchange: {exchange}\nPolygon ID: {polygon_id}\n")
        elif name.lower() in value[1].lower():
            mic_list.append(mic)
            polygon_id = value[0]
            exchange = value[1]
            print(f"MIC Code: {mic}\nExchange: {exchange}\nPolygon ID: {polygon_id}\n")

    if len(mic_list) == 1:
        print(mic_list[0])
        return mic_list[0]
    else:
        print(mic_list)
        return mic_list


# find_mic("ny")


# TODO: Get all ticker types.
def get_ticker_types() -> List[str]:
    pass


# Get all tickers of a given exchange.
def get_tickers_list(exchange: str, instrument: str) -> List[str]:
    """
    Polygon's REST API only returns 1000 tickers per request.
    However, it has a built-in pagination function, just append to list on each pagination.
    """
    symbols = []

    tickers = poly_rest().list_tickers(
        market="stocks",
        exchange=exchange,
        active=True,
        limit=1000,
        type=instrument,
    )

    for t in tickers:
        symbols.append(t.ticker)

    print(len(symbols))

    return symbols


# TODO: Get stock financials for each ticker
def get_financials(ticker: str) -> None:
    pass


# TODO: Get Polygon's Ticker Details v3 for each ticker
def ticker_info(ticker: str) -> None:
    pass


# TODO: Get Universal Snapshot for each ticker
def snapshot(ticker: str) -> None:
    pass


@dataclass
class HourlyAgg:
    """
    ------------------------------------------------------------------------------------------------------------------------
    Dataclass for past 5 years hourly data for a single ticker.

    All fields have default values except ticker.
    Multiplier is used to get data for different timespans:
    Example:
    multi = 1, timespan = "hour" -> 1 hour data
    multi = 1, timespan = "day" -> 1 day data

    End date default is today, start date default is 5 years ago from today.
    ------------------------------------------------------------------------------------------------------------------------
    """

    # Required, single ticker only. Should do the job for now, just loop through all tickers.
    ticker: str

    # Default value: 1 hour interval data.
    multi: int = 1
    timespan: str = "hour"

    # Today's date.
    end: str = datetime.today().strftime("%Y-%m-%d")
    # 5 years ago from today.
    start: str = (datetime.today() - timedelta(days=365 * 5)).strftime("%Y-%m-%d")


# Get historical data based on HourlyAgg dataclass.
def get_historical(
    ticker: str, multiplier: int, timespan: str, from_: str, to: str
) -> List[Agg]:
    aggs = []
    count = 0
    for bar in poly_rest().list_aggs(
        ticker=ticker,
        multiplier=multiplier,
        timespan=timespan,
        from_=from_,
        to=to,
        limit=50000,
    ):
        aggs.append(bar)
        count += 1
        print(count)

    return print(aggs)

    # print(client.list_aggs("AAPL", 1, "hour", "2023-11-22", "2023-11-22", raw=True))


# Establish connection to database through psycopg2.
def get_connection() -> psycopg2.extensions.connection:
    connection = psycopg2.connect(
        host=config.DB_HOST,
        database=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASS,
    )
    return connection


# TODO: Rewrite DB cursor function.
def REWRITE____xxxget_cursor() -> None:
    # Cursor allows Python code to execute SQL queries.
    cursor = get_connection().cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Instantiate the Alpaca API.
    api = tradeapi.REST(
        config.ALPACA_API_KEY, config.ALPACA_API_SECRET, base_url=config.ALPACA_API_URL
    )

    # List all available assets.
    assets = api.list_assets()

    # Filter out OTC stocks and non-equities,
    # then add to the database.
    for asset in assets:
        asset_exchange = getattr(asset, "exchange")
        asset_class = getattr(asset, "class")
        asset_symbol = getattr(asset, "symbol")
        asset_name = getattr(asset, "name")
        if asset_class == "us_equity" and asset_exchange != "OTC":
            cursor.execute(
                """
                INSERT INTO stock (symbol, stock_name, exchange, stock_type, in_etf)
                SELECT %s, %s, %s, %s, %s
                WHERE NOT EXISTS (SELECT symbol FROM stock WHERE symbol = %s);
                """,
                (
                    asset_symbol,
                    asset_name,
                    asset_exchange,
                    asset_class,
                    False,
                    asset_symbol,
                ),
            )
            print(
                f"Added stock {asset.symbol} {asset.name} {asset.exchange} to the database."
            )

    get_connection().commit()
