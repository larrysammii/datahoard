from polygon import WebSocketClient, RESTClient
from polygon.websocket.models import WebSocketMessage
from typing import List
from dataclasses import dataclass
from polygon.rest.models import Agg
import config
from datetime import datetime, timedelta

# TODO:
# *** CHECK WHICH DATA I NEED TO STORE IN DB ***
#
# Pipeline for Polygon API:
#
# [] 1. Get all tickers from exchange
#          -> [] Choose whether save as csv or store in DB
# [] 2. Websocket Client: Stream live data for each ticker
#          -> [] Check data requirements, definte columns, create table in DB
# [] 3. Get last 5 years historical 1 hour data for each ticker
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

# Exchanges
NYSE = "XNYS"
NASDAQ = "XNAS"


# REST Client
client = RESTClient(api_key=config.POLYGON_API)


# Get all tickers of a given exchange.
# TODO: Define similar functions for ETFs, Structured Products, etc.
def get_stock_tickers(exchange: str) -> List[str]:
    # Polygon's API has a limit of 1000 tickers per request.
    # Need check if this works.
    stocks = []

    while True:
        tickers = client.tickers(
            market="stocks",
            exchange=exchange,
            active=True,
            limit=1000,
            type="CS",
        )
        stocks.extend(tickers)
        if len(tickers) < 1000:
            break

    return stocks


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

    ticker: str  # Required, single ticker only. Should do the job for now, just loop through all tickers.

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
    for a in client.list_aggs(
        "AAPL",
        1,
        "hour",
        "2023-11-22",
        "2023-11-22",
        limit=50000,
    ):
        aggs.append(a)
        count += 1
        print(count)

    return print(aggs)

    # print(client.list_aggs("AAPL", 1, "hour", "2023-11-22", "2023-11-22", raw=True))
