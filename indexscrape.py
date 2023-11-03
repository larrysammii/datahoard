import requests
import wikipediaapi
import pandas as pd
import config
import psycopg2
import psycopg2.extras
import json


# Scraping all S&P 500 companies from Wikipedia.
def get_sp500():
    # Using the wikitable2json
    wikisp500 = "https://www.wikitable2json.com/api/List_of_S%26P_500_companies?table=0&lang=en&keyRows=1&cleanRef=True"

    sp500_stocks = requests.get(wikisp500).json()
    sp500_stocks = json.dumps(sp500_stocks, indent=4, sort_keys=True)
    return sp500_stocks

def parse_json():
    dd = dd.from_pandas(sp500_stocks, npartitions=1)

sp500_stocks = get_sp500()
print(sp500_stocks)

connection = psycopg2.connect(
    host=config.DB_HOST,
    database=config.DB_NAME,
    user=config.DB_USER,
    password=config.DB_PASS,
)


cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

print(sp500_stocks.key())
"""
stock_symbol = stock["Symbol"]
stock_name = stock["Security"]
stock_sector = stock["GICS Sector"]
stock_subindustry = stock["GICS Sub-Industry"]
stock_hq = stock["Headquarters Location"]
stock_dateadded = stock["Date added"]
stock_founded = stock["Founded"]

print(f"Added stock {stock_symbol} {stock_name} {stock_sector} {stock_subindustry} {stock_hq} {stock_dateadded} {stock_founded}to the database.")

"""