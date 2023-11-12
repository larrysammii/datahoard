from etfpy import ETF, get_available_etfs_list
from typing import List
import config
import psycopg2
import psycopg2.extras
import time

# Connect to the database, configs are stored in your own config.py.
connection = psycopg2.connect(
    host=config.DB_HOST,
    database=config.DB_NAME,
    user=config.DB_USER,
    password=config.DB_PASS,
)


# Create a cursor to execute SQL commands.
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

# Get all the available ETFs from etfpy.
etfs = get_available_etfs_list()

"""

Before inserting to database, make sure you have all the columns created

"""


# Get all the basic string info of an ETF
def etf_basic(etfs: List[str]) -> None:
    for etf in etfs:
        start = time.time()
        etf_info = ETF(etf).to_tabular().info
        etf_info.set_index("metric", inplace=True)
        etf_info = etf_info.transpose()

        cursor.execute(
            """
            INSERT INTO etf_info (symbol, url, issuer, inception, index_tracked, last_updated, category, asset_class, segment, focus, niche, strategy, weight_scheme)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                etf,
                etf_info.get("Url").iloc[0],
                etf_info.get("Issuer").iloc[0],
                etf_info.get("Inception").iloc[0],
                etf_info.get("Index Tracked").iloc[0],
                etf_info.get("Last Updated").iloc[0],
                etf_info.get("Category").iloc[0],
                etf_info.get("Asset Class").iloc[0],
                etf_info.get("Segment").iloc[0],
                etf_info.get("Focus").iloc[0],
                etf_info.get("Niche").iloc[0],
                etf_info.get("Strategy").iloc[0],
                etf_info.get("Weighting Scheme").iloc[0],
            ),
        )
        connection.commit()
        end = time.time()
        elapsed = round(end - start, 2)
        print(f"Added ETF {etf} to the database. Time taken: {elapsed} seconds.")


# Get all the basic numeric info of an ETF
def etf_basic_numeric(etfs: List[str]) -> None:
    for etf in etfs:
        start = time.time()

        etf_nums = ETF(etf).to_tabular().info_numeric
        etf_nums.set_index("metric", inplace=True)
        etf_nums = etf_nums.transpose()

        cursor.execute(
            """
            INSERT INTO etf_info_num (symbol, expense_ratio, price, change, pe_ratio, high_52week, low_52week, aum, shares)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                etf,
                etf_nums.get("Expense Ratio (%)").iloc[0],
                etf_nums.get("Price ($)").iloc[0],
                etf_nums.get("Change ($)").iloc[0],
                etf_nums.get("P/E Ratio").iloc[0],
                etf_nums.get("52 Week Hi ($)").iloc[0],
                etf_nums.get("52 Week Lo ($)").iloc[0],
                etf_nums.get("AUM ($)").iloc[0],
                etf_nums.get("Shares").iloc[0],
            ),
        )
        end = time.time()
        connection.commit()
        elapsed = round(end - start, 2)

        print(
            f"Added ETF {etf}'s numeric data to the database. Time taken: {elapsed} seconds."
        )


# Get all the holdings of an ETF
def etf_holdings(etfs: List[str]) -> None:
    # Get each ETF's holding information, then add to the database.
    for etf in etfs:
        start = time.time()
        etf_symbol = etf
        holdings = ETF(etf).to_tabular().holdings

        asset_symbol = holdings.get("symbol").iloc[0]
        asset_name = holdings.get("holding").iloc[0]
        asset_share = holdings.get("%_share").iloc[0]
        asset_url = holdings.get("url").iloc[0]

        cursor.execute(
            """
                INSERT INTO etf_holdings (etf_symbol, stock_symbol, holding, shares, url)
                VALUE (%s, %s, %s, %s, %s);
                """,
            (
                etf_symbol,
                asset_symbol,
                asset_name,
                asset_share,
                asset_url,
            ),
        )

        end = time.time()
        elapsed = round(end - start, 2)

        connection.commit()

        print(
            f"Added ETF: {etf_symbol}'s {asset_name}({asset_symbol}) into the database. Time taken: {elapsed} seconds."
        )


# etf_basic(etfs)
# etf_basic_numeric(etfs)
# etf_holdings(etfs)


# etf_info = ETF("SPY").to_tabular().holdings
# etf_info.set_index("metric", inplace=True)
# etf_info = etf_info.transpose()
# print(etf_info.get("%_share").iloc[0])