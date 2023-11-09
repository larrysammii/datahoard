from etfpy import ETF, get_available_etfs_list
import json
import os
import io
import polars as pl
import pandas as pd
import time
import config
import psycopg2
import psycopg2.extras
from typing import List

# List all available ETFs, save as json.

connection = psycopg2.connect(
    host=config.DB_HOST,
    database=config.DB_NAME,
    user=config.DB_USER,
    password=config.DB_PASS,
)

cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

etfs = get_available_etfs_list()

# Before inserting to database, make sure you have all the columns created


def etf_basic(etfs: List[str]) -> None:
    for etf in etfs:
        etf_info = ETF(etf).to_tabular().info

        cursor.execute(
            """
            INSERT INTO etf_info (symbol, url, issuer, inception, index_tracked, last_updated, category, asset_class, segment, focus, niche, strategy, weight_scheme)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                etf,
                etf_info["URL"],
                etf_info["Issuer"],
                etf_info["Inception"],
                etf_info["Index Tracked"],
                etf_info["Last Updated"],
                etf_info["Category"],
                etf_info["Asset Class"],
                etf_info["Segment"],
                etf_info["Focus"],
                etf_info["Niche"],
                etf_info["Strategy"],
                etf_info["Weighting Scheme"],
            ),
        )
        print(f"Added ETF {etf} to the database.")

    connection.commit()


def etf_basic_numeric(etfs: List[str]) -> None:
    for etf in etfs:
        etf_nums = ETF(etf).to_tabular().info_numeric

        cursor.execute(
            """
            INSERT INTO etf_info_num (symbol, expense_ratio, price, change, pe_ratio, high_52week, low_52week, aum, shares)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                etf,
                etf_nums["Expense Ratio (%)"],
                etf_nums["Price ($)"],
                etf_nums["Change ($)"],
                etf_nums["P/E Ratio"],
                etf_nums["52 Week Hi ($)"],
                etf_nums["52 Week Lo ($)"],
                etf_nums["AUM ($)"],
                etf_nums["Shares"],
            ),
        )
        print(f"Added ETF {etf}'s numeric data to the database.")

    connection.commit()


"""
fixed income
dont buy hk china`
    when retire
    now trend? except retire u can have many buckets(life goals), different investments to support each bucket
    """
