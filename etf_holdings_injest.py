from etfpy import get_available_etfs_list
import json
import os
import config
import psycopg2
import psycopg2.extras


def holdings2db() -> None:
    # List all available ETFs.
    etfs = get_available_etfs_list()

    connection = psycopg2.connect(
        host=config.DB_HOST,
        database=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASS,
    )

    # Cursor allows Python code to execute SQL queries.
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Get each ETF's holding information, then add to the database.
    for etf in etfs:
        etf_symbol = etf.symbol
        holdings = etf.holdings
        for holding in holdings:
            asset_symbol = holding.get("Symbol")
            asset_name = holding.get("Holding")
            asset_share = float(holding.get("Share")[:-1]) / 100

            cursor.execute(
                """
                INSERT INTO etf_holdings (stock_symbol, stock_name, etf_symbol, share)
                SELECT %s, %s, %s, %s
                WHERE NOT EXISTS (SELECT stock_symbol FROM etf_holdings WHERE stock_symbol = %s AND etf_symbol = %s AND share != %s);
                """,
                (
                    asset_symbol,
                    asset_name,
                    etf_symbol,
                    asset_share,
                    asset_symbol,
                    etf_symbol,
                    asset_share,
                ),
            )

            print(
                f"Added ETF: {etf_symbol}'s {asset_name}({asset_symbol}) into the database."
            )

    connection.commit()
