import config
import alpaca_trade_api as tradeapi
import psycopg2
import psycopg2.extras

# Establish connection to database through psycopg2.
# I know you can have convoluted solutions like dotenv, but I'm not a fan of that.
# I'd rather have a config.py file that I can gitignore and keep my credentials there.
connection = psycopg2.connect(
    host=config.DB_HOST,
    database=config.DB_NAME,
    user=config.DB_USER,
    password=config.DB_PASS,
)

# Cursor allows Python code to execute SQL queries.
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

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

connection.commit()
