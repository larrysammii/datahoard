from etfpy import load_etf, get_available_etfs_list
import config
import psycopg2
import psycopg2.extras
import pandas as pd

connection = psycopg2.connect(
    host=config.DB_HOST,
    database=config.DB_NAME,
    user=config.DB_USER,
    password=config.DB_PASS,
)

# Cursor allows Python code to execute SQL queries.
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

# List all available ETFs, save as json.
etfs = get_available_etfs_list()

for etf in etfs:
    etf = load_etf(etf)
    etf = etf.info
    etf_symbol = etf["Symbol"]

    cursor.execute(
        """
        UPDATE stock
        SET in_etf = True 
        WHERE symbol = %s
        """,
        (etf_symbol,),
    )

    asset_class = etf.get("Asset Class", None)
    asset_class_size = etf.get("Asset Class Size", None)
    asset_class_style = etf.get("Asset Class Style", None)
    aum = etf.get("AUM", None)
    category = etf.get("Category:", None)
    expense_ratio = etf.get("Expense Ratio", None)
    focus = etf.get("Focus", None)
    inception = etf.get("Inception", None)
    last_updated = etf.get("Last Updated:", None)
    niche = etf.get("Niche", None)
    pe = etf.get("P/E Ratio", None)
    pe_cat_avg = pe.get("ETF Database Category Average", None)
    pe_factset_seg_avg = pe.get("FactSet Segment Average", None)
    pe_ratio = pe.get(f"{etf_symbol}", None)
    segment = etf.get("Segment", None)
    shares = etf.get("Shares", None)
    weight_scheme = etf.get("Weighting Scheme", None)

    cursor.execute(
        """  
        INSERT INTO etf (
            symbol,  
            aum, 
            asset_class, 
            asset_class_size, 
            asset_class_style, 
            category, 
            expense_ratio, 
            focus, 
            inception, 
            last_updated,
            pe_cat_avg,
            pe_factset_seg_avg,
            pe_ratio, 
            niche, 
            segment, 
            shares, 
            weight_scheme
        )
        SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        """,
        (
            etf_symbol,
            aum,
            asset_class,
            asset_class_size,
            asset_class_style,
            category,
            expense_ratio,
            focus,
            inception,
            last_updated,
            pe_cat_avg,
            pe_factset_seg_avg,
            pe_ratio,
            niche,
            segment,
            shares,
            weight_scheme,
        ),
    )
    print(f"Added stock {etf_symbol} to the ETF database.")

connection.commit()
