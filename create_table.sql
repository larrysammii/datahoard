CREATE TABLE stock (
    id SERIAL PRIMARY KEY,
    symbol TEXT UNIQUE NOT NULL,
    stock_name TEXT,
    exchange TEXT NOT NULL,
    stock_type TEXT NOT NULL,
    in_index TEXT NOT NULL,
    in_etf TEXT NOT NULL,
    sector TEXT NOT NULL,
    subsector TEXT NOT NULL
);
CREATE TABLE company (
    id SERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL,
    company_name TEXT,
    industry TEXT NOT NULL,
    hq_loc TEXT,
    founded TEXT,
    date_added DATE,
    CONSTRAINT fk_stock FOREIGN KEY (stock_id) REFERENCES stock(id)
);
CREATE TABLE stock_price (
    stock_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    date_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (stock_id, date_time),
    CONSTRAINT fk_stock FOREIGN KEY (stock_id) REFERENCES stock(id),
    CONSTRAINT fk_symbol FOREIGN KEY (symbol) REFERENCES stock(symbol)
);
CREATE TABLE fin_statement (
    company_id INTEGER NOT NULL,
    company_name TEXT,
    statement_type TEXT NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    CONSTRAINT fk_company FOREIGN KEY (company_id) REFERENCES company(id),
    CONSTRAINT fk_company_name FOREIGN KEY (company_name) REFERENCES company(company_name)
);
CREATE TABLE etf_info (
    symbol TEXT,
    url TEXT,
    issuer TEXT inception TEXT,
    index_tracked TEXT,
    last_updated TEXT,
    category TEXT,
    asset_class TEXT,
    segment TEXT,
    focus TEXT,
    niche TEXT,
    strategy TEXT,
    weight_scheme TEXT,
    CONSTRAINT fk_symbol FOREIGN KEY (symbol) REFERENCES stock(symbol)
);
CREATE TABLE etf_info_num (
    symbol TEXT,
    expense_ratio DOUBLE PRECISION,
    price DOUBLE PRECISION,
    change DOUBLE PRECISION,
    pe_ratio DOUBLE PRECISION,
    52_week_high DOUBLE PRECISION,
    52_week_low DOUBLE PRECISION,
    aum DOUBLE PRECISION,
    shares DOUBLE PRECISION,
    CONSTRAINT fk_symbol FOREIGN KEY (symbol) REFERENCES stock(symbol)
);
CREATE TABLE etf_holdings (
    etf_symbol TEXT,
    stock_symbol TEXT,
    holding TEXT,
    shares DOUBLE PRECISION,
    url TEXT,
    CONSTRAINT fk_stock_symbol FOREIGN KEY (stock_symbol) REFERENCES stock(symbol),
    CONSTRAINT fk_etf FOREIGN KEY (etf_symbol) REFERENCES stock(symbol)
);
--------------
INSERT INTO etf (symbol)
SELECT symbol
FROM stock;
--------------
INSERT INTO etf_holdings (symbol)
SELECT symbol
FROM stock;
--------------
CREATE INDEX ON stock_price (stock_id, symbol, date_time DESC);
--------------
CREATE INDEX ON fin_statement (
    company_name,
    statement_type,
    period_start,
    period_end
);