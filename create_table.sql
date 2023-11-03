CREATE TABLE stock (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    exchange TEXT NOT NULL,
    is_etf BOOLEAN
);
CREATE TABLE etf_holding (
    etf_id INTEGER NOT NULL,
    holding_id INTEGER NOT NULL,
    date_time DATE NOT NULL,
    shares NUMERIC,
    weight NUMERIC,
    PRIMARY KEY (etf_id, holding_id, date_time),
    CONSTRAINT fkey_etf FOREIGN KEY (etf_id) REFERENCES stock (id),
    CONSTRAINT fkey_holding FOREIGN KEY (holding_id) REFERENCES stock (id)
);
CREATE TABLE stock_price (
    stock_id INTEGER NOT NULL,
    date_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC NOT NULL,
    PRIMARY KEY (stock_id, date_time),
    CONSTRAINT fkey_stock FOREIGN KEY (stock_id) REFERENCES stock (id)
);
CREATE INDEX ON stock_price (stock_id, date_time DESC);