CREATE TABLE market_data (
    instrument_id TEXT,
    price FLOAT,
    volume FLOAT,
    timestamp TIMESTAMP,
    vwap FLOAT,
    is_outlier BOOLEAN,
    PRIMARY KEY (instrument_id, timestamp)
);