CREATE OR REPLACE TABLE market_news (
    id BIGINT,
    author VARCHAR,
    headline VARCHAR,
    source VARCHAR,
    summary VARCHAR,
    data_provider VARCHAR,
    `url` VARCHAR,
    symbol VARCHAR,
    sentiment DECIMAL,
    timestamp_ms BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(timestamp_ms, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'market-news',
    'properties.bootstrap.servers' = 'redpanda-1:29092,redpanda-2:29092',
    'properties.group.id' = 'test-group',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json'
);

CREATE OR REPLACE TABLE stock_prices (
    symbol VARCHAR,
    `open` FLOAT,
    high FLOAT,
    low FLOAT,
    `close` FLOAT,
    volume DECIMAL,
    trade_count FLOAT,
    vwap DECIMAL,
    provider VARCHAR,
    `timestamp` BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'stock-prices',
    'properties.bootstrap.servers' = 'redpanda-1:29092,redpanda-2:29093',
    'properties.group.id' = 'test-group',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json'
);
