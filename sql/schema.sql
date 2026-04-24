-- Main table for daily cryptocurrency market data
CREATE TABLE IF NOT EXISTS crypto_market_daily (
    -- Primary identifiers
    date DATE NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    
    -- Raw market data
    price FLOAT,
    market_cap FLOAT,
    volume FLOAT,
    
    -- Computed indicators
    daily_return FLOAT,      -- Daily percentage return
    ma_7 FLOAT,              -- 7-day moving average of price
    volatility_7 FLOAT,      -- 7-day rolling volatility (std dev of returns)
    volatility FLOAT,        -- 7-day rolling volatility (std dev of price)
    price_change_pct FLOAT,  -- Day-over-day percentage price change
    is_bullish BOOLEAN,      -- True when daily_return > 0
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Composite primary key enables UPSERT
    PRIMARY KEY (symbol, date)
);

-- Index for date-based queries 
CREATE INDEX IF NOT EXISTS idx_crypto_market_date 
ON crypto_market_daily(date);

-- Index for symbol-based queries 
CREATE INDEX IF NOT EXISTS idx_crypto_market_symbol 
ON crypto_market_daily(symbol);

-- Index for recent data queries
CREATE INDEX IF NOT EXISTS idx_crypto_market_date_desc 
ON crypto_market_daily(date DESC);

-- Comments for documentation
COMMENT ON TABLE crypto_market_daily IS 'Daily cryptocurrency market data with technical indicators';
COMMENT ON COLUMN crypto_market_daily.daily_return IS 'Percentage change from previous day';
COMMENT ON COLUMN crypto_market_daily.ma_7 IS '7-day simple moving average of price';
COMMENT ON COLUMN crypto_market_daily.volatility_7 IS '7-day rolling standard deviation of returns';
COMMENT ON COLUMN crypto_market_daily.volatility IS '7-day rolling standard deviation of price';
COMMENT ON COLUMN crypto_market_daily.price_change_pct IS 'Day-over-day price change percentage';
COMMENT ON COLUMN crypto_market_daily.is_bullish IS 'Whether daily return is positive';

-- Compatibility view for dashboard/reporting outputs
CREATE OR REPLACE VIEW crypto_prices AS
SELECT
    symbol,
    date,
    price,
    daily_return,
    ma_7 AS "7day_avg",
    volatility,
    is_bullish
FROM crypto_market_daily;
