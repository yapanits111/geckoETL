-- Crypto ETL Pipeline - Database Schema
-- PostgreSQL table definition for crypto_market_daily

-- Drop table if exists (use with caution)
-- DROP TABLE IF EXISTS crypto_market_daily;

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
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Composite primary key enables UPSERT
    PRIMARY KEY (symbol, date)
);

-- Index for date-based queries (e.g., "get all data for today")
CREATE INDEX IF NOT EXISTS idx_crypto_market_date 
ON crypto_market_daily(date);

-- Index for symbol-based queries (e.g., "get all BTC history")
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
