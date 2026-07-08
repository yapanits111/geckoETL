import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from datetime import datetime, timezone
from typing import Tuple

from config import Config
from utils.logger import logger, log_load


def get_connection():
    """Create PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(Config.get_db_url())
        logger.info("[LOAD] Database connection established")
        return conn
    except Exception as e:
        logger.error(f"[LOAD] Database connection failed: {str(e)}")
        raise


def create_table_if_not_exists(conn) -> bool:
    """Create the crypto_market_daily table if it doesn't exist."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS crypto_market_daily (
        date DATE NOT NULL,
        symbol VARCHAR(50) NOT NULL,
        price FLOAT,
        market_cap FLOAT,
        volume FLOAT,
        daily_return FLOAT,
        ma_7 FLOAT,
        volatility_7 FLOAT,
        volatility FLOAT,
        price_change_pct FLOAT,
        is_bullish BOOLEAN,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol, date)
    );

    ALTER TABLE crypto_market_daily
    ADD COLUMN IF NOT EXISTS volatility FLOAT;

    ALTER TABLE crypto_market_daily
    ADD COLUMN IF NOT EXISTS price_change_pct FLOAT;

    ALTER TABLE crypto_market_daily
    ADD COLUMN IF NOT EXISTS is_bullish BOOLEAN;
    
    -- Create index for faster queries by date
    CREATE INDEX IF NOT EXISTS idx_crypto_market_date 
    ON crypto_market_daily(date);
    
    -- Create index for faster queries by symbol
    CREATE INDEX IF NOT EXISTS idx_crypto_market_symbol 
    ON crypto_market_daily(symbol);

    -- Compatibility view for dashboard/reporting with requested column names
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
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
        conn.commit()
        logger.info("[LOAD] Table crypto_market_daily and view crypto_prices verified/created")
        return True
    except Exception as e:
        logger.error(f"[LOAD] Table creation failed: {str(e)}")
        conn.rollback()
        return False


def upsert_data(conn, df: pd.DataFrame) -> Tuple[int, int]:
    """UPSERT data into crypto_market_daily (idempotent)."""
    if df.empty:
        logger.warning("[LOAD] Empty DataFrame, nothing to load")
        return 0, 0
    
    upsert_sql = """
    INSERT INTO crypto_market_daily 
        (
            date, symbol, price, market_cap, volume, daily_return,
            ma_7, volatility_7, volatility, price_change_pct, is_bullish, updated_at
        )
    VALUES %s
    ON CONFLICT (symbol, date) 
    DO UPDATE SET
        price = EXCLUDED.price,
        market_cap = EXCLUDED.market_cap,
        volume = EXCLUDED.volume,
        daily_return = EXCLUDED.daily_return,
        ma_7 = EXCLUDED.ma_7,
        volatility_7 = EXCLUDED.volatility_7,
        volatility = EXCLUDED.volatility,
        price_change_pct = EXCLUDED.price_change_pct,
        is_bullish = EXCLUDED.is_bullish,
        updated_at = CURRENT_TIMESTAMP
    -- (xmax = 0) is true only for freshly INSERTed rows, false for rows that
    -- hit the ON CONFLICT UPDATE path. This gives exact insert/update counts
    -- for THIS batch, unlike a table-wide COUNT(*) which is wrong under any
    -- concurrent writer.
    RETURNING (xmax = 0) AS inserted
    """

    # Prepare data tuples
    now = datetime.now(timezone.utc)
    records = []
    for _, row in df.iterrows():
        records.append((
            row["date"],
            row["symbol"],
            row["price"],
            row["market_cap"],
            row["volume"],
            row["daily_return"],
            row["ma_7"],
            row["volatility_7"],
            row["volatility"],
            row["price_change_pct"],
            row["is_bullish"],
            now
        ))

    try:
        with conn.cursor() as cursor:
            results = execute_values(cursor, upsert_sql, records, fetch=True)

        conn.commit()

        inserted = sum(1 for r in results if r[0])
        updated = len(results) - inserted

        log_load(inserted, updated)

        return inserted, updated
        
    except Exception as e:
        logger.error(f"[LOAD] Upsert failed: {str(e)}")
        conn.rollback()
        raise


def load_to_postgres(df: pd.DataFrame) -> Tuple[int, int]:
    """Main load function - connects, creates table, and upserts data."""
    logger.info(f"[LOAD] Starting load of {len(df)} records")
    
    conn = None
    try:
        conn = get_connection()
        create_table_if_not_exists(conn)
        inserted, updated = upsert_data(conn, df)
        return inserted, updated
        
    finally:
        if conn:
            conn.close()
            logger.info("[LOAD] Database connection closed")


def get_latest_records(limit: int = 10) -> pd.DataFrame:
    """Fetch latest records from database."""
    # limit is coerced to int and passed as a bound parameter rather than
    # interpolated into the SQL string, so it can never be an injection vector.
    query = """
    SELECT * FROM crypto_market_daily
    ORDER BY date DESC, symbol
    LIMIT %s
    """

    conn = None
    try:
        conn = get_connection()
        df = pd.read_sql(query, conn, params=(int(limit),))
        return df
    finally:
        if conn:
            conn.close()
