import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd
from datetime import datetime
from typing import Tuple

from config import Config
from utils.logger import logger, log_load


def get_connection():
    """Create PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            dbname=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD
        )
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
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol, date)
    );
    
    -- Create index for faster queries by date
    CREATE INDEX IF NOT EXISTS idx_crypto_market_date 
    ON crypto_market_daily(date);
    
    -- Create index for faster queries by symbol
    CREATE INDEX IF NOT EXISTS idx_crypto_market_symbol 
    ON crypto_market_daily(symbol);
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
        conn.commit()
        logger.info("[LOAD] Table crypto_market_daily verified/created")
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
        (date, symbol, price, market_cap, volume, daily_return, ma_7, volatility_7, updated_at)
    VALUES %s
    ON CONFLICT (symbol, date) 
    DO UPDATE SET
        price = EXCLUDED.price,
        market_cap = EXCLUDED.market_cap,
        volume = EXCLUDED.volume,
        daily_return = EXCLUDED.daily_return,
        ma_7 = EXCLUDED.ma_7,
        volatility_7 = EXCLUDED.volatility_7,
        updated_at = CURRENT_TIMESTAMP
    """
    
    # Prepare data tuples
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
            datetime.utcnow()
        ))
    
    try:
        with conn.cursor() as cursor:
            # Get count before insert
            cursor.execute("SELECT COUNT(*) FROM crypto_market_daily")
            count_before = cursor.fetchone()[0]
            
            # Execute upsert
            execute_values(cursor, upsert_sql, records)
            
            # Get count after insert
            cursor.execute("SELECT COUNT(*) FROM crypto_market_daily")
            count_after = cursor.fetchone()[0]
        
        conn.commit()
        
        # Calculate inserted vs updated
        inserted = count_after - count_before
        updated = len(records) - inserted
        
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
    query = f"""
    SELECT * FROM crypto_market_daily 
    ORDER BY date DESC, symbol 
    LIMIT {limit}
    """
    
    conn = None
    try:
        conn = get_connection()
        df = pd.read_sql(query, conn)
        return df
    finally:
        if conn:
            conn.close()
