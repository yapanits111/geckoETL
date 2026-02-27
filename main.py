"""
Daily Cryptocurrency Market Data Pipeline
=========================================

Main orchestrator for the ETL pipeline.
Extracts data from CoinGecko, transforms with technical indicators,
and loads to PostgreSQL using UPSERT for idempotency.

Author: Daniel Peñero
Date: February 2026
"""

import sys
import os
from datetime import datetime

from config import Config
from extract.coingecko import extract_all_coins
from transform.indicators import transform_market_data
from load.postgres import load_to_postgres
from utils.logger import logger, log_pipeline_start, log_pipeline_end
from utils.validators import validate_schema


# Data output directory
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def save_to_csv(df, filename: str, stage: str):
    """
    Save DataFrame to CSV for easy viewing
    
    Args:
        df: DataFrame to save
        filename: Output filename
        stage: Pipeline stage (raw/transformed)
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    filepath = os.path.join(DATA_DIR, filename)
    df.to_csv(filepath, index=False)
    logger.info(f"[{stage.upper()}] Saved {len(df)} records to {filename}")


def run_pipeline() -> bool:
    """
    Execute the full ETL pipeline
    
    Returns:
        True if pipeline completed successfully
    """
    log_pipeline_start()
    
    total_records = 0
    
    try:
        # ========== EXTRACT ==========
        logger.info("[PIPELINE] Step 1/3: EXTRACT")
        raw_data = extract_all_coins()
        
        if raw_data.empty:
            logger.error("[PIPELINE] Extraction returned no data. Aborting.")
            log_pipeline_end(success=False)
            return False
        
        logger.info(f"[PIPELINE] Extracted {len(raw_data)} raw records")
        
        # Save raw data to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_to_csv(raw_data, f"raw_data_{timestamp}.csv", "extract")
        save_to_csv(raw_data, "raw_data_latest.csv", "extract")
        
        # ========== TRANSFORM ==========
        logger.info("[PIPELINE] Step 2/3: TRANSFORM")
        transformed_data = transform_market_data(raw_data)
        
        if transformed_data.empty:
            logger.error("[PIPELINE] Transformation returned no data. Aborting.")
            log_pipeline_end(success=False)
            return False
        
        # Validate schema before loading
        if not validate_schema(transformed_data):
            logger.error("[PIPELINE] Schema validation failed. Aborting.")
            log_pipeline_end(success=False)
            return False
        
        logger.info(f"[PIPELINE] Transformed {len(transformed_data)} records")
        
        # Save transformed data to CSV
        save_to_csv(transformed_data, f"transformed_data_{timestamp}.csv", "transform")
        save_to_csv(transformed_data, "transformed_data_latest.csv", "transform")
        
        # ========== LOAD ==========
        logger.info("[PIPELINE] Step 3/3: LOAD")
        inserted, updated = load_to_postgres(transformed_data)
        
        total_records = inserted + updated
        logger.info(f"[PIPELINE] Loaded {total_records} records (inserted: {inserted}, updated: {updated})")
        
        log_pipeline_end(success=True, records_processed=total_records)
        return True
        
    except Exception as e:
        logger.error(f"[PIPELINE] Pipeline failed with error: {str(e)}")
        log_pipeline_end(success=False)
        return False


def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("DAILY CRYPTOCURRENCY MARKET DATA PIPELINE")
    logger.info(f"Coins: {Config.COINS}")
    logger.info(f"History: {Config.HISTORY_DAYS} days")
    logger.info("=" * 60)
    
    # Validate configuration
    if not Config.validate():
        logger.error("Invalid configuration. Check environment variables.")
        sys.exit(1)
    
    # Run the pipeline
    success = run_pipeline()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
