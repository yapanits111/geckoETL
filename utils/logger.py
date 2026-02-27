"""
Logging configuration for Crypto ETL Pipeline
Provides structured logging with timestamps
"""

import logging
import sys
from datetime import datetime


def setup_logger(name: str = "crypto_etl") -> logging.Logger:
    """
    Set up a configured logger instance
    
    Args:
        name: Logger name
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.INFO)
    
    # Console handler with formatting
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Professional log format
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    
    return logger


# Create default logger instance
logger = setup_logger()


def log_pipeline_start():
    """Log pipeline start"""
    logger.info("=" * 60)
    logger.info("CRYPTO ETL PIPELINE STARTED")
    logger.info(f"Run timestamp: {datetime.utcnow().isoformat()}")
    logger.info("=" * 60)


def log_pipeline_end(success: bool, records_processed: int = 0):
    """Log pipeline completion"""
    logger.info("=" * 60)
    if success:
        logger.info(f"PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Records processed: {records_processed}")
    else:
        logger.error("PIPELINE FAILED")
    logger.info("=" * 60)


def log_extract(coin: str, records: int):
    """Log extraction results"""
    logger.info(f"[EXTRACT] {coin.upper()}: {records} records fetched")


def log_transform(records: int):
    """Log transformation results"""
    logger.info(f"[TRANSFORM] Processed {records} records with indicators")


def log_load(inserted: int, updated: int):
    """Log load results"""
    logger.info(f"[LOAD] Inserted: {inserted}, Updated: {updated}")
