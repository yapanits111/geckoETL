import logging
import sys
from datetime import datetime


def setup_logger(name: str = "crypto_etl") -> logging.Logger:
    logger = logging.getLogger(name)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.INFO)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    
    return logger


logger = setup_logger()


def log_pipeline_start():
    logger.info("=" * 60)
    logger.info("CRYPTO ETL PIPELINE STARTED")
    logger.info(f"Run timestamp: {datetime.utcnow().isoformat()}")
    logger.info("=" * 60)


def log_pipeline_end(success: bool, records_processed: int = 0):
    logger.info("=" * 60)
    if success:
        logger.info(f"PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Records processed: {records_processed}")
    else:
        logger.error("PIPELINE FAILED")
    logger.info("=" * 60)


def log_extract(coin: str, records: int):
    logger.info(f"[EXTRACT] {coin.upper()}: {records} records fetched")


def log_transform(records: int):
    logger.info(f"[TRANSFORM] Processed {records} records with indicators")


def log_load(inserted: int, updated: int):
    logger.info(f"[LOAD] Inserted: {inserted}, Updated: {updated}")
