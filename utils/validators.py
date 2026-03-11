import pandas as pd
from typing import List, Tuple
from utils.logger import logger


def validate_required_columns(
    df: pd.DataFrame, 
    required_columns: List[str]
) -> Tuple[bool, List[str]]:
    """Check that DataFrame has all required columns."""
    missing = [col for col in required_columns if col not in df.columns]
    is_valid = len(missing) == 0
    
    if not is_valid:
        logger.warning(f"Missing columns: {missing}")
    
    return is_valid, missing


def validate_no_nulls(
    df: pd.DataFrame, 
    columns: List[str]
) -> Tuple[bool, dict]:
    """Check that specified columns have no null values."""
    null_counts = {}
    for col in columns:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                null_counts[col] = null_count
    
    is_valid = len(null_counts) == 0
    
    if not is_valid:
        logger.warning(f"Null values found: {null_counts}")
    
    return is_valid, null_counts


def validate_date_range(
    df: pd.DataFrame, 
    date_column: str = "date"
) -> Tuple[bool, str]:
    """Check date range in DataFrame."""
    if date_column not in df.columns:
        return False, f"Date column '{date_column}' not found"
    
    min_date = df[date_column].min()
    max_date = df[date_column].max()
    
    message = f"Date range: {min_date} to {max_date}"
    logger.info(f"[VALIDATION] {message}")
    
    return True, message


def validate_positive_values(
    df: pd.DataFrame, 
    columns: List[str]
) -> Tuple[bool, dict]:
    """Check that specified columns have positive values."""
    negative_counts = {}
    for col in columns:
        if col in df.columns:
            negative_count = (df[col] < 0).sum()
            if negative_count > 0:
                negative_counts[col] = negative_count
    
    is_valid = len(negative_counts) == 0
    
    if not is_valid:
        logger.warning(f"Negative values found: {negative_counts}")
    
    return is_valid, negative_counts


def validate_schema(df: pd.DataFrame) -> bool:
    """Run full schema validation for crypto market data."""
    logger.info("[VALIDATION] Starting schema validation...")
    
    required = [
        "date", "symbol", "price", "market_cap", 
        "volume", "daily_return", "ma_7", "volatility_7"
    ]
    
    # Check required columns
    has_columns, _ = validate_required_columns(df, required)
    if not has_columns:
        return False
    
    # Check for nulls in critical columns
    critical_columns = ["date", "symbol", "price"]
    no_nulls, _ = validate_no_nulls(df, critical_columns)
    if not no_nulls:
        return False
    
    # Check date range
    validate_date_range(df)
    
    # Check positive values for price/volume
    positive_columns = ["price", "market_cap", "volume"]
    has_positive, _ = validate_positive_values(df, positive_columns)
    
    logger.info(f"[VALIDATION] Schema validation {'PASSED' if has_positive else 'FAILED'}")
    return has_positive
