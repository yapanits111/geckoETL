"""
Data validation utilities for Crypto ETL Pipeline
Ensures data quality before loading
"""

import pandas as pd
from typing import List, Tuple
from utils.logger import logger


def validate_required_columns(
    df: pd.DataFrame, 
    required_columns: List[str]
) -> Tuple[bool, List[str]]:
    """
    Validate that DataFrame has all required columns
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        Tuple of (is_valid, missing_columns)
    """
    missing = [col for col in required_columns if col not in df.columns]
    is_valid = len(missing) == 0
    
    if not is_valid:
        logger.warning(f"Missing columns: {missing}")
    
    return is_valid, missing


def validate_no_nulls(
    df: pd.DataFrame, 
    columns: List[str]
) -> Tuple[bool, dict]:
    """
    Validate that specified columns have no null values
    
    Args:
        df: DataFrame to validate
        columns: Columns to check for nulls
        
    Returns:
        Tuple of (is_valid, null_counts)
    """
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
    """
    Validate date range in DataFrame
    
    Args:
        df: DataFrame to validate
        date_column: Name of date column
        
    Returns:
        Tuple of (is_valid, message)
    """
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
    """
    Validate that specified columns have positive values
    
    Args:
        df: DataFrame to validate
        columns: Columns to check for positive values
        
    Returns:
        Tuple of (is_valid, negative_counts)
    """
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
    """
    Full schema validation for crypto market data
    
    Args:
        df: DataFrame to validate
        
    Returns:
        True if all validations pass
    """
    logger.info("[VALIDATION] Starting schema validation...")
    
    # Required columns for final output
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
