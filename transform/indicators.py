"""
Transform module for cryptocurrency data
Computes technical indicators: daily returns, moving averages, volatility
"""

import pandas as pd
import numpy as np
from typing import Optional

from utils.logger import logger, log_transform


def calculate_daily_return(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily percentage return for each coin
    
    daily_return = (price_today - price_yesterday) / price_yesterday * 100
    
    Args:
        df: DataFrame with 'price', 'symbol', 'date' columns
        
    Returns:
        DataFrame with 'daily_return' column added
    """
    df = df.copy()
    df = df.sort_values(["symbol", "date"])
    
    # Calculate return per symbol group
    df["daily_return"] = df.groupby("symbol")["price"].pct_change() * 100
    
    return df


def calculate_moving_average(
    df: pd.DataFrame, 
    window: int = 7,
    column: str = "price"
) -> pd.DataFrame:
    """
    Calculate rolling moving average
    
    Args:
        df: DataFrame with price data
        window: Rolling window size (default 7 days)
        column: Column to calculate MA on
        
    Returns:
        DataFrame with moving average column added
    """
    df = df.copy()
    df = df.sort_values(["symbol", "date"])
    
    ma_column = f"ma_{window}"
    
    df[ma_column] = df.groupby("symbol")[column].transform(
        lambda x: x.rolling(window=window, min_periods=1).mean()
    )
    
    return df


def calculate_volatility(
    df: pd.DataFrame, 
    window: int = 7
) -> pd.DataFrame:
    """
    Calculate rolling volatility (standard deviation of returns)
    
    Volatility is a key risk metric - higher = more price variation
    
    Args:
        df: DataFrame with 'daily_return' column
        window: Rolling window size (default 7 days)
        
    Returns:
        DataFrame with volatility column added
    """
    df = df.copy()
    df = df.sort_values(["symbol", "date"])
    
    volatility_column = f"volatility_{window}"
    
    # Must have daily_return calculated first
    if "daily_return" not in df.columns:
        df = calculate_daily_return(df)
    
    df[volatility_column] = df.groupby("symbol")["daily_return"].transform(
        lambda x: x.rolling(window=window, min_periods=1).std()
    )
    
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and prepare data for loading
    
    - Fill NaN values appropriately
    - Ensure correct data types
    - Remove duplicates
    
    Args:
        df: Raw DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    df = df.copy()
    
    # Fill NaN for first row of each symbol (no previous day for return)
    df["daily_return"] = df["daily_return"].fillna(0)
    df["ma_7"] = df["ma_7"].fillna(df["price"])
    df["volatility_7"] = df["volatility_7"].fillna(0)
    
    # Ensure proper types
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = df["symbol"].astype(str)
    df["price"] = df["price"].astype(float)
    df["market_cap"] = df["market_cap"].astype(float)
    df["volume"] = df["volume"].astype(float)
    df["daily_return"] = df["daily_return"].astype(float)
    df["ma_7"] = df["ma_7"].astype(float)
    df["volatility_7"] = df["volatility_7"].astype(float)
    
    # Remove any exact duplicates
    df = df.drop_duplicates(subset=["date", "symbol"], keep="last")
    
    return df


def transform_market_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full transformation pipeline for market data
    
    Applies:
    1. Daily return calculation
    2. 7-day moving average
    3. 7-day volatility
    4. Data cleaning
    
    Args:
        df: Raw extracted DataFrame
        
    Returns:
        Fully transformed DataFrame ready for loading
    """
    if df.empty:
        logger.warning("[TRANSFORM] Empty DataFrame received, skipping transformation")
        return df
    
    logger.info(f"[TRANSFORM] Starting transformation of {len(df)} records")
    
    # Apply transformations sequentially
    df = calculate_daily_return(df)
    df = calculate_moving_average(df, window=7)
    df = calculate_volatility(df, window=7)
    df = clean_data(df)
    
    # Select final columns in order
    final_columns = [
        "date", "symbol", "price", "market_cap", "volume",
        "daily_return", "ma_7", "volatility_7"
    ]
    
    df = df[final_columns]
    
    log_transform(len(df))
    
    return df
