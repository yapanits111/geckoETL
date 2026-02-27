"""
CoinGecko API extraction module
Fetches cryptocurrency market data with retry logic
"""

import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd

from config import Config
from utils.logger import logger, log_extract


def fetch_with_retry(
    url: str, 
    params: Optional[Dict] = None,
    max_retries: int = None,
    retry_delay: int = None
) -> Optional[Dict]:
    """
    Fetch data from URL with retry logic and exponential backoff
    
    Args:
        url: API endpoint URL
        params: Query parameters
        max_retries: Maximum retry attempts
        retry_delay: Initial delay between retries (seconds)
        
    Returns:
        JSON response data or None if failed
    """
    max_retries = max_retries or Config.MAX_RETRIES
    retry_delay = retry_delay or Config.RETRY_DELAY
    
    for attempt in range(max_retries):
        try:
            logger.info(f"[EXTRACT] Fetching: {url} (attempt {attempt + 1})")
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"[EXTRACT] Attempt {attempt + 1} failed: {str(e)}")
            
            if attempt < max_retries - 1:
                sleep_time = retry_delay * (2 ** attempt)  # Exponential backoff
                logger.info(f"[EXTRACT] Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                logger.error(f"[EXTRACT] All {max_retries} attempts failed for {url}")
                return None
    
    return None


def extract_market_chart(
    coin_id: str, 
    days: int = None,
    vs_currency: str = None
) -> Optional[pd.DataFrame]:
    """
    Extract historical market data for a cryptocurrency
    
    Args:
        coin_id: CoinGecko coin ID (e.g., 'bitcoin', 'ethereum')
        days: Number of days of historical data
        vs_currency: Currency for price conversion
        
    Returns:
        DataFrame with date, price, market_cap, volume columns
    """
    days = days or Config.HISTORY_DAYS
    vs_currency = vs_currency or Config.VS_CURRENCY
    
    url = f"{Config.COINGECKO_BASE_URL}/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": vs_currency,
        "days": days,
        "interval": "daily"
    }
    
    data = fetch_with_retry(url, params)
    
    if data is None:
        logger.error(f"[EXTRACT] Failed to fetch data for {coin_id}")
        return None
    
    try:
        # Parse prices, market_caps, and volumes
        prices = data.get("prices", [])
        market_caps = data.get("market_caps", [])
        volumes = data.get("total_volumes", [])
        
        if not prices:
            logger.warning(f"[EXTRACT] No price data returned for {coin_id}")
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame({
            "timestamp": [p[0] for p in prices],
            "price": [p[1] for p in prices],
            "market_cap": [m[1] for m in market_caps] if market_caps else [None] * len(prices),
            "volume": [v[1] for v in volumes] if volumes else [None] * len(prices)
        })
        
        # Convert timestamp to date
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.date
        df["symbol"] = coin_id
        
        # Drop timestamp column and duplicates (keep last value per day)
        df = df.drop(columns=["timestamp"])
        df = df.drop_duplicates(subset=["date", "symbol"], keep="last")
        
        # Reorder columns
        df = df[["date", "symbol", "price", "market_cap", "volume"]]
        
        log_extract(coin_id, len(df))
        
        return df
        
    except Exception as e:
        logger.error(f"[EXTRACT] Error parsing data for {coin_id}: {str(e)}")
        return None


def extract_all_coins(coins: List[str] = None) -> pd.DataFrame:
    """
    Extract market data for all configured coins
    
    Args:
        coins: List of coin IDs to extract (uses config if not provided)
        
    Returns:
        Combined DataFrame with all coin data
    """
    coins = coins or Config.COINS
    
    logger.info(f"[EXTRACT] Starting extraction for {len(coins)} coins: {coins}")
    
    all_data = []
    
    for coin in coins:
        df = extract_market_chart(coin)
        
        if df is not None and not df.empty:
            all_data.append(df)
        
        # Rate limiting - CoinGecko free tier allows ~10-50 calls/minute
        time.sleep(1.5)
    
    if not all_data:
        logger.error("[EXTRACT] No data extracted for any coin")
        return pd.DataFrame()
    
    # Combine all coin data
    combined_df = pd.concat(all_data, ignore_index=True)
    
    logger.info(f"[EXTRACT] Total records extracted: {len(combined_df)}")
    
    return combined_df
