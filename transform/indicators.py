import pandas as pd

from utils.logger import logger, log_transform


def calculate_daily_return(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily percentage return for each coin."""
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
    """Calculate rolling moving average."""
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
    """Calculate rolling volatility (std of returns)."""
    df = df.copy()
    df = df.sort_values(["symbol", "date"])
    
    volatility_column = f"volatility_{window}"
    
    if "daily_return" not in df.columns:
        df = calculate_daily_return(df)
    
    df[volatility_column] = df.groupby("symbol")["daily_return"].transform(
        lambda x: x.rolling(window=window, min_periods=1).std()
    )
    
    return df


def calculate_price_volatility(
    df: pd.DataFrame,
    window: int = 7
) -> pd.DataFrame:
    """Calculate rolling volatility as std dev of price."""
    df = df.copy()
    df = df.sort_values(["symbol", "date"])

    df["volatility"] = df.groupby("symbol")["price"].transform(
        lambda x: x.rolling(window=window, min_periods=1).std()
    )

    return df


def calculate_price_change_pct(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate day-over-day percentage change in price."""
    df = df.copy()
    df = df.sort_values(["symbol", "date"])

    previous_price = df.groupby("symbol")["price"].shift(1)
    df["price_change_pct"] = ((df["price"] - previous_price) / previous_price) * 100

    return df


def calculate_bullish_flag(df: pd.DataFrame) -> pd.DataFrame:
    """Flag rows where daily return is positive."""
    df = df.copy()

    if "daily_return" not in df.columns:
        df = calculate_daily_return(df)

    df["is_bullish"] = df["daily_return"] > 0

    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare data for loading."""
    df = df.copy()
    
    df["daily_return"] = df["daily_return"].fillna(0)
    df["ma_7"] = df["ma_7"].fillna(df["price"])
    df["volatility_7"] = df["volatility_7"].fillna(0)
    df["volatility"] = df["volatility"].fillna(0)
    df["price_change_pct"] = df["price_change_pct"].fillna(0)
    df["is_bullish"] = df["is_bullish"].fillna(False)
    
    # Ensure proper types
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = df["symbol"].astype(str)
    df["price"] = df["price"].astype(float)
    df["market_cap"] = df["market_cap"].astype(float)
    df["volume"] = df["volume"].astype(float)
    df["daily_return"] = df["daily_return"].astype(float)
    df["ma_7"] = df["ma_7"].astype(float)
    df["volatility_7"] = df["volatility_7"].astype(float)
    df["volatility"] = df["volatility"].astype(float)
    df["price_change_pct"] = df["price_change_pct"].astype(float)
    df["is_bullish"] = df["is_bullish"].astype(bool)
    
    # Remove any exact duplicates
    df = df.drop_duplicates(subset=["date", "symbol"], keep="last")
    
    return df


def transform_market_data(df: pd.DataFrame) -> pd.DataFrame:
    """Run full transformation pipeline on market data."""
    if df.empty:
        logger.warning("[TRANSFORM] Empty DataFrame received, skipping transformation")
        return df
    
    logger.info(f"[TRANSFORM] Starting transformation of {len(df)} records")
    
    # Apply transformations sequentially
    df = calculate_daily_return(df)
    df = calculate_moving_average(df, window=7)
    df = calculate_volatility(df, window=7)
    df = calculate_price_volatility(df, window=7)
    df = calculate_price_change_pct(df)
    df = calculate_bullish_flag(df)
    df = clean_data(df)
    
    # Select final columns in order
    final_columns = [
        "date", "symbol", "price", "market_cap", "volume",
        "daily_return", "ma_7", "volatility_7", "volatility",
        "price_change_pct", "is_bullish"
    ]
    
    df = df[final_columns]
    
    log_transform(len(df))
    
    return df
