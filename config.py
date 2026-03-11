import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    
    # Database Configuration
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "crypto_etl")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
    
    # CoinGecko API Configuration
    COINGECKO_BASE_URL = os.getenv(
        "COINGECKO_BASE_URL", 
        "https://api.coingecko.com/api/v3"
    )
    
    # Coins to track (comma-separated in env)
    COINS = os.getenv("COINS", "bitcoin,ethereum,solana").split(",")
    
    # Currency for price conversion
    VS_CURRENCY = os.getenv("VS_CURRENCY", "usd")
    
    # Number of days of historical data to fetch
    HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "30"))
    
    # Retry configuration
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))  # seconds
    
    @classmethod
    def get_db_url(cls) -> str:
        return (
            f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}"
            f"@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
        )
    
    @classmethod
    def validate(cls) -> bool:
        required = [cls.DB_HOST, cls.DB_NAME, cls.DB_USER, cls.DB_PASSWORD]
        return all(required)
