import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()


class Config:

    # Database Configuration
    # Non-secret values may keep dev-friendly defaults; the password must never
    # have a hardcoded fallback (a default like "postgres" is a real security
    # hole if it ever ships to prod).
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "crypto_etl")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    # sslmode=require is expected by managed Postgres (Neon/Supabase/Aiven).
    DB_SSLMODE = os.getenv("DB_SSLMODE", "prefer")
    DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

    # CoinGecko API Configuration
    COINGECKO_BASE_URL = os.getenv(
        "COINGECKO_BASE_URL",
        "https://api.coingecko.com/api/v3"
    )
    # Optional CoinGecko Demo API key (higher, more reliable rate limits).
    COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()

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
        # A full DATABASE_URL (as handed out by Neon/Supabase/Aiven) wins and is
        # used verbatim so its embedded sslmode/credentials are preserved.
        if cls.DATABASE_URL:
            return cls.DATABASE_URL
        # Credentials are URL-encoded so special characters in a password
        # (@, :, /, #) don't corrupt the connection string.
        user = quote_plus(cls.DB_USER)
        password = quote_plus(cls.DB_PASSWORD)
        return (
            f"postgresql://{user}:{password}"
            f"@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
            f"?sslmode={cls.DB_SSLMODE}"
        )

    @classmethod
    def validate(cls) -> bool:
        # DATABASE_URL is self-contained; trust it.
        if cls.DATABASE_URL:
            return True
        # Discrete-parameter mode: everything must be present AND a password
        # must be explicitly supplied (no silent insecure default).
        required = [cls.DB_HOST, cls.DB_NAME, cls.DB_USER, cls.DB_PASSWORD]
        return all(required)
