# Daily Cryptocurrency Market Data Pipeline

A production-ready batch ETL pipeline that extracts cryptocurrency market data from CoinGecko API, transforms it with technical indicators, and loads it into PostgreSQL.

**Built for Data Engineering Internship Showcase**

---

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  CoinGecko  │────▶│   EXTRACT   │────▶│  TRANSFORM  │────▶│    LOAD     │
│     API     │     │ (API calls) │     │ (Pandas)    │     │ (PostgreSQL)│
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                          │                   │                   │
                          ▼                   ▼                   ▼
                    Retry Logic        Technical         UPSERT for
                    Rate Limiting      Indicators        Idempotency
```

## Features

- **Modular ETL Design**: Clean separation of Extract, Transform, Load
- **Idempotent Loads**: Uses PostgreSQL UPSERT (ON CONFLICT) for safe reruns
- **Error Handling**: Exponential backoff retry logic for API calls
- **Technical Indicators**: Daily returns, 7-day moving average, volatility
- **Docker Ready**: Full containerization with docker-compose
- **Structured Logging**: Timestamped logs for debugging and monitoring
- **Data Validation**: Schema validation before loading

## Project Structure

```
gecko/
├── main.py              # Pipeline orchestrator
├── config.py            # Configuration management
├── requirements.txt     # Python dependencies
├── Dockerfile           # Container definition
├── docker-compose.yml   # Multi-container setup
├── .env.example         # Environment template
├── extract/
│   └── coingecko.py     # API extraction logic
├── transform/
│   └── indicators.py    # Technical indicator calculations
├── load/
│   └── postgres.py      # PostgreSQL UPSERT loader
├── utils/
│   ├── logger.py        # Logging utilities
│   └── validators.py    # Data validation
├── sql/
│   └── schema.sql       # Database schema
└── logs/                # Pipeline logs
```

## Quick Start

### Option 1: Docker (Recommended)

```bash
# Start PostgreSQL and run the pipeline
docker-compose up --build

# Run pipeline only (if DB already running)
docker-compose run etl
```

### Option 2: Local Development

```bash
# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
copy .env.example .env
# Edit .env with your database credentials

# Run the pipeline
python main.py
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | localhost | PostgreSQL host |
| `DB_PORT` | 5432 | PostgreSQL port |
| `DB_NAME` | crypto_etl | Database name |
| `DB_USER` | postgres | Database user |
| `DB_PASSWORD` | - | Database password |
| `COINS` | bitcoin,ethereum,solana | Coins to track |
| `HISTORY_DAYS` | 30 | Days of historical data |
| `MAX_RETRIES` | 3 | API retry attempts |

## Database Schema

```sql
CREATE TABLE crypto_market_daily (
    date DATE NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    price FLOAT,
    market_cap FLOAT,
    volume FLOAT,
    daily_return FLOAT,      -- % change
    ma_7 FLOAT,              -- 7-day moving average
    volatility_7 FLOAT,      -- 7-day volatility
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (symbol, date)
);
```

## Tech Stack

- **Python 3.11** - Core language
- **Pandas** - Data transformation
- **PostgreSQL** - Data warehouse
- **psycopg2** - Database adapter
- **Docker** - Containerization
- **CoinGecko API** - Data source

## Engineering Best Practices

1. **Idempotency**: Safe to rerun without duplicates
2. **Separation of Concerns**: E/T/L modules are independent
3. **Configuration Management**: Environment-based config
4. **Error Handling**: Graceful failures with logging
5. **Rate Limiting**: Respects API limits (1.5s between calls)
6. **Data Validation**: Schema checks before loading

## Sample Output

```
2025-02-01 10:00:00 | INFO | ============================================================
2025-02-01 10:00:00 | INFO | DAILY CRYPTOCURRENCY MARKET DATA PIPELINE
2025-02-01 10:00:00 | INFO | Coins: ['bitcoin', 'ethereum', 'solana']
2025-02-01 10:00:00 | INFO | ============================================================
2025-02-01 10:00:00 | INFO | [PIPELINE] Starting pipeline at 2025-02-01 10:00:00
2025-02-01 10:00:00 | INFO | [PIPELINE] Step 1/3: EXTRACT
2025-02-01 10:00:05 | INFO | [EXTRACT] Extracted 90 records for 3 coins
2025-02-01 10:00:05 | INFO | [PIPELINE] Step 2/3: TRANSFORM
2025-02-01 10:00:05 | INFO | [TRANSFORM] Calculated indicators for 90 records
2025-02-01 10:00:05 | INFO | [PIPELINE] Step 3/3: LOAD
2025-02-01 10:00:06 | INFO | [LOAD] Loaded 90 records (inserted: 3, updated: 87)
2025-02-01 10:00:06 | INFO | [PIPELINE] Pipeline completed successfully
```

## Author

**Daniel Peñero**  
Data Engineering Internship Project  
February 2026
