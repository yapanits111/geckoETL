# Automated Crypto Market Data Pipeline with Webhook Alerts and Interactive Dashboard

Automated ETL pipeline that pulls crypto market data from CoinGecko, computes indicators, loads PostgreSQL, triggers Discord webhook alerts, and serves a Streamlit dashboard.

## Architecture

```
CoinGecko API
    |
    v
Python ETL Script
    |
    v
PostgreSQL Database
    |
    +--> Streamlit Dashboard (http://localhost:8501)
    |
    +--> Discord Webhook Alert (>5% move)
    |
    v
Docker Compose
```

## Features

- Modular E/T/L separation
- Idempotent loads with UPSERT
- Retry with exponential backoff
- Technical indicators (`daily_return`, `7day_avg`, `volatility`, `price_change_pct`, `is_bullish`)
- Docker ready
- Schema validation
- Streamlit dashboard for charts and KPI monitoring
- Discord webhook notifications for pipeline status and price alerts

## Project Structure

```
gecko/
├── main.py              # Pipeline orchestrator
├── dashboard.py         # Streamlit dashboard
├── config.py            # Configuration management
├── requirements.txt     # Python dependencies
├── Dockerfile           # Container definition
├── docker-compose.yml   # Multi-container setup
├── .env.example         # Environment template
├── extract/
│   └── coingecko.py     # API extraction logic
├── trans
│   └── indicators.py    # Technical indicator calculations
├── load/
│   └── postgres.py      # PostgreSQL UPSERT loader
├── utils/
│   ├── logger.py        # Logging utilities
│   ├── notifier.py      # Discord webhook notifications
│   └── validators.py    # Data validation
├── sql/
│   └── schema.sql       # Database schema
├── logs/                # Pipeline logs
└── screenshots/
    ├── dashboard.png    # Streamlit dashboard screenshot
    └── discord.png      # Discord alert screenshot
```

## Quick Start

### Option 1: Docker (Recommended)

```bash
# Start PostgreSQL + dashboard
docker-compose up --build -d postgres dashboard

# Run ETL pipeline manually
docker-compose run --rm etl python main.py
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

# Run Streamlit dashboard
streamlit run dashboard.py
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
| `DISCORD_WEBHOOK_URL` | (empty) | Discord webhook for notifications |

## Project Outputs

### 1. Running Streamlit Dashboard
- URL: `http://localhost:8501`
- Includes:
    - Price history chart
    - Daily returns chart
    - Metrics (latest price, 7-day average, volatility)
    - Raw data table

### 2. Discord Notifications
- Pipeline success message
- Price alert when absolute daily movement is above 5%

### 3. PostgreSQL Database
- Base table: `crypto_market_daily`
- Compatibility view: `crypto_prices` with columns:
    - `symbol`
    - `date`
    - `price`
    - `daily_return`
    - `7day_avg`
    - `volatility`
    - `is_bullish`

### 4. Log File
- `logs/pipeline.log`
- Structured pipeline events including start, extract, transform, load, webhook, and completion

## Database Schema

```sql
CREATE TABLE crypto_market_daily (
    date DATE NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    price FLOAT,
    market_cap FLOAT,
    volume FLOAT,
        daily_return FLOAT,
        ma_7 FLOAT,
        volatility_7 FLOAT,
        volatility FLOAT,
        price_change_pct FLOAT,
        is_bullish BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (symbol, date)
);

CREATE VIEW crypto_prices AS
SELECT
    symbol,
    date,
    price,
    daily_return,
    ma_7 AS "7day_avg",
    volatility,
    is_bullish
FROM crypto_market_daily;
```

## Tech Stack

Python 3.11, Pandas, PostgreSQL, psycopg2, Streamlit, Plotly, SQLAlchemy, Docker, CoinGecko API, Discord Webhooks

## Screenshots

Add your real screenshots here after running:
- Dashboard: `screenshots/dashboard.png`
- Discord alerts: `screenshots/discord.png`

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
February 2026
