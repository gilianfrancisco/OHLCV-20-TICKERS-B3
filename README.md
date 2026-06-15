# prices_b3

Small Python project that downloads daily OHLCV data for 20 selected B3 tickers from Yahoo Finance and stores it in PostgreSQL.

This repository is meant to be a compact portfolio project, not a production market data platform. The focus is on clear code, reproducible local setup, and an honest project boundary.

## What Problem It Solves

If you want a simple local dataset of Brazilian stock prices for analysis, dashboards, or SQL practice, this project gives you a minimal ingestion flow:

- download daily OHLCV history from `yfinance`
- store the data in PostgreSQL
- update existing tickers incrementally on later runs
- keep the tracked dataset intentionally small enough for local use and portfolio review
- keep the implementation small enough to review quickly

## Current Scope

- Fixed list of 20 B3 tickers defined in code to keep data volume manageable
- One ingestion script: `ingestor_prices_b3.py`
- One PostgreSQL table: `daily_prices`
- Daily historical prices only
- Local development setup only

## Repository Layout

- `ingestor_prices_b3.py`: downloads data and upserts it into PostgreSQL
- `tests/test_ingestor_prices_b3.py`: unit tests plus an optional PostgreSQL smoke test
- `requirements.txt`: pinned runtime dependencies
- `.env.example`: local PostgreSQL settings template
- `docker-compose.yml`: optional local PostgreSQL service for easier setup
- `.github/workflows/tests.yml`: runs the unit test suite on push and pull request
- `LICENSE`: MIT license for the repository

## Requirements

- Python 3.14
- PostgreSQL
- Internet access when running the ingestor, because data comes from Yahoo Finance

Validated locally with Python `3.14.4`. The GitHub Actions workflow currently tests Python `3.14` only.

## Quick Start

The command examples below use bash on Linux/macOS. On Windows PowerShell, use the equivalent virtual environment path such as `.venv\Scripts\python`.

1. Create a virtual environment and install dependencies.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

2. Create a local config file.

```bash
cp .env.example .env
```

3. Start PostgreSQL.

Option A: use the included Docker Compose service.

```bash
docker compose up -d db
```

Option B: use your own local PostgreSQL instance and update `.env` with its connection details.

4. Run the ingestor.

```bash
.venv/bin/python ingestor_prices_b3.py
```

The script loads `.env` automatically if it exists. If `PGPASSWORD` is not set, it will prompt for the PostgreSQL password interactively.

## Fresh Checkout Validation

These commands are the simplest review path for a recruiter or technical reviewer:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp .env.example .env
.venv/bin/python -m unittest discover -s tests -v
```

To run the actual ingestion end to end:

```bash
docker compose up -d db
.venv/bin/python ingestor_prices_b3.py
```

The live ingestion path depends on Yahoo Finance being reachable and may return incomplete or empty results if the upstream service changes, rate-limits requests, or is temporarily unavailable.

To confirm data landed in PostgreSQL:

```bash
PGPASSWORD=postgres psql -h 127.0.0.1 -U postgres -d prices_b3 -c \
  "SELECT ticker, MIN(trade_date), MAX(trade_date), COUNT(*) AS rows_count
   FROM daily_prices
   GROUP BY ticker
   ORDER BY ticker;"
```

## Example Output

```text
2026-06-15 10:12:08 | INFO | Starting ingestion | tickers=20 | database=localhost:5432/prices_b3
2026-06-15 10:12:08 | INFO | VALE3 | start=2000-01-01
2026-06-15 10:12:10 | INFO | VALE3 | window=2000-01-01..2001-12-31 | inserted=498
```

## How It Works

- The ticker list is hard-coded in the script.
- Data is downloaded in 2-year windows.
- Existing tickers are refreshed from the last stored date with a 7-day lookback.
- Inserts use PostgreSQL upserts on `(ticker, trade_date)`.
- The table is created automatically if it does not exist.

## Testing

Current tests are intentionally small and cover the most important non-network behavior:

- `.env` loading
- PostgreSQL settings resolution
- date window generation
- price normalization
- Yahoo Finance row shaping and normalization

The testing strategy is intentionally reviewer-friendly:

- deterministic unit tests for core logic
- one optional local PostgreSQL smoke test for the upsert path
- no live-network tests in CI, so the default validation path stays stable and easy to reproduce

Run them with:

```bash
.venv/bin/python -m unittest discover -s tests -v
```

The unit test suite also runs automatically in GitHub Actions on push and pull request.

There is also an optional PostgreSQL smoke test for the upsert path. It uses a temporary table on the same connection, so it does not modify the permanent `daily_prices` table.

```bash
docker compose up -d db
PGPASSWORD=postgres PRICES_B3_RUN_DB_TESTS=1 .venv/bin/python -m unittest discover -s tests -v
```

## Limitations

- No CLI arguments or configurable ticker universe
- No scheduling, orchestration, or production deployment support
- Depends on Yahoo Finance availability and `yfinance` behavior
- Live downloads are best-effort only and are not designed for guaranteed completeness
- Not intended for trading, backtesting accuracy guarantees, or production use

## Publishing Notes

- Do not commit a real `.env` file
- Local database backups and generated files are ignored by `.gitignore`
- The Docker setup is intentionally minimal and only exists to make local validation easier
