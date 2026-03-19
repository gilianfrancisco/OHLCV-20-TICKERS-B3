# OHLCV-20-TICKERS-B3

Minimal Python ingestor for daily OHLCV data from 20 selected B3 tickers. The script downloads historical market data from `yfinance` in 2-year windows and stores it in a local SQLite database, keeping the project small, readable, and easy to extend.

## Who It Is For

This repository is for recruiters, collaborators, and developers who want a compact example of a market data ingestion script with clear inputs, a predictable local output, and a simple incremental update flow.

## Project Scope

- Fixed universe of 20 B3 tickers defined in code.
- Single Python script with a single SQLite output database.
- Incremental reloads based on the last stored trading date per ticker.
- Portfolio-first implementation: simple structure over heavy architecture.

## Repository Structure

- `ingestor_prices_b3.py`: main script.
- `prices_b3.db`: generated locally when the script runs.
- `requirements.txt`: pinned runtime dependencies.

## Install

```powershell
python -m venv .venv
python -m pip install -r requirements.txt
```

## Run

```powershell
python ingestor_prices_b3.py
```

## Smoke Test

```powershell
python -m py_compile ingestor_prices_b3.py
```

## Example Output

```text
VALE3: +6234 rows
PETR4: +6210 rows
ITUB4: +6198 rows
```

## Important Decisions And Limitations

- Data source: `yfinance`.
- The ticker universe is intentionally fixed in the script.
- The local database file is recreated automatically if it does not exist.
- This is not a production trading system and does not include tests beyond a smoke validation with `python -m py_compile`.

## Contribution / Project Boundary

This repository is intentionally small and focused. Improvements should keep the code understandable, avoid unnecessary abstraction, and preserve the single-script entry point unless a new requirement clearly justifies more structure.
