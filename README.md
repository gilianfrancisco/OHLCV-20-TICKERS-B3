# OHLCV-20-TICKERS-B3

Minimal Python ingestor for daily OHLCV data from 20 selected B3 tickers. The script downloads historical market data from `yfinance` in 2-year windows and stores it in PostgreSQL, keeping the project small, readable, and easy to extend.

## Who It Is For

This repository is for recruiters, collaborators, and developers who want a compact example of a market data ingestion script with clear inputs, a predictable local output, and a simple incremental update flow.

## Project Scope

- Fixed universe of 20 B3 tickers defined in code.
- Single Python script with a single PostgreSQL target table.
- Incremental reloads based on the last stored trading date per ticker.
- Portfolio-first implementation: simple structure over heavy architecture.

## Repository Structure

- `ingestor_prices_b3.py`: main script.
- `audit_prices_b3.py`: PostgreSQL audit script for date coverage, counts, and suspicious rows.
- `export_petr4_prices.py`: exports all PETR4 rows to CSV for external comparison.
- `requirements.txt`: pinned runtime dependencies.
- `.env.example`: local configuration template.
- `SYSTEM_STATE.md`: dated log of small repository changes.

## Install

```powershell
python -m venv .venv
python -m pip install -r requirements.txt
```

## Configuration

- The script reads configuration from environment variables.
- PostgreSQL connection settings come from `PGHOST`, `PGPORT`, `PGDATABASE`, and `PGUSER`.
- The script asks for the PostgreSQL password interactively at runtime.
- `.env.example` documents the supported local configuration.
- No secrets in repo: do not commit `.env`, tokens, connection strings, or personal paths.

## Run

```powershell
python ingestor_prices_b3.py
python audit_prices_b3.py
python export_petr4_prices.py
```

## Smoke Test

```powershell
python -m py_compile ingestor_prices_b3.py
python -m py_compile audit_prices_b3.py
python -m py_compile export_petr4_prices.py
```

## Example Output

```text
VALE3: +6234 rows
PETR4: +6210 rows
ITUB4: +6198 rows
```

## Important Decisions And Limitations

- Data source: `yfinance`.
- Dependencies are pinned in `requirements.txt`, and GitHub Dependabot is configured for automated dependency alerts.
- The ticker universe is intentionally fixed in the script.
- The script creates the PostgreSQL `daily_prices` table if it does not exist.
- The PostgreSQL schema stores `trade_date` as `DATE`.
- The audit script helps verify overall date coverage, per-ticker max dates, yearly counts, and suspicious rows.
- The PETR4 export script writes `outputs/petr4_prices.csv` for direct comparison with Power BI or charting tools.
- Small repository changes are recorded in `SYSTEM_STATE.md`.
- This is not a production trading system and does not include tests beyond a smoke validation with `python -m py_compile`.

## Contribution / Project Boundary

This repository is intentionally small and focused. Improvements should keep the code understandable, avoid unnecessary abstraction, and preserve the single-script entry point unless a new requirement clearly justifies more structure.
