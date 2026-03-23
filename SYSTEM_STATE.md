# SYSTEM_STATE

Chronological log of small repository changes.

Rule: add a new entry at the top with local timestamp `YYYY-MM-DD HH:MM:SS +/-TZ` and a short description before finishing a task.

## Entries

- 2026-03-22 23:44:17 -03:00 | Switched `yfinance` downloads to `auto_adjust=False` so new ingestions store raw OHLC instead of adjusted OHLC.
- 2026-03-22 03:12:21 -03:00 | Created `SYSTEM_STATE.md` to track small changes in the repository.
- 2026-03-22 03:12:21 -03:00 | Removed the temporary `migrar_prices_b3.py` from the public repo and cleaned related README references.
- 2026-03-22 03:02:11 -03:00 | Normalized the local SQLite `daily_prices.trade_date` schema to enforce ISO `YYYY-MM-DD` values and created a backup in `ARCHIVE/backups/`.
- 2026-03-22 03:01:13 -03:00 | Tightened secret-safe repo defaults with `.gitignore` rules and environment-variable documentation.
- 2026-03-21 23:49:04 -03:00 | Refactored DB path resolution and extracted `process_window(...)` to reduce duplication.
- 2026-03-19 23:15:39 -03:00 | Added environment-based configuration and secret-safe repo defaults.
- 2026-03-19 22:13:14 -03:00 | Added Dependabot and documented dependency pinning.
- 2026-03-18 23:37:59 -03:00 | Moved failed-window recovery to run only after the first full ingestion pass.
- 2026-03-18 23:26:20 -03:00 | Replaced `print()` calls with standard `logging`.
- 2026-03-18 23:07:54 -03:00 | Improved README install command portability with `python -m pip`.
- 2026-03-18 22:59:37 -03:00 | Made the README quickstart cross-platform.
- 2026-03-16 18:03:13 -03:00 | Re-rooted the repository to `PRICES_B3`.
