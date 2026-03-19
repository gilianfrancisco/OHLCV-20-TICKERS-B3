import logging
import sqlite3
import time
from datetime import date
from datetime import timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

TICKERS = [
    "VALE3",
    "PETR4",
    "ITUB4",
    "BBAS3",
    "PRIO3",
    "BBDC4",
    "B3SA3",
    "AXIA3",
    "BPAC11",
    "PETR3",
    "ITSA4",
    "SBSP3",
    "RENT3",
    "EMBJ3",
    "ABEV3",
    "ENEV3",
    "WEGE3",
    "SUZB3",
    "CPLE3",
    "EQTL3",
]

START_DATE = date(2000, 1, 1)
CHUNK_YEARS = 2
THROTTLE_SECONDS = 0.5
RECOVERY_ROUNDS = 5
RECOVERY_DELAY_SECONDS = 2.0
DB_PATH = Path(__file__).resolve().with_name("prices_b3.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def connect_db():
    connection = sqlite3.connect(DB_PATH)
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_prices (
            ticker TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open_price REAL NOT NULL,
            high_price REAL NOT NULL,
            low_price REAL NOT NULL,
            close_price REAL NOT NULL,
            volume INTEGER NOT NULL,
            PRIMARY KEY (ticker, trade_date)
        )
        """
    )
    connection.commit()
    return connection


def add_years(current_date, years):
    try:
        return current_date.replace(year=current_date.year + years)
    except ValueError:
        return current_date.replace(year=current_date.year + years, month=2, day=28)


def two_year_windows(start_date, end_date):
    end_date_exclusive = end_date + timedelta(days=1)
    current_date = start_date
    while current_date < end_date_exclusive:
        next_date = add_years(current_date, CHUNK_YEARS)
        if next_date > end_date_exclusive:
            next_date = end_date_exclusive
        yield current_date, next_date
        current_date = next_date


def get_last_date(connection, ticker):
    row = connection.execute(
        "SELECT MAX(trade_date) FROM daily_prices WHERE ticker = ?",
        (ticker,),
    ).fetchone()
    return date.fromisoformat(row[0]) if row and row[0] else None


def download_rows(ticker, start_date, end_date):
    dataframe = yf.download(
        f"{ticker}.SA",
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        auto_adjust=True,
        progress=False,
    )
    if dataframe.empty:
        return []

    dataframe = dataframe.reset_index()
    if isinstance(dataframe.columns, pd.MultiIndex):
        dataframe.columns = [column[0] for column in dataframe.columns]

    rows = []
    for _, row in dataframe.iterrows():
        rows.append(
            (
                ticker,
                pd.to_datetime(row["Date"]).strftime("%Y-%m-%d"),
                float(row["Open"]),
                float(row["High"]),
                float(row["Low"]),
                float(row["Close"]),
                int(row["Volume"]),
            )
        )
    return rows


def save_rows(connection, rows):
    changes_before = connection.total_changes
    connection.executemany(
        """
        INSERT OR REPLACE INTO daily_prices
        (ticker, trade_date, open_price, high_price, low_price, close_price, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    connection.commit()
    return connection.total_changes - changes_before


def main():
    connection = connect_db()
    today = date.today()
    failed_windows = []
    logger.info("Starting ingestion | tickers=%s | database=%s", len(TICKERS), DB_PATH.name)

    try:
        for ticker in TICKERS:
            last_date = get_last_date(connection, ticker)
            start_date = last_date + timedelta(days=1) if last_date else START_DATE
            logger.info("%s | start=%s", ticker, start_date)

            if start_date >= today:
                logger.info("%s | up to date", ticker)
                continue

            inserted_rows = 0

            for chunk_start, chunk_end in two_year_windows(start_date, today):
                chunk_end_inclusive = chunk_end - timedelta(days=1)
                logger.info("%s | window=%s..%s", ticker, chunk_start, chunk_end_inclusive)
                try:
                    rows = download_rows(ticker, chunk_start, chunk_end)
                except Exception:
                    logger.exception(
                        "%s | failed window=%s..%s | queued_for_recovery",
                        ticker,
                        chunk_start,
                        chunk_end_inclusive,
                    )
                    failed_windows.append((ticker, chunk_start, chunk_end))
                    time.sleep(THROTTLE_SECONDS)
                    continue

                rows_inserted = 0
                if rows:
                    rows_inserted = save_rows(connection, rows)
                    inserted_rows += rows_inserted

                logger.info(
                    "%s | window=%s..%s | inserted=%s",
                    ticker,
                    chunk_start,
                    chunk_end_inclusive,
                    rows_inserted,
                )

                time.sleep(THROTTLE_SECONDS)

            logger.info("%s | first_pass_finished | inserted=%s", ticker, inserted_rows)

        for recovery_round in range(1, RECOVERY_ROUNDS + 1):
            if not failed_windows:
                break

            logger.info(
                "Starting recovery round %s/%s | pending_windows=%s",
                recovery_round,
                RECOVERY_ROUNDS,
                len(failed_windows),
            )

            pending_windows = failed_windows
            failed_windows = []

            for ticker, chunk_start, chunk_end in pending_windows:
                chunk_end_inclusive = chunk_end - timedelta(days=1)
                logger.info(
                    "%s | recovery_round=%s | window=%s..%s",
                    ticker,
                    recovery_round,
                    chunk_start,
                    chunk_end_inclusive,
                )

                try:
                    rows = download_rows(ticker, chunk_start, chunk_end)
                except Exception:
                    logger.exception(
                        "%s | recovery_round=%s | failed window=%s..%s",
                        ticker,
                        recovery_round,
                        chunk_start,
                        chunk_end_inclusive,
                    )
                    failed_windows.append((ticker, chunk_start, chunk_end))
                    continue

                rows_inserted = 0
                if rows:
                    rows_inserted = save_rows(connection, rows)

                logger.info(
                    "%s | recovery_round=%s | window=%s..%s | inserted=%s",
                    ticker,
                    recovery_round,
                    chunk_start,
                    chunk_end_inclusive,
                    rows_inserted,
                )
                time.sleep(THROTTLE_SECONDS)

            if failed_windows and recovery_round < RECOVERY_ROUNDS:
                logger.warning(
                    "Recovery round %s finished | remaining_windows=%s | retry_in=%ss",
                    recovery_round,
                    len(failed_windows),
                    RECOVERY_DELAY_SECONDS,
                )
                time.sleep(RECOVERY_DELAY_SECONDS)

        if failed_windows:
            logger.warning("Recovery finished with %s failed windows still pending", len(failed_windows))
        else:
            logger.info("Recovery finished with no pending failed windows")
    finally:
        connection.close()
        logger.info("Finished ingestion")


if __name__ == "__main__":
    main()
