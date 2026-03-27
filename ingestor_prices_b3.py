import getpass
import logging
import os
import time
from datetime import date
from datetime import timedelta

import pandas as pd
import psycopg
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_postgres_settings():
    required_values = {
        "PGHOST": os.getenv("PGHOST"),
        "PGDATABASE": os.getenv("PGDATABASE"),
        "PGUSER": os.getenv("PGUSER"),
    }
    settings = {
        "host": required_values["PGHOST"],
        "port": os.getenv("PGPORT", "5432"),
        "dbname": required_values["PGDATABASE"],
        "user": required_values["PGUSER"],
    }
    missing = [name for name, value in required_values.items() if not value]
    if missing:
        raise RuntimeError(
            "Missing required PostgreSQL environment variables: " + ", ".join(missing)
        )
    password = getpass.getpass("PostgreSQL password: ")
    if not password:
        raise RuntimeError("PostgreSQL password prompt was empty.")
    settings["password"] = password
    return settings


def format_database_label(settings):
    return f"{settings['host']}:{settings['port']}/{settings['dbname']}"


def connect_db(settings):
    connection = psycopg.connect(**settings)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_prices (
                ticker TEXT NOT NULL,
                trade_date DATE NOT NULL,
                open_price DOUBLE PRECISION NOT NULL,
                high_price DOUBLE PRECISION NOT NULL,
                low_price DOUBLE PRECISION NOT NULL,
                close_price DOUBLE PRECISION NOT NULL,
                volume BIGINT NOT NULL,
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
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT MAX(trade_date) FROM daily_prices WHERE ticker = %s",
            (ticker,),
        )
        row = cursor.fetchone()

    if not row or not row[0]:
        return None
    if isinstance(row[0], date):
        return row[0]
    return date.fromisoformat(row[0])


def download_rows(ticker, start_date, end_date):
    dataframe = yf.download(
        f"{ticker}.SA",
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        auto_adjust=False,
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
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO daily_prices
            (ticker, trade_date, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, trade_date) DO UPDATE
            SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume
            """,
            rows,
        )
        rows_written = cursor.rowcount
    connection.commit()
    return rows_written if rows_written and rows_written > -1 else len(rows)


def process_window(connection, ticker, chunk_start, chunk_end, recovery_round=None):
    chunk_end_inclusive = chunk_end - timedelta(days=1)

    if recovery_round is None:
        logger.info("%s | window=%s..%s", ticker, chunk_start, chunk_end_inclusive)
    else:
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
        if recovery_round is None:
            logger.exception(
                "%s | failed window=%s..%s | queued_for_recovery",
                ticker,
                chunk_start,
                chunk_end_inclusive,
            )
        else:
            logger.exception(
                "%s | recovery_round=%s | failed window=%s..%s",
                ticker,
                recovery_round,
                chunk_start,
                chunk_end_inclusive,
            )
        return None

    rows_inserted = save_rows(connection, rows) if rows else 0

    if recovery_round is None:
        logger.info(
            "%s | window=%s..%s | inserted=%s",
            ticker,
            chunk_start,
            chunk_end_inclusive,
            rows_inserted,
        )
    else:
        logger.info(
            "%s | recovery_round=%s | window=%s..%s | inserted=%s",
            ticker,
            recovery_round,
            chunk_start,
            chunk_end_inclusive,
            rows_inserted,
        )

    return rows_inserted


def main():
    settings = get_postgres_settings()
    database_label = format_database_label(settings)
    connection = connect_db(settings)
    today = date.today()
    failed_windows = []
    logger.info("Starting ingestion | tickers=%s | database=%s", len(TICKERS), database_label)

    try:
        for ticker in TICKERS:
            last_date = get_last_date(connection, ticker)
            # Revisit the last stored day so a partial latest bar can be corrected on the next run.
            start_date = last_date if last_date else START_DATE
            logger.info("%s | start=%s", ticker, start_date)

            if start_date > today:
                logger.info("%s | up to date", ticker)
                continue

            inserted_rows = 0

            for chunk_start, chunk_end in two_year_windows(start_date, today):
                rows_inserted = process_window(connection, ticker, chunk_start, chunk_end)
                if rows_inserted is None:
                    failed_windows.append((ticker, chunk_start, chunk_end))
                    time.sleep(THROTTLE_SECONDS)
                    continue

                inserted_rows += rows_inserted
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
                rows_inserted = process_window(
                    connection,
                    ticker,
                    chunk_start,
                    chunk_end,
                    recovery_round=recovery_round,
                )
                if rows_inserted is None:
                    failed_windows.append((ticker, chunk_start, chunk_end))
                    continue
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
