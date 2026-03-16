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
DB_PATH = Path(__file__).resolve().with_name("prices_b3.db")


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
    connection.executemany(
        """
        INSERT OR REPLACE INTO daily_prices
        (ticker, trade_date, open_price, high_price, low_price, close_price, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    connection.commit()


def main():
    connection = connect_db()
    today = date.today()

    try:
        for ticker in TICKERS:
            last_date = get_last_date(connection, ticker)
            start_date = last_date + timedelta(days=1) if last_date else START_DATE

            if start_date >= today:
                print(f"{ticker}: up to date")
                continue

            inserted_rows = 0

            for chunk_start, chunk_end in two_year_windows(start_date, today):
                try:
                    rows = download_rows(ticker, chunk_start, chunk_end)
                except Exception as exc:
                    print(
                        f"{ticker}: failed for {chunk_start} to "
                        f"{chunk_end - timedelta(days=1)} - {exc}"
                    )
                    time.sleep(THROTTLE_SECONDS)
                    continue

                if rows:
                    changes_before = connection.total_changes
                    save_rows(connection, rows)
                    inserted_rows += connection.total_changes - changes_before

                time.sleep(THROTTLE_SECONDS)

            print(f"{ticker}: +{inserted_rows} rows")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
