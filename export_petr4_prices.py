import csv
import getpass
import os
from pathlib import Path

import psycopg

OUTPUT_PATH = Path("outputs") / "petr4_prices.csv"


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


def fetch_petr4_rows(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                trade_date,
                open_price,
                high_price,
                low_price,
                close_price,
                volume
            FROM daily_prices
            WHERE ticker = 'PETR4'
            ORDER BY trade_date
            """
        )
        return cursor.fetchall()


def write_csv(rows, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["trade_date", "open_price", "high_price", "low_price", "close_price", "volume"])
        writer.writerows(rows)


def main():
    settings = get_postgres_settings()
    connection = psycopg.connect(**settings)
    try:
        rows = fetch_petr4_rows(connection)
    finally:
        connection.close()

    if not rows:
        raise RuntimeError("No PETR4 rows were found in daily_prices.")

    write_csv(rows, OUTPUT_PATH)

    print(f"rows_exported: {len(rows)}")
    print(f"output: {OUTPUT_PATH.resolve()}")
    print(f"min_date: {rows[0][0]}")
    print(f"max_date: {rows[-1][0]}")


if __name__ == "__main__":
    main()
