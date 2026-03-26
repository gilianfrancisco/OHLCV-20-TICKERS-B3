import getpass
import os

import psycopg


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


def print_section(title):
    print()
    print(title)
    print("-" * len(title))


def print_table(headers, rows):
    widths = [len(str(header)) for header in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(str(value)))

    print(" | ".join(str(header).ljust(widths[index]) for index, header in enumerate(headers)))
    print("-+-".join("-" * width for width in widths))
    for row in rows:
        print(" | ".join(str(value).ljust(widths[index]) for index, value in enumerate(row)))


def fetch_overview(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT ticker) AS distinct_tickers,
                MIN(trade_date) AS min_date,
                MAX(trade_date) AS max_date,
                COUNT(*) FILTER (WHERE trade_date >= DATE '2011-01-01') AS rows_from_2011,
                COUNT(*) FILTER (WHERE trade_date < DATE '2011-01-01') AS rows_before_2011
            FROM daily_prices
            """
        )
        return cursor.fetchone()


def fetch_by_ticker(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                ticker,
                COUNT(*) AS row_count,
                MIN(trade_date) AS min_date,
                MAX(trade_date) AS max_date
            FROM daily_prices
            GROUP BY ticker
            ORDER BY ticker
            """
        )
        return cursor.fetchall()


def fetch_by_year(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                EXTRACT(YEAR FROM trade_date)::INT AS year,
                COUNT(*) AS row_count
            FROM daily_prices
            GROUP BY year
            ORDER BY year
            """
        )
        return cursor.fetchall()


def fetch_suspicious_rows(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM daily_prices
            WHERE open_price <= 0
               OR high_price <= 0
               OR low_price <= 0
               OR close_price <= 0
               OR volume < 0
            """
        )
        suspicious_count = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT
                ticker,
                trade_date,
                open_price,
                high_price,
                low_price,
                close_price,
                volume
            FROM daily_prices
            WHERE open_price <= 0
               OR high_price <= 0
               OR low_price <= 0
               OR close_price <= 0
               OR volume < 0
            ORDER BY trade_date DESC, ticker
            LIMIT 20
            """
        )
        suspicious_rows = cursor.fetchall()

    return suspicious_count, suspicious_rows


def main():
    settings = get_postgres_settings()
    print(f"Database: {format_database_label(settings)}")

    connection = psycopg.connect(**settings)
    try:
        overview = fetch_overview(connection)
        by_ticker = fetch_by_ticker(connection)
        by_year = fetch_by_year(connection)
        suspicious_count, suspicious_rows = fetch_suspicious_rows(connection)
    finally:
        connection.close()

    print_section("Overview")
    print(f"total_rows: {overview[0]}")
    print(f"distinct_tickers: {overview[1]}")
    print(f"min_date: {overview[2]}")
    print(f"max_date: {overview[3]}")
    print(f"rows_from_2011: {overview[4]}")
    print(f"rows_before_2011: {overview[5]}")

    print_section("Per Ticker")
    print_table(
        ["ticker", "row_count", "min_date", "max_date"],
        by_ticker,
    )

    print_section("By Year")
    print_table(
        ["year", "row_count"],
        by_year,
    )

    print_section("Suspicious Rows")
    print(f"suspicious_count: {suspicious_count}")
    if suspicious_rows:
        print_table(
            ["ticker", "trade_date", "open", "high", "low", "close", "volume"],
            suspicious_rows,
        )


if __name__ == "__main__":
    main()
