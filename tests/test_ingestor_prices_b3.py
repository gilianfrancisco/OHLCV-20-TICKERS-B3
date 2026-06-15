import os
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from unittest import mock

import ingestor_prices_b3
import pandas as pd


class LoadEnvFileTests(unittest.TestCase):
    def test_load_env_file_sets_missing_values_only(self):
        original_value = os.environ.get("PGHOST")
        os.environ["PGHOST"] = "db.example"

        try:
            with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as handle:
                handle.write("PGHOST=localhost\n")
                handle.write("PGPORT=5432\n")
                env_path = handle.name

            ingestor_prices_b3.load_env_file(env_path)

            self.assertEqual(os.environ["PGHOST"], "db.example")
            self.assertEqual(os.environ["PGPORT"], "5432")
        finally:
            if original_value is None:
                os.environ.pop("PGHOST", None)
            else:
                os.environ["PGHOST"] = original_value
            os.environ.pop("PGPORT", None)
            os.unlink(env_path)


class GetPostgresSettingsTests(unittest.TestCase):
    @mock.patch.dict(
        os.environ,
        {
            "PGHOST": "localhost",
            "PGPORT": "5432",
            "PGDATABASE": "prices_b3",
            "PGUSER": "postgres",
            "PGPASSWORD": "secret",
        },
        clear=True,
    )
    @mock.patch("getpass.getpass")
    def test_get_postgres_settings_prefers_env_password(self, prompt_mock):
        settings = ingestor_prices_b3.get_postgres_settings()

        self.assertEqual(settings["password"], "secret")
        prompt_mock.assert_not_called()

    @mock.patch.dict(os.environ, {}, clear=True)
    @mock.patch("getpass.getpass", return_value="prompt-secret")
    def test_get_postgres_settings_prompts_when_password_missing(self, prompt_mock):
        settings = ingestor_prices_b3.get_postgres_settings()

        self.assertEqual(settings["host"], ingestor_prices_b3.DEFAULT_PGHOST)
        self.assertEqual(settings["password"], "prompt-secret")
        prompt_mock.assert_called_once()


class DateAndPriceHelpersTests(unittest.TestCase):
    def test_add_years_handles_leap_day(self):
        self.assertEqual(
            ingestor_prices_b3.add_years(date(2024, 2, 29), 1),
            date(2025, 2, 28),
        )

    def test_two_year_windows_cover_end_date(self):
        windows = list(
            ingestor_prices_b3.two_year_windows(
                date(2020, 1, 1),
                date(2023, 12, 31),
            )
        )

        self.assertEqual(
            windows,
            [
                (date(2020, 1, 1), date(2022, 1, 1)),
                (date(2022, 1, 1), date(2024, 1, 1)),
            ],
        )

    def test_normalize_price_uses_six_decimal_places(self):
        self.assertEqual(
            ingestor_prices_b3.normalize_price("10.1234567"),
            Decimal("10.123457"),
        )


class DownloadRowsTests(unittest.TestCase):
    @mock.patch("ingestor_prices_b3.yf.download")
    def test_download_rows_normalizes_multiindex_data(self, download_mock):
        download_mock.return_value = pd.DataFrame(
            [[10.1234567, 10.3, 9.9, 10.2, 1234]],
            index=pd.Index([pd.Timestamp("2024-01-02")], name="Date"),
            columns=pd.MultiIndex.from_tuples(
                [
                    ("Open", "VALE3.SA"),
                    ("High", "VALE3.SA"),
                    ("Low", "VALE3.SA"),
                    ("Close", "VALE3.SA"),
                    ("Volume", "VALE3.SA"),
                ]
            ),
        )

        rows = ingestor_prices_b3.download_rows("VALE3", date(2024, 1, 1), date(2024, 1, 3))

        self.assertEqual(
            rows,
            [
                (
                    "VALE3",
                    "2024-01-02",
                    Decimal("10.123457"),
                    Decimal("10.300000"),
                    Decimal("9.900000"),
                    Decimal("10.200000"),
                    1234,
                )
            ],
        )
        download_mock.assert_called_once_with(
            "VALE3.SA",
            start="2024-01-01",
            end="2024-01-03",
            auto_adjust=False,
            repair=True,
            progress=False,
        )

    @mock.patch("ingestor_prices_b3.yf.download", return_value=pd.DataFrame())
    def test_download_rows_returns_empty_list_for_empty_frame(self, _download_mock):
        rows = ingestor_prices_b3.download_rows("VALE3", date(2024, 1, 1), date(2024, 1, 3))

        self.assertEqual(rows, [])


@unittest.skipUnless(
    os.getenv("PRICES_B3_RUN_DB_TESTS") == "1",
    "Set PRICES_B3_RUN_DB_TESTS=1 to run PostgreSQL smoke tests.",
)
class SaveRowsPostgresSmokeTests(unittest.TestCase):
    def test_save_rows_upserts_against_postgres(self):
        settings = {
            "host": os.getenv("PGHOST") or "127.0.0.1",
            "port": os.getenv("PGPORT") or ingestor_prices_b3.DEFAULT_PGPORT,
            "dbname": os.getenv("PGDATABASE") or ingestor_prices_b3.DEFAULT_PGDATABASE,
            "user": os.getenv("PGUSER") or ingestor_prices_b3.DEFAULT_PGUSER,
            "password": os.getenv("PGPASSWORD") or "postgres",
        }
        connection = ingestor_prices_b3.psycopg.connect(**settings)

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TEMP TABLE daily_prices (
                        ticker TEXT NOT NULL,
                        trade_date DATE NOT NULL,
                        open_price NUMERIC(18,6) NOT NULL,
                        high_price NUMERIC(18,6) NOT NULL,
                        low_price NUMERIC(18,6) NOT NULL,
                        close_price NUMERIC(18,6) NOT NULL,
                        volume BIGINT NOT NULL,
                        PRIMARY KEY (ticker, trade_date)
                    )
                    """
                )

            initial_rows = [
                (
                    "TEST3",
                    "2024-01-02",
                    Decimal("10.100000"),
                    Decimal("10.500000"),
                    Decimal("9.900000"),
                    Decimal("10.200000"),
                    1000,
                )
            ]
            updated_rows = [
                (
                    "TEST3",
                    "2024-01-02",
                    Decimal("10.100000"),
                    Decimal("10.600000"),
                    Decimal("9.800000"),
                    Decimal("10.400000"),
                    1200,
                )
            ]

            self.assertEqual(ingestor_prices_b3.save_rows(connection, initial_rows), 1)
            self.assertEqual(ingestor_prices_b3.save_rows(connection, updated_rows), 1)

            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT open_price, high_price, low_price, close_price, volume
                    FROM daily_prices
                    WHERE ticker = %s AND trade_date = %s
                    """,
                    ("TEST3", date(2024, 1, 2)),
                )
                saved_row = cursor.fetchone()

            self.assertEqual(
                saved_row,
                (
                    Decimal("10.100000"),
                    Decimal("10.600000"),
                    Decimal("9.800000"),
                    Decimal("10.400000"),
                    1200,
                ),
            )
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
