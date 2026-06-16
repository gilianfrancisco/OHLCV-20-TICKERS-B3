import os
import re
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from unittest import mock

import ingestor_prices_b3
import pandas as pd
from curl_cffi.requests.exceptions import HTTPError

ingestor_prices_b3.load_env_file()


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


class DatabaseHelperTests(unittest.TestCase):
    def test_get_last_date_returns_none_when_no_rows_exist(self):
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (None,)

        last_date = ingestor_prices_b3.get_last_date(connection, "VALE3")

        self.assertIsNone(last_date)

    def test_get_last_date_returns_date_instance_without_conversion(self):
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (date(2024, 1, 2),)

        last_date = ingestor_prices_b3.get_last_date(connection, "VALE3")

        self.assertEqual(last_date, date(2024, 1, 2))

    def test_get_last_date_converts_iso_string_to_date(self):
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("2024-01-02",)

        last_date = ingestor_prices_b3.get_last_date(connection, "VALE3")

        self.assertEqual(last_date, date(2024, 1, 2))

    @mock.patch("ingestor_prices_b3.psycopg.connect")
    def test_connect_db_creates_table_and_commits(self, connect_mock):
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        connect_mock.return_value = connection
        settings = {
            "host": "localhost",
            "port": "5432",
            "dbname": "prices_b3",
            "user": "postgres",
            "password": "secret",
        }

        returned_connection = ingestor_prices_b3.connect_db(settings)

        self.assertIs(returned_connection, connection)
        connect_mock.assert_called_once_with(**settings)
        self.assertEqual(cursor.execute.call_count, 2)
        executed_sql = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
        self.assertIn("CREATE TABLE IF NOT EXISTS daily_prices", executed_sql)
        self.assertIn("ALTER TABLE daily_prices", executed_sql)
        connection.commit.assert_called_once()


class TickerListTests(unittest.TestCase):
    def test_ticker_list_has_expected_size_and_unique_values(self):
        self.assertEqual(len(ingestor_prices_b3.TICKERS), 20)
        self.assertEqual(len(set(ingestor_prices_b3.TICKERS)), 20)

    def test_ticker_list_uses_b3_style_symbols(self):
        for ticker in ingestor_prices_b3.TICKERS:
            self.assertRegex(ticker, re.compile(r"^[A-Z0-9]{4,6}$"))


class DownloadRowsTests(unittest.TestCase):
    def test_window_targets_present_day_returns_true_only_for_current_window(self):
        today = date.today()

        self.assertTrue(ingestor_prices_b3.window_targets_present_day(today + ingestor_prices_b3.timedelta(days=1)))
        self.assertFalse(ingestor_prices_b3.window_targets_present_day(today))

    @mock.patch("ingestor_prices_b3.download_history_frame")
    def test_download_rows_normalizes_multiindex_data(self, download_history_frame_mock):
        download_history_frame_mock.return_value = pd.DataFrame(
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
        download_history_frame_mock.assert_called_once_with(
            "VALE3",
            date(2024, 1, 1),
            date(2024, 1, 3),
        )

    @mock.patch("ingestor_prices_b3.download_history_frame", return_value=pd.DataFrame())
    def test_download_rows_returns_empty_list_for_empty_frame(self, _download_history_frame_mock):
        rows = ingestor_prices_b3.download_rows("VALE3", date(2024, 1, 1), date(2024, 1, 3))

        self.assertEqual(rows, [])

    @mock.patch(
        "ingestor_prices_b3.download_history_frame",
        side_effect=ingestor_prices_b3.YFPricesMissingError(
            "VALE3.SA",
            '(1d 2024-01-01 -> 2024-01-03) (Yahoo error = "Data doesn\'t exist")',
        ),
    )
    def test_download_rows_returns_empty_list_for_missing_price_data(self, _download_history_frame_mock):
        rows = ingestor_prices_b3.download_rows("VALE3", date(2024, 1, 1), date(2024, 1, 3))

        self.assertEqual(rows, [])

    @mock.patch(
        "ingestor_prices_b3.download_history_frame",
        side_effect=ModuleNotFoundError("No module named 'scipy'"),
    )
    def test_download_rows_propagates_unexpected_download_failures(self, _download_history_frame_mock):
        with self.assertRaises(ModuleNotFoundError):
            ingestor_prices_b3.download_rows("VALE3", date(2024, 1, 1), date(2024, 1, 3))

    @mock.patch(
        "ingestor_prices_b3.download_history_frame",
        side_effect=ingestor_prices_b3.YFTzMissingError("VALE3.SA"),
    )
    def test_download_rows_wraps_timezone_metadata_failures_as_recoverable(self, _download_history_frame_mock):
        with self.assertRaises(ingestor_prices_b3.RecoverableDownloadError):
            ingestor_prices_b3.download_rows("VALE3", date(2024, 1, 1), date(2024, 1, 3))

    @mock.patch(
        "ingestor_prices_b3.download_history_frame",
        side_effect=HTTPError("HTTP Error 404:", 0, mock.Mock(status_code=404)),
    )
    def test_download_rows_wraps_yahoo_http_failures_as_recoverable(self, _download_history_frame_mock):
        with self.assertRaises(ingestor_prices_b3.RecoverableDownloadError):
            ingestor_prices_b3.download_rows("VALE3", date(2024, 1, 1), date(2024, 1, 3))

    @mock.patch("ingestor_prices_b3.download_history_frame")
    def test_download_rows_uses_previous_day_data_when_present_day_fails(self, download_history_frame_mock):
        today = date.today()
        start_date = today - ingestor_prices_b3.timedelta(days=6)
        end_date = today + ingestor_prices_b3.timedelta(days=1)

        download_history_frame_mock.side_effect = [
            ingestor_prices_b3.YFPricesMissingError("VALE3.SA", '(1d current day unavailable)'),
            pd.DataFrame(
                [[10.1234567, 10.3, 9.9, 10.2, 1234]],
                index=pd.Index([pd.Timestamp(start_date.isoformat())], name="Date"),
                columns=pd.MultiIndex.from_tuples(
                    [
                        ("Open", "VALE3.SA"),
                        ("High", "VALE3.SA"),
                        ("Low", "VALE3.SA"),
                        ("Close", "VALE3.SA"),
                        ("Volume", "VALE3.SA"),
                    ]
                ),
            ),
        ]

        rows = ingestor_prices_b3.download_rows("VALE3", start_date, end_date)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "VALE3")
        self.assertEqual(download_history_frame_mock.call_args_list[0].args, ("VALE3", start_date, end_date))
        self.assertEqual(
            download_history_frame_mock.call_args_list[1].args,
            ("VALE3", start_date, today),
        )

    @mock.patch(
        "ingestor_prices_b3.download_history_frame",
        side_effect=ingestor_prices_b3.YFPricesMissingError("VALE3.SA", '(1d current day unavailable)'),
    )
    def test_download_rows_returns_empty_for_present_day_only_window(self, _download_history_frame_mock):
        today = date.today()
        rows = ingestor_prices_b3.download_rows(
            "VALE3",
            today,
            today + ingestor_prices_b3.timedelta(days=1),
        )

        self.assertEqual(rows, [])

    @mock.patch("ingestor_prices_b3.yf.Ticker")
    def test_download_history_frame_calls_ticker_history_and_restores_yfinance_config(self, ticker_mock):
        ticker_mock.return_value.history.return_value = pd.DataFrame()
        original_value = ingestor_prices_b3.YfConfig.debug.hide_exceptions
        ingestor_prices_b3.YfConfig.debug.hide_exceptions = True

        try:
            ingestor_prices_b3.download_history_frame("VALE3", date(2024, 1, 1), date(2024, 1, 3))
        finally:
            self.assertTrue(ingestor_prices_b3.YfConfig.debug.hide_exceptions)
            ingestor_prices_b3.YfConfig.debug.hide_exceptions = original_value

        ticker_mock.assert_called_once_with("VALE3.SA")
        ticker_mock.return_value.history.assert_called_once_with(
            start="2024-01-01",
            end="2024-01-03",
            interval="1d",
            actions=False,
            auto_adjust=False,
            repair=True,
        )


class ProcessWindowTests(unittest.TestCase):
    def test_classify_recoverable_download_error_formats_yahoo_http_error(self):
        error = HTTPError("HTTP Error 404:", 0, mock.Mock(status_code=404))

        self.assertEqual(
            ingestor_prices_b3.classify_recoverable_download_error(error),
            "Yahoo metadata request failed: HTTP 404",
        )

    @mock.patch("ingestor_prices_b3.logger.warning")
    @mock.patch(
        "ingestor_prices_b3.download_rows",
        side_effect=ingestor_prices_b3.RecoverableDownloadError("temporary Yahoo metadata failure"),
    )
    def test_process_window_logs_recoverable_failures_without_traceback(
        self,
        _download_rows_mock,
        warning_mock,
    ):
        rows_inserted = ingestor_prices_b3.process_window(
            mock.MagicMock(),
            "VALE3",
            date(2024, 1, 1),
            date(2024, 1, 3),
        )

        self.assertIsNone(rows_inserted)
        warning_mock.assert_called_once()
        self.assertIn("queued_for_recovery", warning_mock.call_args.args[0])


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
