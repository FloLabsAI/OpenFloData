"""
Simple focused tests for stream_service.py ProductionStreamer class.

Tests the actual implementation, not just formulas.
"""

import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from stream_service import ProductionStreamer


class TestConfiguration:
    """Test configuration loading from environment variables."""

    @patch.dict(
        os.environ,
        {
            "START_OFFSET_DAYS": "730",
            "BATCH_SIZE_HOURS": "336",
            "TZ": "Asia/Kuala_Lumpur",
        },
    )
    def test_loads_environment_variables(self):
        """Verify ProductionStreamer reads configuration from env vars."""
        streamer = ProductionStreamer()

        assert streamer.start_offset_days == 730
        assert streamer.batch_size_hours == 336
        assert streamer.local_tz == ZoneInfo("Asia/Kuala_Lumpur")

    def test_uses_defaults_when_env_not_set(self):
        """Verify default values when environment variables aren't set."""
        with patch.dict(os.environ, {}, clear=True):
            streamer = ProductionStreamer()

            assert streamer.start_offset_days == 0
            assert streamer.batch_size_hours == 168
            assert streamer.local_tz == ZoneInfo("UTC")


class TestDataTransformations:
    """Test data cleaning and derived metrics in _fetch_data_window."""

    @patch("stream_service.ProductionStreamer.connect_databases")
    @patch("stream_service.duckdb.connect")
    def test_cleans_well_names(self, mock_duckdb, mock_connect):
        """Test well name cleaning: '15/9-F-11 B' → 'F-11B'."""
        streamer = ProductionStreamer()

        # Mock DuckDB to return raw well names
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.fetchdf.return_value = pd.DataFrame(
            {
                "original_time": [datetime(2024, 1, 1, 10, 0)],
                "well_name": ["15/9-F-11 B"],  # Raw name from database
                "well_type": ["OP"],
                "oil_rate": [1000.0],
                "gas_rate": [50000.0],
                "water_rate": [100.0],
                "water_inj_rate": [0.0],
                "on_stream_hrs": [1.0],
                "downhole_pressure": [3000.0],
                "downhole_temperature": [200.0],
                "dp_tubing": [100.0],
                "annulus_pressure": [500.0],
                "choke_size": [32.0],
                "choke_size_uom": ["mm"],
                "dp_choke_size": [0.0],
                "thp": [1000.0],
                "wht": [150.0],
                "flow_kind": ["production"],
            }
        )
        mock_conn.execute.return_value = mock_result
        streamer.duck_conn = mock_conn

        # Call the actual method
        df = streamer._fetch_data_window(datetime(2024, 1, 1), datetime(2024, 1, 2))

        # Verify cleaning worked
        assert df["well_name"].iloc[0] == "F-11B"

    @patch("stream_service.ProductionStreamer.connect_databases")
    @patch("stream_service.duckdb.connect")
    def test_calculates_derived_metrics(self, mock_duckdb, mock_connect):
        """Test GOR, watercut, and liquid_rate calculations."""
        streamer = ProductionStreamer()

        # Mock DuckDB response
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.fetchdf.return_value = pd.DataFrame(
            {
                "original_time": [datetime(2024, 1, 1, 10, 0)],
                "well_name": ["F-11B"],
                "well_type": ["OP"],
                "oil_rate": [1000.0],
                "gas_rate": [50000.0],
                "water_rate": [100.0],
                "water_inj_rate": [0.0],
                "on_stream_hrs": [1.0],
                "downhole_pressure": [3000.0],
                "downhole_temperature": [200.0],
                "dp_tubing": [100.0],
                "annulus_pressure": [500.0],
                "choke_size": [32.0],
                "choke_size_uom": ["mm"],
                "dp_choke_size": [0.0],
                "thp": [1000.0],
                "wht": [150.0],
                "flow_kind": ["production"],
            }
        )
        mock_conn.execute.return_value = mock_result
        streamer.duck_conn = mock_conn

        df = streamer._fetch_data_window(datetime(2024, 1, 1), datetime(2024, 1, 2))

        # Verify derived metrics
        assert df["gor"].iloc[0] == 50.0  # 50000 / 1000
        assert abs(df["watercut"].iloc[0] - 0.0909) < 0.01  # 100 / 1100
        assert df["liquid_rate"].iloc[0] == 1100.0  # 1000 + 100

    @patch("stream_service.ProductionStreamer.connect_databases")
    @patch("stream_service.duckdb.connect")
    def test_handles_zero_oil_rate(self, mock_duckdb, mock_connect):
        """Test GOR calculation when oil_rate is zero (division by zero)."""
        streamer = ProductionStreamer()

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.fetchdf.return_value = pd.DataFrame(
            {
                "original_time": [datetime(2024, 1, 1, 10, 0)],
                "well_name": ["F-14"],
                "well_type": ["WI"],
                "oil_rate": [0.0],  # Zero!
                "gas_rate": [10000.0],
                "water_rate": [0.0],
                "water_inj_rate": [5000.0],
                "on_stream_hrs": [1.0],
                "downhole_pressure": [3500.0],
                "downhole_temperature": [210.0],
                "dp_tubing": [120.0],
                "annulus_pressure": [550.0],
                "choke_size": [0.0],
                "choke_size_uom": ["mm"],
                "dp_choke_size": [0.0],
                "thp": [1100.0],
                "wht": [160.0],
                "flow_kind": ["injection"],
            }
        )
        mock_conn.execute.return_value = mock_result
        streamer.duck_conn = mock_conn

        df = streamer._fetch_data_window(datetime(2024, 1, 1), datetime(2024, 1, 2))

        # Should not crash, should return 0
        assert df["gor"].iloc[0] == 0.0


class TestTimeBehindCalculation:
    """Test calculate_time_behind method for catch-up mode detection."""

    @patch("stream_service.datetime")
    @patch("stream_service.ProductionStreamer.connect_databases")
    def test_synchronized_returns_near_zero(self, mock_connect, mock_datetime):
        """When current_timestamp matches expected time, should return ~0."""
        mock_datetime.now.return_value = datetime(2024, 1, 15, 14, 30, tzinfo=ZoneInfo("UTC"))

        streamer = ProductionStreamer()
        streamer.min_date = pd.Timestamp("2007-09-01 00:00:00", tz="UTC")
        streamer.start_offset_days = 365
        streamer.local_tz = ZoneInfo("UTC")

        # Set position to exactly where it should be
        reference_base = streamer.min_date + timedelta(days=365)
        expected_position = reference_base + timedelta(hours=14.5)
        streamer.current_timestamp = expected_position

        hours_behind = streamer.calculate_time_behind()

        assert abs(hours_behind) < 0.1  # Nearly synchronized

    @patch("stream_service.datetime")
    @patch("stream_service.ProductionStreamer.connect_databases")
    def test_detects_when_behind_schedule(self, mock_connect, mock_datetime):
        """When current_timestamp is behind, should return positive hours."""
        mock_datetime.now.return_value = datetime(2024, 1, 15, 14, 30, tzinfo=ZoneInfo("UTC"))

        streamer = ProductionStreamer()
        streamer.min_date = pd.Timestamp("2007-09-01 00:00:00", tz="UTC")
        streamer.start_offset_days = 365
        streamer.local_tz = ZoneInfo("UTC")

        # Set position 5 hours behind
        reference_base = streamer.min_date + timedelta(days=365)
        expected_position = reference_base + timedelta(hours=14.5)
        streamer.current_timestamp = expected_position - timedelta(hours=5)

        hours_behind = streamer.calculate_time_behind()

        assert hours_behind > 4.9  # Approximately 5 hours behind


class TestSignalHandling:
    """Test graceful shutdown."""

    def test_signal_handler_stops_streaming(self):
        """Signal handler should set running=False to stop the loop."""
        streamer = ProductionStreamer()

        assert streamer.running is True

        streamer.signal_handler(2, None)  # SIGINT

        assert streamer.running is False
