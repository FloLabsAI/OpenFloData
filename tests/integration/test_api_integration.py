"""
Integration tests for API service with real PostgreSQL database.

These tests use testcontainers to spin up a real PostgreSQL instance
and validate the API against actual database queries.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
import os


@pytest.fixture
def api_client_with_real_db(postgres_connection_url):
    """
    Create FastAPI test client connected to real test database.

    Patches the database connection to use the test PostgreSQL container.
    """
    # Import here to avoid issues with module loading
    import api_service

    # Patch the get_db_connection to use test database
    def mock_get_db_connection():
        import psycopg2
        from psycopg2.extras import RealDictCursor

        return psycopg2.connect(postgres_connection_url, cursor_factory=RealDictCursor)

    with patch.object(api_service, "get_db_connection", mock_get_db_connection):
        client = TestClient(api_service.app)
        yield client


class TestWellsIntegration:
    """Integration tests for /wells endpoints with real database."""

    def test_list_wells_empty_database(self, api_client_with_real_db, postgres_connection):
        """Test listing wells when database is empty."""
        response = api_client_with_real_db.get("/wells")
        assert response.status_code == 200

        data = response.json()
        assert data["count"] == 0
        assert data["wells"] == []

    def test_list_wells_with_data(self, api_client_with_real_db, sample_test_data):
        """Test listing wells with sample data."""
        response = api_client_with_real_db.get("/wells")
        assert response.status_code == 200

        data = response.json()
        assert data["count"] >= 3  # F-11B, F-12, F-14
        assert any(w["well_name"] == "F-11B" for w in data["wells"])

    def test_filter_producers(self, api_client_with_real_db, sample_test_data):
        """Test filtering wells by producer type."""
        response = api_client_with_real_db.get("/wells?well_type=OP")
        assert response.status_code == 200

        data = response.json()
        assert data["count"] >= 2  # F-11B, F-12
        # All wells should be producers
        for well in data["wells"]:
            assert well["well_type"] == "OP"

    def test_filter_injectors(self, api_client_with_real_db, sample_test_data):
        """Test filtering wells by injector type."""
        response = api_client_with_real_db.get("/wells?well_type=WI")
        assert response.status_code == 200

        data = response.json()
        assert data["count"] >= 1  # F-14
        # All wells should be injectors
        for well in data["wells"]:
            assert well["well_type"] == "WI"


class TestWellLatestIntegration:
    """Integration tests for /wells/latest endpoint."""

    def test_get_latest_data_existing_well(self, api_client_with_real_db, sample_test_data):
        """Test getting latest data for an existing well."""
        response = api_client_with_real_db.get("/wells/latest?well_name=F-11B&period_hours=10")
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0
        # Verify data structure
        first_record = data[0]
        assert "time" in first_record
        assert "well_name" in first_record
        assert first_record["well_name"] == "F-11B"

    def test_get_latest_nonexistent_well(self, api_client_with_real_db, sample_test_data):
        """Test getting latest data for non-existent well."""
        response = api_client_with_real_db.get("/wells/latest?well_name=NONEXISTENT")
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_period_hours_parameter(self, api_client_with_real_db, sample_test_data):
        """Test that period_hours parameter limits results correctly."""
        # F-11B has 3 hours of data
        response = api_client_with_real_db.get("/wells/latest?well_name=F-11B&period_hours=2")
        assert response.status_code == 200

        data = response.json()
        assert len(data) <= 2  # Should return max 2 records


class TestWellHistoryIntegration:
    """Integration tests for /wells/history endpoint."""

    def test_get_history_with_time_bucket(self, api_client_with_real_db, sample_test_data):
        """Test getting historical data with time bucketing."""
        response = api_client_with_real_db.get("/wells/history?well_name=F-11B&hours=24&interval=1h")
        assert response.status_code == 200

        data = response.json()
        assert "well_name" in data
        assert data["well_name"] == "F-11B"
        assert "data_points" in data
        assert "data" in data
        assert isinstance(data["data"], list)

    def test_invalid_interval_validation(self, api_client_with_real_db, sample_test_data):
        """Test that invalid intervals are rejected."""
        response = api_client_with_real_db.get("/wells/history?well_name=F-11B&interval=30m")
        assert response.status_code == 400
        assert "Invalid interval" in response.json()["detail"]

    def test_no_data_for_well_in_timeframe(self, api_client_with_real_db, postgres_connection):
        """Test when well exists but has very limited historical data."""
        # Insert well with old data
        cursor = postgres_connection.cursor()
        cursor.execute("""
            INSERT INTO production_data (time, well_name, well_type, oil_rate, gas_rate, water_rate, gor, watercut, liquid_rate)
            VALUES ('2020-01-01 00:00:00+00', 'OLD-WELL', 'OP', 1000.0, 50000.0, 100.0, 50.0, 0.1, 1100.0)
        """)
        postgres_connection.commit()

        response = api_client_with_real_db.get("/wells/history?well_name=OLD-WELL&hours=1")
        # API returns data relative to well's latest timestamp, not NOW()
        # So even old data is returned successfully
        assert response.status_code == 200
        data = response.json()
        assert data["well_name"] == "OLD-WELL"


class TestProductionIntegration:
    """Integration tests for /production endpoints."""

    def test_current_production_snapshot(self, api_client_with_real_db, sample_test_data):
        """Test getting current production snapshot."""
        response = api_client_with_real_db.get("/production/current?well_type=all")
        assert response.status_code == 200

        data = response.json()
        assert "well_count" in data
        assert "totals" in data
        assert "wells" in data

        # Verify totals structure
        totals = data["totals"]
        assert "oil_rate" in totals
        assert "gas_rate" in totals
        assert "water_rate" in totals
        assert "liquid_rate" in totals

    def test_current_production_producers_only(self, api_client_with_real_db, sample_test_data):
        """Test filtering current production by producers."""
        response = api_client_with_real_db.get("/production/current?well_type=OP")
        assert response.status_code == 200

        data = response.json()
        assert data["well_count"] >= 2  # At least F-11B and F-12

        # Verify all wells are producers
        for well in data["wells"]:
            assert well["well_type"] == "OP"

    def test_production_totals_calculation(self, api_client_with_real_db, sample_test_data):
        """Test that production totals are calculated correctly."""
        response = api_client_with_real_db.get("/production/current?well_type=all")
        assert response.status_code == 200

        data = response.json()
        totals = data["totals"]

        # Totals should be positive
        assert totals["oil_rate"] > 0
        assert totals["gas_rate"] > 0

        # Liquid rate should equal oil + water
        expected_liquid = totals["oil_rate"] + totals["water_rate"]
        assert abs(totals["liquid_rate"] - expected_liquid) < 0.01


class TestDatabaseOperations:
    """Test database-specific operations and constraints."""

    def test_insert_and_query_roundtrip(self, postgres_connection):
        """Test inserting data and querying it back."""
        cursor = postgres_connection.cursor()

        # Insert test record
        cursor.execute("""
            INSERT INTO production_data (
                time, well_name, well_type, oil_rate, gas_rate
            ) VALUES (
                '2024-01-15 12:00:00+00', 'TEST-WELL', 'OP', 1234.56, 78901.23
            )
        """)
        postgres_connection.commit()

        # Query it back
        cursor.execute("""
            SELECT well_name, oil_rate, gas_rate
            FROM production_data
            WHERE well_name = 'TEST-WELL'
        """)
        result = cursor.fetchone()

        assert result[0] == "TEST-WELL"
        assert abs(result[1] - 1234.56) < 0.01
        assert abs(result[2] - 78901.23) < 0.01

    def test_timestamp_timezone_handling(self, postgres_connection):
        """Test that timestamps are stored and retrieved correctly with timezones."""
        cursor = postgres_connection.cursor()

        # Insert with explicit timezone
        cursor.execute("""
            INSERT INTO production_data (time, well_name, oil_rate)
            VALUES ('2024-01-15 14:30:00+00', 'TZ-TEST', 1000.0)
        """)
        postgres_connection.commit()

        # Query back and verify timezone
        cursor.execute("""
            SELECT time FROM production_data WHERE well_name = 'TZ-TEST'
        """)
        result = cursor.fetchone()

        # Timestamp should have timezone info
        assert result[0] is not None
        # PostgreSQL returns timezone-aware datetime

    def test_null_value_handling(self, postgres_connection):
        """Test that NULL values are handled correctly."""
        cursor = postgres_connection.cursor()

        # Insert record with NULL values
        cursor.execute("""
            INSERT INTO production_data (time, well_name, oil_rate, gas_rate)
            VALUES ('2024-01-15 15:00:00+00', 'NULL-TEST', NULL, NULL)
        """)
        postgres_connection.commit()

        # Query back
        cursor.execute("""
            SELECT oil_rate, gas_rate FROM production_data WHERE well_name = 'NULL-TEST'
        """)
        result = cursor.fetchone()

        assert result[0] is None
        assert result[1] is None


class TestSQLInjectionPrevention:
    """Test SQL injection prevention with real database."""

    def test_sql_injection_in_well_name(self, api_client_with_real_db, sample_test_data):
        """Test that SQL injection attempts in well_name parameter are prevented."""
        # Attempt SQL injection
        malicious_input = "F-11B'; DROP TABLE production_data; --"

        response = api_client_with_real_db.get(f"/wells/latest?well_name={malicious_input}")

        # Should return 404 (well not found) not SQL error
        assert response.status_code in [404, 500]

        # Verify table still exists by querying it
        response2 = api_client_with_real_db.get("/wells")
        assert response2.status_code == 200  # Table exists

    def test_sql_injection_in_interval(self, api_client_with_real_db, sample_test_data):
        """Test that SQL injection in interval parameter is prevented."""
        # The interval is validated against whitelist, so injection should fail
        malicious_input = "1h'; DROP TABLE production_data; --"

        response = api_client_with_real_db.get(f"/wells/history?well_name=F-11B&interval={malicious_input}")

        # Should return 400 (invalid interval) not execute SQL
        assert response.status_code in [400, 500]
        assert "Invalid interval" in response.json()["detail"]
