"""
Integration test fixtures using testcontainers for real databases.

These fixtures provide real PostgreSQL and DuckDB instances for integration testing.
Tests run against actual databases to catch issues that mocks can't detect.
"""

import pytest
import psycopg2
from testcontainers.postgres import PostgresContainer
import time
from loguru import logger


@pytest.fixture(scope="session")
def postgres_container():
    """
    Provide a PostgreSQL testcontainer for the entire test session.

    This spins up a real PostgreSQL instance in Docker for integration tests.
    The container is shared across all tests to improve performance.
    """
    logger.info("Starting PostgreSQL testcontainer...")

    # Start PostgreSQL container with TimescaleDB extension
    postgres = PostgresContainer(
        image="timescale/timescaledb:latest-pg16", port=5432, username="testuser", password="testpass", dbname="testdb"
    )

    postgres.start()

    # Wait for PostgreSQL to be ready
    connection_url = postgres.get_connection_url()
    # Fix connection URL format (remove +psycopg2 if present)
    connection_url = connection_url.replace("postgresql+psycopg2://", "postgresql://")
    logger.info(f"PostgreSQL container started: {connection_url}")

    # Create TimescaleDB extension
    try:
        conn = psycopg2.connect(connection_url)
        cursor = conn.cursor()
        cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("TimescaleDB extension created")
    except Exception as e:
        logger.warning(f"Could not create TimescaleDB extension: {e}")

    yield postgres

    # Cleanup
    logger.info("Stopping PostgreSQL testcontainer...")
    postgres.stop()


@pytest.fixture
def postgres_connection(postgres_container):
    """
    Provide a fresh PostgreSQL connection for each test.

    Creates tables and provides a clean database state for each test.
    """
    connection_url = postgres_container.get_connection_url()
    # Fix connection URL format
    connection_url = connection_url.replace("postgresql+psycopg2://", "postgresql://")
    conn = psycopg2.connect(connection_url)

    # Create production_data table
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS production_data (
            time TIMESTAMPTZ NOT NULL,
            well_name TEXT NOT NULL,
            well_type TEXT,
            oil_rate DOUBLE PRECISION,
            gas_rate DOUBLE PRECISION,
            water_rate DOUBLE PRECISION,
            water_inj_rate DOUBLE PRECISION,
            on_stream_hrs DOUBLE PRECISION,
            downhole_pressure DOUBLE PRECISION,
            downhole_temperature DOUBLE PRECISION,
            dp_tubing DOUBLE PRECISION,
            annulus_pressure DOUBLE PRECISION,
            choke_size DOUBLE PRECISION,
            thp DOUBLE PRECISION,
            wht DOUBLE PRECISION,
            gor DOUBLE PRECISION,
            watercut DOUBLE PRECISION,
            liquid_rate DOUBLE PRECISION
        );
    """)

    # Try to create hypertable (may fail if not TimescaleDB)
    try:
        cursor.execute("""
            SELECT create_hypertable('production_data', 'time',
                                     if_not_exists => TRUE);
        """)
        logger.info("Hypertable created")
    except Exception as e:
        logger.warning(f"Could not create hypertable: {e}")
        conn.rollback()

    # Create materialized view for latest production data
    cursor.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS latest_production AS
        SELECT DISTINCT ON (well_name)
            well_name,
            time,
            well_type,
            oil_rate,
            gas_rate,
            water_rate,
            water_inj_rate,
            on_stream_hrs,
            downhole_pressure,
            downhole_temperature,
            dp_tubing,
            annulus_pressure,
            choke_size,
            thp,
            wht,
            gor,
            watercut,
            liquid_rate
        FROM production_data
        ORDER BY well_name, time DESC;
    """)
    logger.info("latest_production view created")

    conn.commit()

    yield conn

    # Cleanup - drop view and truncate table for next test
    cursor.execute("DROP MATERIALIZED VIEW IF EXISTS latest_production;")
    cursor.execute("TRUNCATE TABLE production_data;")
    conn.commit()
    cursor.close()
    conn.close()


@pytest.fixture
def postgres_connection_url(postgres_container):
    """Provide the PostgreSQL connection URL."""
    url = postgres_container.get_connection_url()
    # Fix connection URL format
    return url.replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture
def sample_test_data(postgres_connection):
    """
    Insert sample test data into PostgreSQL for integration tests.

    Returns connection with pre-populated test data.
    """
    cursor = postgres_connection.cursor()

    # Insert sample production data
    test_records = [
        (
            "2024-01-01 00:00:00+00",
            "F-11B",
            "OP",
            1500.0,
            50000.0,
            100.0,
            0.0,
            1.0,
            3000.0,
            200.0,
            100.0,
            500.0,
            32.0,
            1000.0,
            150.0,
            33.33,
            0.0625,
            1600.0,
        ),
        (
            "2024-01-01 01:00:00+00",
            "F-11B",
            "OP",
            1450.0,
            48000.0,
            120.0,
            0.0,
            1.0,
            2980.0,
            198.0,
            98.0,
            495.0,
            32.0,
            990.0,
            148.0,
            33.10,
            0.0764,
            1570.0,
        ),
        (
            "2024-01-01 02:00:00+00",
            "F-11B",
            "OP",
            1480.0,
            49000.0,
            110.0,
            0.0,
            1.0,
            2990.0,
            199.0,
            99.0,
            498.0,
            32.0,
            995.0,
            149.0,
            33.11,
            0.0692,
            1590.0,
        ),
        (
            "2024-01-01 00:00:00+00",
            "F-12",
            "OP",
            1000.0,
            40000.0,
            200.0,
            0.0,
            1.0,
            2800.0,
            195.0,
            95.0,
            480.0,
            28.0,
            950.0,
            145.0,
            40.0,
            0.1667,
            1200.0,
        ),
        (
            "2024-01-01 00:00:00+00",
            "F-14",
            "WI",
            0.0,
            0.0,
            0.0,
            5000.0,
            1.0,
            3500.0,
            210.0,
            120.0,
            550.0,
            0.0,
            1100.0,
            160.0,
            0.0,
            0.0,
            0.0,
        ),
    ]

    insert_query = """
        INSERT INTO production_data (
            time, well_name, well_type, oil_rate, gas_rate, water_rate, water_inj_rate,
            on_stream_hrs, downhole_pressure, downhole_temperature, dp_tubing,
            annulus_pressure, choke_size, thp, wht, gor, watercut, liquid_rate
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cursor.executemany(insert_query, test_records)
    postgres_connection.commit()

    # Refresh materialized view to include new test data
    cursor.execute("REFRESH MATERIALIZED VIEW latest_production;")
    postgres_connection.commit()

    logger.info(f"Inserted {len(test_records)} test records")

    yield postgres_connection
