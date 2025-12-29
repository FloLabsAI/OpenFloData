"""
Shared test fixtures for FloData testing suite.

This module contains pytest fixtures that are shared across all test modules.
Fixtures are automatically discovered by pytest when placed in conftest.py.
"""

from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def api_client():
    """
    FastAPI test client fixture for testing API endpoints.

    Returns:
        TestClient: FastAPI test client instance
    """
    from api_service import app

    return TestClient(app)


@pytest.fixture
def mock_db_connection():
    """
    Mock database connection for unit tests.

    This fixture mocks the get_db_connection function to prevent
    actual database connections during unit tests.

    Yields:
        Mock: Mocked database connection
    """
    with patch("api_service.get_db_connection") as mock:
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock.return_value = mock_conn
        yield mock


@pytest.fixture
def mock_db_cursor(mock_db_connection):
    """
    Mock database cursor for unit tests.

    Args:
        mock_db_connection: The mocked database connection

    Returns:
        Mock: Mocked database cursor
    """
    return mock_db_connection.return_value.cursor.return_value


@pytest.fixture
def sample_wells_list():
    """
    Generate sample list of wells for testing.

    Returns:
        list: List of well dictionaries
    """
    return [
        {"well_name": "F-11B", "well_type": "OP"},
        {"well_name": "F-12", "well_type": "OP"},
        {"well_name": "F-4", "well_type": "OP"},
        {"well_name": "F-14", "well_type": "WI"},
        {"well_name": "F-15", "well_type": "WI"},
    ]
