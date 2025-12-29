"""
Unit tests for API Service endpoints.

Tests the FastAPI endpoints without requiring actual database connections.
Uses mocks to isolate the API logic from external dependencies.
"""

from unittest.mock import Mock


class TestRootEndpoint:
    """Tests for the root endpoint (/)."""

    def test_root_returns_api_info(self, api_client):
        """Test that root endpoint returns correct API information."""
        response = api_client.get("/")
        assert response.status_code == 200

        data = response.json()
        assert "service" in data
        assert "endpoints" in data
        assert isinstance(data["endpoints"], dict)

    def test_root_lists_endpoints(self, api_client):
        """Test that root endpoint lists all available endpoints."""
        response = api_client.get("/")
        endpoints = response.json()["endpoints"]

        # Verify key endpoints are listed
        assert len(endpoints) > 0
        assert any("wells" in str(v).lower() for v in endpoints.values())
        assert any("health" in str(v).lower() for v in endpoints.values())


class TestHealthCheck:
    """Tests for the health check endpoint (/health)."""

    def test_health_check_healthy(self, api_client, mock_db_connection):
        """Test health endpoint returns healthy status when DB is accessible."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_connection.return_value = mock_conn

        response = api_client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"

    def test_health_check_unhealthy(self, api_client, mock_db_connection):
        """Test health endpoint returns unhealthy when DB connection fails."""
        mock_db_connection.side_effect = Exception("Connection failed")

        response = api_client.get("/health")
        assert response.status_code == 503
        assert response.json()["status"] == "unhealthy"
        assert "error" in response.json()

    def test_health_check_includes_timestamp(self, api_client, mock_db_connection):
        """Test health endpoint includes timestamp."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_connection.return_value = mock_conn

        response = api_client.get("/health")
        assert response.status_code == 200

        data = response.json()
        assert "timestamp" in data
        assert "status" in data


class TestWellsEndpoint:
    """Tests for the wells listing endpoint (/wells)."""

    def test_list_all_wells(self, api_client, mock_db_connection, sample_wells_list):
        """Test fetching all wells without filters."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_connection.return_value = mock_conn
        mock_cursor.fetchall.return_value = sample_wells_list

        response = api_client.get("/wells")
        assert response.status_code == 200

        data = response.json()
        assert "count" in data
        assert "wells" in data
        assert data["count"] == len(sample_wells_list)

    def test_filter_by_producer_wells(self, api_client, mock_db_connection):
        """Test filtering wells by producer type (OP)."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_connection.return_value = mock_conn

        producer_wells = [
            {"well_name": "F-11B", "well_type": "OP"},
            {"well_name": "F-12", "well_type": "OP"},
        ]
        mock_cursor.fetchall.return_value = producer_wells

        response = api_client.get("/wells?well_type=OP")
        assert response.status_code == 200

        data = response.json()
        assert data["count"] == 2
        # Verify all returned wells are producers
        for well in data["wells"]:
            assert well["well_type"] == "OP"

    def test_filter_by_injector_wells(self, api_client, mock_db_connection):
        """Test filtering wells by injector type (WI)."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_connection.return_value = mock_conn

        injector_wells = [
            {"well_name": "F-14", "well_type": "WI"},
            {"well_name": "F-15", "well_type": "WI"},
        ]
        mock_cursor.fetchall.return_value = injector_wells

        response = api_client.get("/wells?well_type=WI")
        assert response.status_code == 200

        data = response.json()
        assert data["count"] == 2
        for well in data["wells"]:
            assert well["well_type"] == "WI"

    def test_empty_result_handling(self, api_client, mock_db_connection):
        """Test wells endpoint handles empty results gracefully."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_connection.return_value = mock_conn
        mock_cursor.fetchall.return_value = []

        response = api_client.get("/wells")
        assert response.status_code == 200

        data = response.json()
        assert data["count"] == 0
        assert data["wells"] == []

    def test_wells_query_execution(self, api_client, mock_db_connection):
        """Test that wells endpoint executes database query."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_connection.return_value = mock_conn
        mock_cursor.fetchall.return_value = []

        response = api_client.get("/wells")
        assert response.status_code == 200

        # Verify that cursor.execute was called (query was executed)
        mock_cursor.execute.assert_called_once()


class TestUtilityFunctions:
    """Tests for utility functions in the API service."""

    def test_clean_nan_values_basic(self):
        """Test cleaning basic NaN values."""
        from api_service import clean_nan_values

        data = {"value": float("nan"), "normal": 10}
        result = clean_nan_values(data)

        assert result["value"] is None
        assert result["normal"] == 10

    def test_clean_nan_values_infinity(self):
        """Test cleaning infinity values."""
        from api_service import clean_nan_values

        data = {"inf": float("inf"), "neg_inf": float("-inf")}
        result = clean_nan_values(data)

        assert result["inf"] is None
        assert result["neg_inf"] is None

    def test_clean_nan_values_nested_structures(self):
        """Test cleaning NaN values in nested data structures."""
        from api_service import clean_nan_values

        data = {
            "nested": {
                "list": [float("nan"), 1, 2],
                "dict": {"nan_val": float("nan"), "normal": 5},
            }
        }
        result = clean_nan_values(data)

        assert result["nested"]["list"][0] is None
        assert result["nested"]["list"][1] == 1
        assert result["nested"]["dict"]["nan_val"] is None
        assert result["nested"]["dict"]["normal"] == 5
