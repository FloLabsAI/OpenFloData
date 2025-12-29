"""
REST API service for querying real-time production data from TimescaleDB.

Provides endpoints to access current and historical production data as if
querying a live offshore production system.
"""

import math
import os
from datetime import datetime
from enum import Enum
from typing import Any

import pendulum
import psycopg2
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from loguru import logger
from psycopg2.extras import RealDictCursor

# Initialize FastAPI app
app = FastAPI(
    title="FloData Streaming API",
    description="Real-time streaming API for Volve field production data",
    version="1.0.0",
)

# Get target timezone from environment (defaults to UTC)
TARGET_TIMEZONE = os.getenv("TZ", "UTC")


def convert_to_local_tz(dt: datetime) -> datetime:
    """
    Convert UTC datetime to the configured timezone (from TZ env variable).

    Args:
        dt: datetime object (assumed to be UTC)

    Returns:
        datetime in target timezone (timezone-aware)
    """
    if dt is None:
        return None

    # Parse with pendulum (handles timezone-aware and naive datetimes)
    pdt = pendulum.instance(dt, tz="UTC")

    # Convert to target timezone
    return pdt.in_timezone(TARGET_TIMEZONE)


def clean_nan_values(obj: Any) -> Any:
    """
    Recursively convert NaN, None, and infinity values to None.
    Also converts datetime objects to timezone-aware strings in the target timezone.
    """
    if isinstance(obj, dict):
        return {k: clean_nan_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan_values(item) for item in obj]
    elif isinstance(obj, datetime):
        # Convert datetime to target timezone and return as ISO string
        converted = convert_to_local_tz(obj)
        return converted.isoformat() if converted else None
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif obj is None:
        return None
    return obj


class WellType(str, Enum):
    """Well type enumeration."""

    PRODUCER = "OP"
    INJECTOR = "WI"
    ALL = "all"


def get_db_connection():
    """Get PostgreSQL database connection."""
    try:
        db_host = os.getenv("DB_HOST", "localhost")

        # Check if using Cloud SQL Unix socket or TCP connection
        if db_host.startswith("/cloudsql/"):
            # Cloud SQL Unix socket connection (Cloud Run)
            logger.info(f"Connecting to DB via Cloud SQL Unix socket: {db_host}")
            conn = psycopg2.connect(
                host=db_host,
                database=os.getenv("DB_NAME", "volve_production"),
                user=os.getenv("DB_USER", "flodata"),
                password=os.getenv("DB_PASSWORD", "flodata_secret"),
                cursor_factory=RealDictCursor,
            )
        else:
            # Regular TCP connection
            logger.info("f Conneting to DB via TCP: {db_host}")
            conn = psycopg2.connect(
                host=db_host,
                port=os.getenv("DB_PORT", "5432"),
                database=os.getenv("DB_NAME", "volve_production"),
                user=os.getenv("DB_USER", "flodata"),
                password=os.getenv("DB_PASSWORD", "flodata_secret"),
                cursor_factory=RealDictCursor,
            )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=503, detail="Database connection failed")


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "FloData Streaming API",
        "version": "1.0.0",
        "description": "Real-time production data from Volve field simulation",
        "endpoints": {
            "GET /wells": "List all wells",
            "GET /wells/{well_name}/latest": "Get latest data for a specific well",
            "GET /wells/{well_name}/history": "Get historical data for a specific well",
            "GET /production/current": "Get current production for all wells",
            "GET /production/timeseries": "Get time-series production data",
            "GET /health": "Health check",
        },
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        logger.info("Health check: connecting to database")
        conn = get_db_connection()
        cursor = conn.cursor()
        logger.info("Health check: executing test query")
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return {"status": "healthy", "timestamp": pendulum.now(TARGET_TIMEZONE).isoformat()}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "unhealthy", "error": str(e)})


@app.get("/wells")
async def list_wells(well_type: WellType = Query(WellType.ALL, description="Filter by well type")):
    """List all wells in the system."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if well_type == WellType.ALL:
            query = """
                SELECT DISTINCT well_name, well_type
                FROM production_data
                ORDER BY well_name
            """
            cursor.execute(query)
        else:
            query = """
                SELECT DISTINCT well_name, well_type
                FROM production_data
                WHERE well_type = %s
                ORDER BY well_name
            """
            cursor.execute(query, (well_type.value,))

        wells = cursor.fetchall()
        return {"count": len(wells), "wells": wells}
    except Exception as e:
        logger.error(f"Error listing wells: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
    finally:
        cursor.close()
        conn.close()


@app.get("/wells/latest")
async def get_well_latest(
    well_name: str = Query(..., description="Well name (e.g., F-14)"),
    period_hours: int = Query(12, ge=1, le=100, description="Get the last X hours of data"),
):
    """Get the latest data point for a specific well. Use query parameter to handle slashes in well names. By default returns last 12 hours of data."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = f"""
            SELECT
                time,
                well_name,
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
            WHERE well_name = %s
            ORDER BY time DESC
            LIMIT {period_hours}
        """
        cursor.execute(query, (well_name,))
        result = cursor.fetchall()

        if not result:
            raise HTTPException(status_code=404, detail=f"Well '{well_name}' not found")

        return clean_nan_values(result)
    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is
    except Exception as e:
        logger.error(f"Error fetching latest data for well '{well_name}': {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
    finally:
        cursor.close()
        conn.close()


@app.get("/wells/history")
async def get_well_history(
    well_name: str = Query(..., description="Well name (e.g., F-12)"),
    hours: int = Query(24, ge=1, le=720, description="Number of hours to retrieve (max 720)"),
    interval: str = Query("1h", description="Time bucket interval (e.g., 1h, 6h, 1d)"),
):
    """Get historical data for a specific well. Use query parameter to handle slashes in well names."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Validate interval format
        valid_intervals = ["1h", "6h", "12h", "1d", "7d"]
        if interval not in valid_intervals:
            raise HTTPException(status_code=400, detail=f"Invalid interval. Must be one of: {valid_intervals}")

        # Use the latest timestamp as reference to handle future-dated data
        query = f"""
            WITH latest_time AS (
                SELECT MAX(time) as max_time FROM production_data WHERE well_name = %s
            )
            SELECT
                time_bucket('{interval}', time) AS time,
                well_name,
                well_type,
                AVG(oil_rate) as oil_rate,
                AVG(gas_rate) as gas_rate,
                AVG(water_rate) as water_rate,
                AVG(water_inj_rate) as water_inj_rate,
                AVG(on_stream_hrs) as on_stream_hrs,
                AVG(downhole_pressure) as downhole_pressure,
                AVG(downhole_temperature) as downhole_temperature,
                AVG(dp_tubing) as dp_tubing,
                AVG(annulus_pressure) as annulus_pressure,
                AVG(choke_size) as choke_size,
                AVG(thp) as thp,
                AVG(wht) as wht,
                AVG(gor) as gor,
                AVG(watercut) as watercut,
                AVG(liquid_rate) as liquid_rate
            FROM production_data, latest_time
            WHERE well_name = %s
              AND time <= latest_time.max_time
              AND time >= latest_time.max_time - INTERVAL '%s hours'
            GROUP BY time_bucket('{interval}', time), well_name, well_type
            ORDER BY time DESC
        """
        cursor.execute(query, (well_name, well_name, hours))
        results = cursor.fetchall()

        if not results:
            raise HTTPException(
                status_code=404, detail=f"No data found for well '{well_name}' in the last {hours} hours"
            )

        return clean_nan_values(
            {
                "well_name": well_name,
                "hours": hours,
                "interval": interval,
                "data_points": len(results),
                "data": [dict(r) for r in results],
            }
        )

    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is
    except Exception as e:
        logger.error(f"Error fetching historical data for well '{well_name}': {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
    finally:
        cursor.close()
        conn.close()


@app.get("/production/current")
async def get_current_production(well_type: WellType = Query(WellType.PRODUCER, description="Filter by well type")):
    """Get current production snapshot for all wells."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if well_type == WellType.ALL:
            query = """
                SELECT * FROM latest_production
                ORDER BY well_name
            """
            cursor.execute(query)
        else:
            query = """
                SELECT * FROM latest_production
                WHERE well_type = %s
                ORDER BY well_name
            """
            cursor.execute(query, (well_type.value,))

        results = cursor.fetchall()

        # Calculate totals
        total_oil = sum(r.get("oil_rate", 0) or 0 for r in results)
        total_gas = sum(r.get("gas_rate", 0) or 0 for r in results)
        total_water = sum(r.get("water_rate", 0) or 0 for r in results)
        total_liquid = total_oil + total_water

        return clean_nan_values(
            {
                "timestamp": pendulum.now(TARGET_TIMEZONE).isoformat(),
                "well_count": len(results),
                "totals": {
                    "oil_rate": round(total_oil, 2),
                    "gas_rate": round(total_gas, 2),
                    "water_rate": round(total_water, 2),
                    "liquid_rate": round(total_liquid, 2),
                    "avg_gor": round(total_gas / total_oil, 2) if total_oil > 0 else 0,
                    "avg_watercut": round(total_water / total_liquid, 4) if total_liquid > 0 else 0,
                },
                "wells": [dict(r) for r in results],
            }
        )

    except Exception as e:
        logger.error(f"Error fetching current production: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
    finally:
        cursor.close()
        conn.close()


@app.get("/production/timeseries")
async def get_production_timeseries(
    hours: int = Query(24, ge=1, le=720, description="Number of hours to retrieve"),
    interval: str = Query("1h", description="Time bucket interval"),
    well_type: WellType = Query(WellType.PRODUCER, description="Filter by well type"),
    aggregate: bool = Query(True, description="Aggregate all wells into single timeseries"),
):
    """Get aggregated production timeseries data."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        valid_intervals = ["1h", "6h", "12h", "1d"]
        if interval not in valid_intervals:
            raise HTTPException(status_code=400, detail=f"Invalid interval. Must be one of: {valid_intervals}")

        if aggregate:
            # Aggregate all wells
            params = []
            where_conditions = []

            # Add time filter (use string interpolation for interval as it's validated input)
            where_conditions.append(f"time >= NOW() - INTERVAL '{hours} hours'")

            # Add well type filter if specified
            if well_type != WellType.ALL:
                where_conditions.append("well_type = %s")
                params.append(well_type.value)

            where_clause = "WHERE " + " AND ".join(where_conditions)

            query = f"""
                SELECT
                    time_bucket('{interval}', time) AS time,
                    SUM(oil_rate) as total_oil_rate,
                    SUM(gas_rate) as total_gas_rate,
                    SUM(water_rate) as total_water_rate,
                    AVG(gor) as avg_gor,
                    AVG(watercut) as avg_watercut,
                    COUNT(DISTINCT well_name) as well_count
                FROM production_data
                {where_clause}
                GROUP BY time_bucket('{interval}', time)
                ORDER BY time DESC
            """
            cursor.execute(query, tuple(params))
        else:
            # Per-well timeseries
            params = []
            where_conditions = []

            # Add time filter (use string interpolation for interval as it's validated input)
            where_conditions.append(f"time >= NOW() - INTERVAL '{hours} hours'")

            # Add well type filter if specified
            if well_type != WellType.ALL:
                where_conditions.append("well_type = %s")
                params.append(well_type.value)

            where_clause = "WHERE " + " AND ".join(where_conditions)

            query = f"""
                SELECT
                    time_bucket('{interval}', time) AS time,
                    well_name,
                    AVG(oil_rate) as oil_rate,
                    AVG(gas_rate) as gas_rate,
                    AVG(water_rate) as water_rate,
                    AVG(gor) as gor,
                    AVG(watercut) as watercut
                FROM production_data
                {where_clause}
                GROUP BY time_bucket('{interval}', time), well_name
                ORDER BY time DESC, well_name
            """
            cursor.execute(query, tuple(params))

        results = cursor.fetchall()

        return clean_nan_values(
            {
                "hours": hours,
                "interval": interval,
                "well_type": well_type.value,
                "aggregate": aggregate,
                "data_points": len(results),
                "data": [dict(r) for r in results],
            }
        )

    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is
    except Exception as e:
        logger.error(f"Error fetching production timeseries: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

    finally:
        cursor.close()
        conn.close()


@app.get("/stats")
async def get_database_stats():
    """Get database statistics and system info."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Get record count
        cursor.execute("SELECT COUNT(*) as total_records FROM production_data")
        total_records = cursor.fetchone()["total_records"]

        # Get time range
        cursor.execute("""
            SELECT
                MIN(time) as earliest,
                MAX(time) as latest
            FROM production_data
        """)
        time_range = cursor.fetchone()

        # Get well count
        cursor.execute("SELECT COUNT(DISTINCT well_name) as well_count FROM production_data")
        well_count = cursor.fetchone()["well_count"]

        # Get latest update
        cursor.execute("SELECT MAX(time) as last_update FROM production_data")
        last_update = cursor.fetchone()["last_update"]

        return {
            "database": {
                "total_records": total_records,
                "well_count": well_count,
                "earliest_data": time_range["earliest"],
                "latest_data": time_range["latest"],
                "last_update": last_update,
            },
            "streaming": {"status": "active", "simulated_time": last_update},
        }

    except Exception as e:
        logger.error(f"Error fetching database stats: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
