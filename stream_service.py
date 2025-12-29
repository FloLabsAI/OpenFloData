"""
Real-time streaming service that simulates live production data from Volve field.

This service reads historical hourly data from DuckDB and streams it to TimescaleDB
with time-shifted timestamps to simulate real-time offshore production data.
"""

import os
import signal
import sys
import time
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import psycopg2
from loguru import logger
from psycopg2.extras import execute_values

# Configure logger
logger.remove()
logger.add(
    sys.stdout,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
)


class ProductionStreamer:
    """Streams historical production data as if it's happening in real-time."""

    def __init__(self):
        # Database configuration
        self.db_host = os.getenv("DB_HOST", "localhost")
        self.db_port = os.getenv("DB_PORT", "5432")
        self.db_name = os.getenv("DB_NAME", "volve_production")
        self.db_user = os.getenv("DB_USER", "flodata")
        self.db_password = os.getenv("DB_PASSWORD", "flodata_secret")

        # Streaming configuration
        self.start_offset_days = int(os.getenv("START_OFFSET_DAYS", "0"))
        self.batch_size_hours = int(os.getenv("BATCH_SIZE_HOURS", "168"))  # 7 days per batch during historical load
        self.catchup_threshold_hours = int(
            os.getenv("CATCHUP_THRESHOLD_HOURS", "2")
        )  # Enter catch-up mode if > X hours behind
        self.catchup_batch_hours = int(
            os.getenv("CATCHUP_BATCH_HOURS", "24")
        )  # Stream X hours per batch during catch-up

        # Timezone configuration
        tz_name = os.getenv("TZ", "UTC")
        self.local_tz = ZoneInfo(tz_name)

        # DuckDB source
        self.duckdb_path = os.getenv("DUCKDB_PATH", "/app/data/prod_hourly_data.duckdb")

        # State
        self.running = True
        self.current_timestamp: datetime | None = None
        self.reference_date: datetime | None = None  # Date in source data that maps to "now"
        self.min_date: datetime | None = None
        self.max_date: datetime | None = None
        self.streaming_mode = False  # False = batch mode, True = real-time mode
        self.pg_conn = None
        self.duck_conn = None

        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    def connect_databases(self):
        """Establish connections to DuckDB and PostgreSQL."""
        logger.info("Connecting to databases...")

        # Connect to DuckDB
        try:
            self.duck_conn = duckdb.connect(self.duckdb_path, read_only=True)
            logger.success(f"Connected to DuckDB: {self.duckdb_path}")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise

        # Connect to PostgreSQL/TimescaleDB
        max_retries = 30
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                # Check if using Cloud SQL Unix socket or TCP connection
                if self.db_host.startswith("/cloudsql/"):
                    # Cloud SQL Unix socket connection (Cloud Run)
                    self.pg_conn = psycopg2.connect(
                        host=self.db_host,
                        database=self.db_name,
                        user=self.db_user,
                        password=self.db_password,
                        connect_timeout=10,  # 10 second connection timeout
                    )
                    logger.success(f"Connected to PostgreSQL via Cloud SQL socket: {self.db_host}")
                else:
                    # Regular TCP connection
                    self.pg_conn = psycopg2.connect(
                        host=self.db_host,
                        port=self.db_port,
                        database=self.db_name,
                        user=self.db_user,
                        password=self.db_password,
                        connect_timeout=10,  # 10 second connection timeout
                    )
                    logger.success(f"Connected to PostgreSQL at {self.db_host}:{self.db_port}")
                break
            except psycopg2.OperationalError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Failed to connect to PostgreSQL (attempt {attempt + 1}/{max_retries}): {e}")
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to PostgreSQL after all retries")
                    raise

    def get_data_range(self):
        """Get the date range from the source data."""
        query = "SELECT MIN(DATEPRD) as min_date, MAX(DATEPRD) as max_date FROM prod_hourly_data"
        result = self.duck_conn.execute(query).fetchdf()
        self.min_date = pd.Timestamp(result["min_date"].iloc[0])
        self.max_date = pd.Timestamp(result["max_date"].iloc[0])
        return self.min_date, self.max_date

    def initialize_stream_position(self):
        """Determine where to start streaming from and what mode to use."""
        min_date, max_date = self.get_data_range()

        total_days = (max_date - min_date).days
        logger.info("┌─ Source Data Information ───────────────────────────────────┐")
        logger.info(f"│ Range: {min_date.date()} to {max_date.date()}                             │")
        logger.info(f"│ Total: {total_days} days of production data                         │")
        logger.info("└─────────────────────────────────────────────────────────────┘")

        # Set reference date (the date in source data that maps to "today 00:00")
        self.reference_date = min_date + timedelta(days=self.start_offset_days)
        if self.reference_date > max_date:
            logger.warning(f"START_OFFSET_DAYS ({self.start_offset_days}) exceeds data range. Using end date.")
            self.reference_date = max_date

        # Check if there's already data in TimescaleDB
        cursor = self.pg_conn.cursor()
        cursor.execute("SELECT MAX(time), MAX(original_time) FROM production_data")
        result = cursor.fetchone()
        last_timestamp = result[0]
        last_original_time = result[1]
        cursor.close()

        if last_timestamp:
            logger.info(f"Found existing data in TimescaleDB (last timestamp: {last_timestamp})")
            logger.info("Resuming streaming from where we left off...")
            # Use the stored original_time instead of calculating it backwards
            self.current_timestamp = pd.Timestamp(last_original_time)
            self.streaming_mode = True  # Already have historical data, go straight to streaming
            logger.info(f"Resuming from original timestamp: {self.current_timestamp.date()}")
        else:
            # Starting fresh - need to determine if we should batch load or stream
            self.current_timestamp = min_date

            if self.start_offset_days > 0:
                # Batch mode: load historical data quickly up to current hour
                self.streaming_mode = False

                # Calculate how many hours into "today" we are
                now_local = datetime.now(self.local_tz)
                hours_elapsed_today = now_local.hour

                # Batch load endpoint: reference_date + hours elapsed today
                batch_end_time = self.reference_date + timedelta(hours=hours_elapsed_today)
                self.reference_date = batch_end_time  # Update reference to current hour

                logger.info("")
                logger.info("┌─ Batch Loading Mode ────────────────────────────────────────")
                logger.info(f"│ START_OFFSET_DAYS set to {self.start_offset_days} days")
                logger.info(f"│ Current time: {now_local.strftime('%Y-%m-%d %H:%M %Z')}")
                logger.info(f"│ Hours elapsed today: {hours_elapsed_today}")
                logger.info("│ ")
                logger.info("│ Strategy:")
                logger.info(f"│   1. BATCH LOAD: {min_date.date()} → {batch_end_time}")
                logger.info("│      (Fast-load historical data up to current hour)")
                logger.info("│   2. REAL-TIME:  Stream 1 hour at each hour boundary")
                logger.info("│      (Synchronized to real-world clock)")
                logger.info("└─────────────────────────────────────────────────────────────")
            else:
                # Stream from the beginning
                self.streaming_mode = True
                logger.info(f"Starting real-time streaming from: {self.current_timestamp.date()}")

    def batch_load_historical_data(self) -> bool:
        """
        Quickly load historical data from min_date to reference_date.
        Returns True if batch load completed, False if interrupted.
        """
        logger.info("")
        logger.info("Starting batch load of historical data...")

        batch_start = self.current_timestamp
        batch_end = self.reference_date
        total_hours = int((batch_end - batch_start).total_seconds() / 3600)

        logger.info(f"Loading {total_hours} hours of data in batches of {self.batch_size_hours} hours...")

        loaded_hours = 0
        batch_count = 0

        while self.current_timestamp < self.reference_date and self.running:
            # Calculate batch window
            next_timestamp = min(self.current_timestamp + timedelta(hours=self.batch_size_hours), self.reference_date)

            # Fetch data for this batch
            df = self._fetch_data_window(self.current_timestamp, next_timestamp)

            if df is None or df.empty:
                logger.warning(f"No data found for period {self.current_timestamp} to {next_timestamp}")
                self.current_timestamp = next_timestamp
                continue

            # Time-shift to map source data to real-world clock
            # Formula: offset_time = today_00:00 + (original_time - reference_date_base)
            today_local = datetime.now(self.local_tz).replace(hour=0, minute=0, second=0, microsecond=0)
            today_utc = today_local.astimezone(UTC)
            reference_date_base = (self.min_date + timedelta(days=self.start_offset_days)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            reference_date_base_tz = (
                reference_date_base if reference_date_base.tzinfo else pd.Timestamp(reference_date_base, tz="UTC")
            )

            # Calculate offset: each hour in source after reference maps to same hour today
            time_shift = today_utc - reference_date_base_tz
            df["time"] = pd.to_datetime(df["original_time"], utc=True) + time_shift

            # Insert batch
            self._insert_batch_silent(df)

            # Update progress
            batch_count += 1
            # Calculate actual hours loaded (time span, not record count)
            batch_hours = int((next_timestamp - batch_start).total_seconds() / 3600)
            loaded_hours = batch_hours
            self.current_timestamp = next_timestamp
            progress_pct = (loaded_hours / total_hours) * 100 if total_hours > 0 else 100

            logger.info(
                f"Batch {batch_count}: Loaded {len(df)} records | "
                f"Original: {df['original_time'].min().date()} to {df['original_time'].max().date()} | "
                f"Progress: {progress_pct:.1f}%"
            )

        if self.current_timestamp >= self.reference_date:
            logger.success("")
            logger.success(f"✓ Batch load complete! Loaded {loaded_hours} hours of historical data")
            logger.success(f"  Original timeline: {batch_start.date()} → {self.reference_date.date()}")
            logger.success("  Now switching to real-time streaming mode...")
            logger.success("")
            return True

        return False  # Interrupted

    def _fetch_data_window(self, start_ts, end_ts) -> pd.DataFrame | None:
        """
        Internal method to fetch data from DuckDB for a specific time window.
        Returns DataFrame with derived metrics calculated.
        """
        query = f"""
            SELECT
                DATEPRD as original_time,
                NPD_WELL_BORE_NAME as well_name,
                WELL_TYPE as well_type,
                BORE_OIL_VOL as oil_rate,
                BORE_GAS_VOL as gas_rate,
                BORE_WAT_VOL as water_rate,
                BORE_WI_VOL as water_inj_rate,
                ON_STREAM_HRS as on_stream_hrs,
                AVG_DOWNHOLE_PRESSURE as downhole_pressure,
                AVG_DOWNHOLE_TEMPERATURE as downhole_temperature,
                AVG_DP_TUBING as dp_tubing,
                AVG_ANNULUS_PRESS as annulus_pressure,
                AVG_CHOKE_SIZE_P as choke_size,
                AVG_CHOKE_UOM as choke_size_uom,
                DP_CHOKE_SIZE as dp_choke_size,
                AVG_WHP_P as thp,
                AVG_WHT_P as wht,
                FLOW_KIND as flow_kind
            FROM prod_hourly_data
            WHERE DATEPRD > '{start_ts}'
              AND DATEPRD <= '{end_ts}'
            ORDER BY DATEPRD
        """

        df = self.duck_conn.execute(query).fetchdf()

        if df.empty:
            return None

        # Clean up well names: remove "15/9-" prefix and clean spaces
        # Examples: "15/9-F-11 B" → "F-11B", "15/9-F-4 AH" → "F-4AH"
        df["well_name"] = df["well_name"].str.replace("15/9-", "", regex=False)
        df["well_name"] = df["well_name"].str.replace(" ", "", regex=False)

        # Calculate derived metrics
        df["gor"] = df["gas_rate"] / df["oil_rate"].replace(0, float("nan"))
        df["watercut"] = df["water_rate"] / (df["water_rate"] + df["oil_rate"]).replace(0, float("nan"))
        df["liquid_rate"] = df["oil_rate"] + df["water_rate"]

        # Fill NaN values
        df["gor"] = df["gor"].fillna(0)
        df["watercut"] = df["watercut"].fillna(0)

        return df

    def fetch_next_batch(self, hours: int = 1) -> pd.DataFrame | None:
        """
        Fetch the next batch of data for streaming.

        Args:
            hours: Number of hours to fetch (1 for real-time, more for catch-up)
        """
        if self.current_timestamp is None:
            return None

        # Fetch specified number of hours
        next_timestamp = self.current_timestamp + timedelta(hours=hours)

        # Fetch data for this time window
        df = self._fetch_data_window(self.current_timestamp, next_timestamp)

        if df is None or df.empty:
            logger.warning("No more data to stream. Reached end of dataset.")
            return None

        # Time-shift to map source data to real-world clock (same as batch load)
        # Formula: offset_time = today_00:00 + (original_time - reference_date_base)
        today_local = datetime.now(self.local_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        today_utc = today_local.astimezone(UTC)
        reference_date_base = (self.min_date + timedelta(days=self.start_offset_days)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        reference_date_base_tz = (
            reference_date_base if reference_date_base.tzinfo else pd.Timestamp(reference_date_base, tz="UTC")
        )

        time_shift = today_utc - reference_date_base_tz
        df["time"] = pd.to_datetime(df["original_time"], utc=True) + time_shift

        # Update position
        self.current_timestamp = next_timestamp

        return df

    def calculate_time_behind(self) -> float:
        """
        Calculate how many hours behind the current timestamp is from "now".
        Returns the gap in hours (negative if ahead, positive if behind).
        """
        if self.current_timestamp is None:
            return 0.0

        # Calculate what "now" should be in source data timeline
        # Formula: reference_date_base + (now - today_00:00)
        today_local = datetime.now(self.local_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        now_local = datetime.now(self.local_tz)
        hours_elapsed_today = (now_local - today_local).total_seconds() / 3600

        reference_date_base = (self.min_date + timedelta(days=self.start_offset_days)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        expected_timestamp = reference_date_base + timedelta(hours=hours_elapsed_today)

        # Convert to timezone-aware for comparison
        if expected_timestamp.tzinfo is None:
            expected_timestamp = pd.Timestamp(expected_timestamp, tz="UTC")
        if self.current_timestamp.tzinfo is None:
            current_ts = pd.Timestamp(self.current_timestamp, tz="UTC")
        else:
            current_ts = self.current_timestamp

        # Calculate gap in hours
        gap_hours = (expected_timestamp - current_ts).total_seconds() / 3600
        return gap_hours

    def _insert_batch_silent(self, df: pd.DataFrame):
        """Insert a batch of data into TimescaleDB (without verbose logging)."""
        if df.empty:
            return

        # Prepare data for insertion
        columns = [
            "time",
            "original_time",
            "well_name",
            "well_type",
            "oil_rate",
            "gas_rate",
            "water_rate",
            "water_inj_rate",
            "on_stream_hrs",
            "downhole_pressure",
            "downhole_temperature",
            "dp_tubing",
            "annulus_pressure",
            "choke_size",
            "choke_size_uom",
            "dp_choke_size",
            "thp",
            "wht",
            "flow_kind",
            "gor",
            "watercut",
            "liquid_rate",
        ]

        # Convert DataFrame to list of tuples
        values = [tuple(row) for row in df[columns].values]

        # Insert using execute_values for efficiency
        cursor = self.pg_conn.cursor()
        try:
            execute_values(
                cursor,
                f"""
                INSERT INTO production_data ({", ".join(columns)})
                VALUES %s
                ON CONFLICT DO NOTHING
                """,
                values,
                page_size=1000,
            )
            self.pg_conn.commit()

            # Refresh materialized view
            try:
                cursor.execute("REFRESH MATERIALIZED VIEW latest_production")
                self.pg_conn.commit()
            except psycopg2.errors.InsufficientPrivilege:
                # Silently skip if no permissions - batch loading should continue
                self.pg_conn.rollback()
            except Exception:
                # Silently skip other refresh errors during batch loading
                self.pg_conn.rollback()
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to insert batch: {e}")
            raise
        finally:
            cursor.close()

    def insert_batch(self, df: pd.DataFrame):
        """Insert a batch of data into TimescaleDB with detailed logging."""
        if df.empty:
            return

        # Prepare data for insertion
        columns = [
            "time",
            "original_time",
            "well_name",
            "well_type",
            "oil_rate",
            "gas_rate",
            "water_rate",
            "water_inj_rate",
            "on_stream_hrs",
            "downhole_pressure",
            "downhole_temperature",
            "dp_tubing",
            "annulus_pressure",
            "choke_size",
            "choke_size_uom",
            "dp_choke_size",
            "thp",
            "wht",
            "flow_kind",
            "gor",
            "watercut",
            "liquid_rate",
        ]

        # Convert DataFrame to list of tuples
        values = [tuple(row) for row in df[columns].values]

        # Insert using execute_values for efficiency
        cursor = self.pg_conn.cursor()
        try:
            execute_values(
                cursor,
                f"""
                INSERT INTO production_data ({", ".join(columns)})
                VALUES %s
                ON CONFLICT DO NOTHING
                """,
                values,
                page_size=1000,
            )
            self.pg_conn.commit()

            # Refresh materialized view periodically
            try:
                cursor.execute("REFRESH MATERIALIZED VIEW latest_production")
                self.pg_conn.commit()
            except psycopg2.errors.InsufficientPrivilege as e:
                logger.warning(f"Cannot refresh materialized view (insufficient privileges): {e}")
                logger.warning("Run fix-materialized-view-permissions.sh to grant permissions")
                self.pg_conn.rollback()
            except Exception as e:
                logger.warning(f"Failed to refresh materialized view: {e}")
                self.pg_conn.rollback()

            # Enhanced logging with original vs offset timestamps (in local timezone)
            # Ensure timestamps are timezone-aware before converting
            original_min = pd.Timestamp(df["original_time"].min())
            original_max = pd.Timestamp(df["original_time"].max())
            offset_min = pd.Timestamp(df["time"].min())
            offset_max = pd.Timestamp(df["time"].max())

            # Convert to local timezone for display
            if original_min.tzinfo is None:
                original_min = original_min.tz_localize("UTC").tz_convert(self.local_tz)
                original_max = original_max.tz_localize("UTC").tz_convert(self.local_tz)
            else:
                original_min = original_min.tz_convert(self.local_tz)
                original_max = original_max.tz_convert(self.local_tz)

            if offset_min.tzinfo is None:
                offset_min = offset_min.tz_localize("UTC").tz_convert(self.local_tz)
                offset_max = offset_max.tz_localize("UTC").tz_convert(self.local_tz)
            else:
                offset_min = offset_min.tz_convert(self.local_tz)
                offset_max = offset_max.tz_convert(self.local_tz)

            logger.success(
                f"✓ Streamed {len(df)} records | "
                f"Original: {original_min.strftime('%Y-%m-%d %H:%M %Z')} → {original_max.strftime('%Y-%m-%d %H:%M %Z')} | "
                f"Offset: {offset_min.strftime('%Y-%m-%d %H:%M %Z')} → {offset_max.strftime('%Y-%m-%d %H:%M %Z')}"
            )
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to insert batch: {e}")
            raise
        finally:
            cursor.close()

    def run(self):
        """Main streaming loop with support for batch loading and real-time streaming."""
        logger.info("═══════════════════════════════════════════════════════════")
        logger.info("    FloData Production Streamer")
        logger.info("    (Clock-Synchronized Real-Time + Auto Catch-Up)")
        logger.info("═══════════════════════════════════════════════════════════")
        logger.info("Configuration:")
        logger.info(f"  • Start offset: {self.start_offset_days} days")
        logger.info(f"  • Batch size: {self.batch_size_hours} hours")
        logger.info(f"  • Catch-up threshold: {self.catchup_threshold_hours} hours")
        logger.info(f"  • Catch-up batch size: {self.catchup_batch_hours} hours")
        logger.info("")

        try:
            self.connect_databases()
            self.initialize_stream_position()

            # Phase 1: Batch load historical data (if needed)
            if not self.streaming_mode and self.start_offset_days > 0:
                completed = self.batch_load_historical_data()
                if not completed:
                    logger.warning("Batch load was interrupted")
                    return
                # Switch to streaming mode
                self.streaming_mode = True

            # Phase 2: Real-time streaming (synchronized to clock)
            logger.info("┌─ Real-time Streaming Mode ──────────────────────────────────")
            logger.info("│ • Synchronized to real-world clock")
            logger.info("│ • Streams 1 hour of data at each hour boundary")
            logger.info("│ • Auto catch-up if more than %d hours behind" % self.catchup_threshold_hours)
            logger.info("│ • Press Ctrl+C to stop")
            logger.info("└─────────────────────────────────────────────────────────────")
            logger.info("")

            while self.running:
                # Check if we're behind schedule (catch-up mode)
                hours_behind = self.calculate_time_behind()

                if hours_behind > self.catchup_threshold_hours:
                    # CATCH-UP MODE: Stream multiple hours quickly
                    logger.warning(f"⚡ CATCH-UP MODE: {hours_behind:.1f} hours behind schedule")
                    logger.info(f"   Streaming {self.catchup_batch_hours} hours per batch until caught up...")

                    # Stream catch-up batches
                    while hours_behind > self.catchup_threshold_hours and self.running:
                        # Calculate how many hours to stream (don't overshoot)
                        hours_to_stream = min(self.catchup_batch_hours, int(hours_behind))

                        # Fetch and insert batch
                        batch = self.fetch_next_batch(hours=hours_to_stream)
                        if batch is None or batch.empty:
                            logger.info("Reached end of dataset during catch-up. Restarting from beginning...")
                            self.current_timestamp = self.min_date
                            time.sleep(5)
                            break

                        # Use silent insert for catch-up (less verbose)
                        self._insert_batch_silent(batch)

                        # Log progress
                        hours_behind = self.calculate_time_behind()
                        logger.info(
                            f"   Catch-up: Loaded {len(batch)} records ({hours_to_stream}h) | "
                            f"Still {hours_behind:.1f}h behind"
                        )

                    if hours_behind <= self.catchup_threshold_hours:
                        logger.success(
                            f"✓ Caught up! Now only {hours_behind:.1f}h behind. Switching to clock-synchronized mode."
                        )
                        logger.info("")

                else:
                    # NORMAL MODE: Clock-synchronized streaming
                    now_local = datetime.now(self.local_tz)
                    next_hour = (now_local + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                    seconds_until_next_hour = (next_hour - now_local).total_seconds()

                    # Wait until next hour
                    logger.info(
                        f"⏱  Waiting until {next_hour.strftime('%H:%M')} to stream next hour ({seconds_until_next_hour:.0f}s)..."
                    )
                    time.sleep(min(seconds_until_next_hour, 60))  # Check every minute max

                    # Only stream if we've reached the next hour
                    if datetime.now(self.local_tz) < next_hour:
                        continue

                    # Fetch and insert next hour
                    batch = self.fetch_next_batch(hours=1)
                    if batch is None or batch.empty:
                        logger.info("Reached end of dataset. Restarting from beginning...")
                        self.current_timestamp = self.min_date
                        time.sleep(5)
                        continue

                    self.insert_batch(batch)

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up database connections."""
        logger.info("Cleaning up...")
        if self.pg_conn:
            self.pg_conn.close()
        if self.duck_conn:
            self.duck_conn.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    streamer = ProductionStreamer()
    streamer.run()
