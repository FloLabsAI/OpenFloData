-- PostgreSQL with custom time_bucket() function
-- This mimics TimescaleDB's time_bucket() to maintain API compatibility
-- Use this with Cloud SQL PostgreSQL for zero code changes!

-- ============================================================================
-- Custom time_bucket() Function (TimescaleDB-compatible)
-- ============================================================================

-- This function mimics TimescaleDB's time_bucket() for common intervals
-- Supports: 1 minute, 5 minutes, 15 minutes, 1 hour, 1 day, 1 week
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ)
RETURNS TIMESTAMPTZ AS $$
DECLARE
    seconds BIGINT;
    bucket_seconds BIGINT;
    epoch BIGINT;
BEGIN
    -- Extract total seconds from the interval
    seconds := EXTRACT(EPOCH FROM bucket_width)::BIGINT;

    -- Get epoch seconds from timestamp
    epoch := EXTRACT(EPOCH FROM ts)::BIGINT;

    -- Calculate bucket start by rounding down to nearest bucket
    bucket_seconds := (epoch / seconds) * seconds;

    -- Convert back to timestamptz
    RETURN TO_TIMESTAMP(bucket_seconds) AT TIME ZONE 'UTC';
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

-- Create overload for TEXT interval (for backward compatibility)
CREATE OR REPLACE FUNCTION time_bucket(bucket_width TEXT, ts TIMESTAMPTZ)
RETURNS TIMESTAMPTZ AS $$
BEGIN
    RETURN time_bucket(bucket_width::INTERVAL, ts);
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION time_bucket(INTERVAL, TIMESTAMPTZ) IS
'Custom time_bucket function that mimics TimescaleDB behavior. Buckets timestamps into regular intervals.';

-- ============================================================================
-- Production Data Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS production_data (
    time TIMESTAMPTZ NOT NULL,
    original_time TIMESTAMPTZ NOT NULL,
    well_name VARCHAR(50) NOT NULL,
    well_type VARCHAR(10),
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
    choke_size_uom VARCHAR(10),
    dp_choke_size DOUBLE PRECISION,
    thp DOUBLE PRECISION,
    wht DOUBLE PRECISION,
    flow_kind VARCHAR(20),
    gor DOUBLE PRECISION,
    watercut DOUBLE PRECISION,
    liquid_rate DOUBLE PRECISION,
    PRIMARY KEY (well_name, time)
);

-- ============================================================================
-- Indexes for Time-Series Queries
-- ============================================================================

-- Primary time-based index (most important for time-series queries)
CREATE INDEX IF NOT EXISTS idx_time ON production_data (time DESC);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_well_name_time ON production_data (well_name, time DESC);
CREATE INDEX IF NOT EXISTS idx_well_type_time ON production_data (well_type, time DESC);
CREATE INDEX IF NOT EXISTS idx_original_time ON production_data (original_time DESC);

-- Covering index for aggregation queries (includes frequently accessed columns)
CREATE INDEX IF NOT EXISTS idx_time_well_rates ON production_data (time DESC, well_name)
    INCLUDE (oil_rate, gas_rate, water_rate, gor, watercut);

-- ============================================================================
-- Materialized View: Latest Production (for dashboards)
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS latest_production AS
SELECT DISTINCT ON (well_name)
    well_name,
    time,
    original_time,
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

-- Index on materialized view for fast lookups
CREATE UNIQUE INDEX IF NOT EXISTS idx_latest_well ON latest_production (well_name);
CREATE INDEX IF NOT EXISTS idx_latest_well_type ON latest_production (well_type);

-- ============================================================================
-- Materialized View: Hourly Production Statistics
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_production_stats AS
SELECT
    time_bucket('1 hour'::INTERVAL, time) AS hour,
    well_name,
    well_type,
    AVG(oil_rate) as avg_oil_rate,
    MAX(oil_rate) as max_oil_rate,
    MIN(oil_rate) as min_oil_rate,
    AVG(gas_rate) as avg_gas_rate,
    AVG(water_rate) as avg_water_rate,
    AVG(downhole_pressure) as avg_downhole_pressure,
    AVG(downhole_temperature) as avg_downhole_temperature,
    AVG(gor) as avg_gor,
    AVG(watercut) as avg_watercut,
    COUNT(*) as data_points
FROM production_data
GROUP BY time_bucket('1 hour'::INTERVAL, time), well_name, well_type;

-- Indexes on hourly stats for fast queries
CREATE INDEX IF NOT EXISTS idx_hourly_stats_hour ON hourly_production_stats (hour DESC);
CREATE INDEX IF NOT EXISTS idx_hourly_stats_well ON hourly_production_stats (well_name, hour DESC);
CREATE INDEX IF NOT EXISTS idx_hourly_stats_type ON hourly_production_stats (well_type, hour DESC);

-- ============================================================================
-- Helper Functions
-- ============================================================================

-- Function to refresh all materialized views (call this periodically)
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS void AS $$
BEGIN
    -- Refresh latest production view (used by dashboards)
    REFRESH MATERIALIZED VIEW CONCURRENTLY latest_production;

    -- Refresh hourly stats view (used by time-series queries)
    REFRESH MATERIALIZED VIEW CONCURRENTLY hourly_production_stats;

    RAISE NOTICE 'Materialized views refreshed successfully';
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_materialized_views() IS
'Refreshes all materialized views. Call this periodically (e.g., every 5 minutes) to keep dashboard data current.';

-- Function to get database statistics
CREATE OR REPLACE FUNCTION get_database_stats()
RETURNS TABLE (
    metric VARCHAR,
    value BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 'total_records'::VARCHAR, COUNT(*)::BIGINT FROM production_data
    UNION ALL
    SELECT 'total_wells'::VARCHAR, COUNT(DISTINCT well_name)::BIGINT FROM production_data
    UNION ALL
    SELECT 'producer_wells'::VARCHAR, COUNT(DISTINCT well_name)::BIGINT FROM production_data WHERE well_type = 'OP'
    UNION ALL
    SELECT 'injector_wells'::VARCHAR, COUNT(DISTINCT well_name)::BIGINT FROM production_data WHERE well_type = 'WI';
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Permissions
-- ============================================================================

-- Grant privileges on tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO flodata;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO flodata;

-- Grant privileges on materialized views (required for REFRESH)
ALTER MATERIALIZED VIEW latest_production OWNER TO flodata;
ALTER MATERIALIZED VIEW hourly_production_stats OWNER TO flodata;

-- Grant execute on functions
GRANT EXECUTE ON FUNCTION time_bucket(INTERVAL, TIMESTAMPTZ) TO flodata;
GRANT EXECUTE ON FUNCTION time_bucket(TEXT, TIMESTAMPTZ) TO flodata;
GRANT EXECUTE ON FUNCTION refresh_materialized_views() TO flodata;
GRANT EXECUTE ON FUNCTION get_database_stats() TO flodata;

-- ============================================================================
-- Helpful Notes
-- ============================================================================

/*
USAGE NOTES:

1. The custom time_bucket() function works exactly like TimescaleDB's version
   for common intervals. Your API code requires NO changes!

2. Materialized views need periodic refresh to stay current:
   - Option A: Schedule with Cloud Scheduler to call refresh_materialized_views()
   - Option B: Call from your application after data inserts
   - Recommendation: Refresh every 5-15 minutes for real-time dashboards

3. Performance considerations:
   - Good for datasets up to 10-50 GB
   - Time-based queries are fast thanks to indexes
   - Consider partitioning if data grows beyond 50 GB

4. What you're missing compared to TimescaleDB:
   - Automatic compression (manual workaround: partition and compress old data)
   - Continuous aggregates (we use materialized views instead)
   - Automatic retention policies (manual: DROP old partitions)
   - Some advanced time-series functions

5. To schedule materialized view refresh with Cloud Scheduler:
   ```bash
   # Create Cloud Scheduler job to refresh views every 5 minutes
   gcloud scheduler jobs create http refresh-views \
     --schedule="*/5 * * * *" \
     --uri="https://your-api-url.run.app/admin/refresh-views" \
     --http-method=POST
   ```

6. Query examples that work with time_bucket():
   ```sql
   -- Hourly averages
   SELECT time_bucket('1 hour', time) as hour, AVG(oil_rate)
   FROM production_data
   WHERE time >= NOW() - INTERVAL '24 hours'
   GROUP BY hour;

   -- Daily totals
   SELECT time_bucket('1 day', time) as day, SUM(oil_rate)
   FROM production_data
   WHERE time >= NOW() - INTERVAL '30 days'
   GROUP BY day;
   ```

7. Upgrading to TimescaleDB later:
   If you outgrow PostgreSQL, you can migrate to Timescale Cloud:
   - Dump your data: pg_dump
   - Create TimescaleDB hypertable
   - Restore data: pg_restore
   - Drop custom time_bucket() function (use native one)
   - Zero application code changes!

HAPPY TIME-SERIES QUERYING! 🚀
*/
