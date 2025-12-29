# OpenFloData - Real-Time Streaming of Volve Production Data

One of the biggest challenges in learning, prototyping or building reservoir surveillance and production monitoring systems is the lack of access to realistic production data streams. Courses or prototypes usually utilize snapshots of spreadsheets or static databases, which do not reflect the dynamic nature of real-world production systems, making it difficult to grasp the complexities of real-time data handling or streaming architectures.

OpenFloData addresses this gap by providing a simulated real-world system for oil & gas production monitoring. It demonstrates **Real-time data streaming** in PostgreSQL (and REST API) and **Clock-synchronized streaming** - streams historical data as if it's happening "now".

To get started, check out the [Getting Started](#getting-started) section below.

To understand how the data was created, see [The Data](#the-data) section.

---

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- [uv](https://github.com/astral-sh/uv) (Python package manager)
- Python 3.12+

### Setting Up

#### Step 1: Clone the Repository

```bash
git clone https://github.com/FloLabsAI/OpenFloData.git
cd OpenFloData
```

#### Step 2: Configure Environment (Optional)

The defaults work out of the box. Only edit `.env` if you need to customize behavior (e.g., timezone, start offset). See [Configuration](#configuration) for details on available options.

```bash
# Edit the .env file for configuration (optional)
nano .env
```

#### Step 3: Start the System

```bash
# Start PostgreSQL, API, and Streamer services
docker compose up -d
# if flodata-postgres fails to load for the first time, wait for a few seconds and rerun `docker compose up -d`

# Watch the logs to see streaming in action
docker compose logs -f streamer
```

You should see output that looks like this:
```
 Batch 53: Loaded 96 records | Original: 2008-08-30 to 2008-08-31 | Progress: 100.0%

 ✓ Batch load complete! Loaded 8760 hours of historical data
   Original timeline: 2007-09-01 → 2008-08-31
   Now switching to real-time streaming mode...

 ┌─ Real-time Streaming Mode ──────────────────────────────────
 │ • Synchronized to real-world clock
 │ • Streams 1 hour of data at each hour boundary
 │ • Auto catch-up if more than 2 hours behind
 │ • Press Ctrl+C to stop
 └─────────────────────────────────────────────────────────────

 ⏱  Waiting until 01:00 to stream next hour (3091s)...
```

#### Step 4: Verify It's Working

```bash
# API is available at http://localhost:8000
curl http://localhost:8000/health

# Get production data for a well
curl "http://localhost:8000/wells/history?well_name=F-14&hours=24"
```

**API Documentation**: http://localhost:8000/docs

---

## Using OpenFloData

This section covers how to connect to the PostgreSQL database from various tools and programming languages.

### Connection Details

| Parameter | Value |
|-----------|-------|
| Host | `localhost` |
| Port | `5432` |
| Database | `volve_production` |
| Username | `flodata` |
| Password | `flodata_secret` |

### With psql (Command Line)

**psql** is PostgreSQL's interactive terminal. It's the quickest way to run queries and explore the database.

```bash
# Connect using docker exec (recommended - no local install needed)
docker compose exec postgres psql -U flodata -d volve_production

# Connect using local psql installation
psql -h localhost -p 5432 -U flodata -d volve_production
# Enter password when prompted: flodata_secret

# Connect using connection string
psql "postgresql://flodata:flodata_secret@localhost:5432/volve_production"
```

**Useful psql commands:**

```sql
\dt                    -- List all tables
\d production_data     -- Describe table schema
\x                     -- Toggle expanded display mode
\timing                -- Toggle query timing
\q                     -- Quit psql
```

**Useful SQL queries:**

```sql
-- Check how many records are in the database
SELECT COUNT(*) FROM production_data;

-- Get latest records
SELECT * FROM production_data ORDER BY time DESC LIMIT 10;

-- Production by well (last 24 hours)
SELECT
    well_name,
    COUNT(*) as records,
    AVG(annulus_pressure) as avg_pressure
FROM production_data
WHERE time > NOW() - INTERVAL '24 hours'
GROUP BY well_name;

-- Database size
SELECT pg_size_pretty(pg_database_size('volve_production'));
```

**Quick one-liner queries:**

```bash
# Get record count without entering interactive mode
psql -h localhost -p 5432 -U flodata -d volve_production -c "SELECT COUNT(*) FROM production_data;"

# Export query results to CSV
psql -h localhost -p 5432 -U flodata -d volve_production -c "\COPY (SELECT * FROM production_data LIMIT 1000) TO 'export.csv' CSV HEADER"
```

### With Python

Use pandas with SQLAlchemy to read production data directly into a DataFrame for analysis.

```bash
# Install dependencies
uv add pandas sqlalchemy psycopg2-binary
```

```python
import pandas as pd
from sqlalchemy import create_engine

# Create engine
engine = create_engine("postgresql://flodata:flodata_secret@localhost:5432/volve_production")

# Read data into a DataFrame
df = pd.read_sql("""
    SELECT time, well_name, downhole_pressure, downhole_temperature,
           choke_size, on_stream_hrs
    FROM production_data
    WHERE time > NOW() - INTERVAL '7 days'
    ORDER BY time
""", engine)

# Analyze with pandas
print(df.describe())
print(df.groupby('well_name').mean(numeric_only=True))
```

### API Reference

#### Base URL
```
http://localhost:8000
```

#### Interactive Documentation
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

#### Endpoints

**1. Health Check**
```bash
GET /health
```

Response:
```json
{
  "status": "healthy",
  "timestamp": "2025-01-01T00:00:00+00:00"
}
```

**2. List All Wells**
```bash
GET /wells
```

Parameters:
- `well_type` (query, optional): Filter by well type (`OP` for producers, `WI` for injectors, `all` for both)

Example:
```bash
curl "http://localhost:8000/wells"
```

Response:
```json
{
  "count": 4,
  "wells": [
    {"well_name": "F-12", "well_type": "OP"},
    {"well_name": "F-14", "well_type": "OP"},
    {"well_name": "F-4", "well_type": "WI"},
    {"well_name": "F-5", "well_type": "WI"}
  ]
}
```

**3. Get Latest Well Data**
```bash
GET /wells/latest?well_name={name}&period_hours={hours}
```

Parameters:
- `well_name` (query, required): Well identifier (e.g., "F-14")
- `period_hours` (query, optional): Number of hours to retrieve (default: 12, max: 100)

Example:
```bash
curl "http://localhost:8000/wells/latest?well_name=F-14&period_hours=24"
```

**4. Get Well History**
```bash
GET /wells/history?well_name={name}&hours={hours}&interval={interval}
```

Parameters:
- `well_name` (query, required): Well identifier (e.g., "F-14")
- `hours` (query, optional): Number of hours to retrieve (default: 24, max: 720)
- `interval` (query, optional): Time bucket interval (default: "1h", e.g., "1h", "6h", "1d")

Example:
```bash
curl "http://localhost:8000/wells/history?well_name=F-14&hours=24"
```

**5. Get Current Production**
```bash
GET /production/current
```

Parameters:
- `well_type` (query, optional): Filter by well type (default: "OP")

Returns current production snapshot for all wells.

**6. Get Production Timeseries**
```bash
GET /production/timeseries?hours={hours}&interval={interval}
```

Parameters:
- `hours` (query, optional): Number of hours to retrieve (default: 24, max: 720)
- `interval` (query, optional): Time bucket interval (default: "1h")
- `well_type` (query, optional): Filter by well type (default: "OP")
- `aggregate` (query, optional): Aggregate all wells into single timeseries (default: true)

**7. Get Database Stats**
```bash
GET /stats
```

Returns database statistics and system info.

---

## Concepts

### The Data

#### Source: Volve Field Dataset

This project uses data derived from the **Volve field dataset** released by Equinor. The Volve field was an oil field in the North Sea operated from 2007-2016, and Equinor released the complete production dataset (for licensing details, see [LICENSE-VOLVE-DATA.md](LICENSE-VOLVE-DATA.md)).

#### Data Transformation: Daily to Hourly

**Original Volve Data:**
- **Resolution**: Daily production data per well
- **Period**: 2007-2016 (approximately 9 years)
- **Metrics**: Oil rate, gas rate, water rate, downhole pressure, temperature, choke settings, and more

**OpenFloData Hourly Data:**

The original daily data has been **synthetically upsampled to hourly intervals** to create a realistic simulation suitable for real-time streaming demonstrations. The upsampling process:

1. **Preserves Statistical Distribution**
   - Daily mean values are maintained
   - Realistic hourly variations are introduced
   - Production trends and patterns are preserved

2. **Respects Physical Constraints**
   - Oil production systems have physical limits and inertia
   - Sudden changes in production are smoothed realistically
   - Relationships between parameters (pressure, temperature, flow rates) are maintained

3. **Adds Realistic Variability**
   - Small hourly fluctuations reflect real sensor noise and process variations
   - Variations stay within physically plausible bounds
   - Daily aggregations match the original daily values

Caveat: While the statistical and physical realism was preserved as much as possible, there may be minor discrepancies due to the synthetic nature of the upsampling process.

**Example Transformation:**

```
Original Daily Data (from Volve):
Date: 2008-01-29
Oil Rate: 2400 bbl/day
Downhole Pressure: 235 bar

Upsampled Hourly Data (in OpenFloData):
2008-01-29 00:00:00  |  Oil: 2380 bbl/day  |  Pressure: 234.5 bar
2008-01-29 01:00:00  |  Oil: 2410 bbl/day  |  Pressure: 235.2 bar
2008-01-29 02:00:00  |  Oil: 2395 bbl/day  |  Pressure: 235.0 bar
...
2008-01-29 23:00:00  |  Oil: 2405 bbl/day  |  Pressure: 234.8 bar

Daily Average: 2400 bbl/day  |  235.0 bar  ← Matches original
```

**Data Attribution:**

The synthetic hourly data is a **derivative work** of the Volve dataset and is provided under the same CC BY-NC-SA 4.0 license. For full details, see [LICENSE-VOLVE-DATA.md](LICENSE-VOLVE-DATA.md).

### How OpenFloData Works

OpenFloData creates a realistic simulation of a production monitoring system:

1. **Source Data**: Hourly production data stored in DuckDB (`data/prod_hourly_data.duckdb`)
2. **Streaming Service**: Reads historical data and streams it to PostgreSQL as if it's happening "now"
3. **Time Synchronization**: Maps a historical date (e.g., 2008-01-01) to "today 00:00"
4. **API Service**: FastAPI REST API for querying the streaming data

```
┌─────────────────┐
│   DuckDB        │  Historical hourly data (2007-2016)
│  (Source DB)    │  Derived from Volve daily data
└────────┬────────┘
         │
         │ Stream (clock-synchronized)
         ▼
┌─────────────────┐
│  PostgreSQL     │  "Live" production data
│ (Target DB)     │
└────────┬────────┘
         │
         │ Query
         ▼
┌─────────────────┐
│   FastAPI       │  REST API
│  (API Service)  │
└─────────────────┘
```

### Architecture

#### Services

**1. PostgreSQL** (`flodata-postgres`)
- Target database for streaming data
- Stores "live" production metrics
- Exposed on port 5432

**2. API Service** (`flodata-api`)
- FastAPI REST API
- Endpoints for production data queries
- Interactive docs at `/docs`
- Exposed on port 8000

**3. Streamer Service** (`flodata-streamer`)
- Reads from DuckDB, writes to PostgreSQL
- Clock-synchronized streaming
- Auto catch-up when behind
- Always running (no exposed ports)

#### Data Flow

```
Source (DuckDB) → Streamer → PostgreSQL → API → Users
```

---

## Configuration

### Environment Variables

Edit `.env` to customize behavior:

```bash
# PostgreSQL Configuration
DB_HOST=postgres              # Database host
DB_PORT=5432                  # Database port
DB_NAME=volve_production      # Database name
DB_USER=flodata               # Database user
DB_PASSWORD=flodata_secret    # Database password

# Streaming Configuration
START_OFFSET_DAYS=365         # Day in source data that maps to "now"
                              # Source starts at 2007-09-01 (day 0)
                              # 365 = 2008-08-31 maps to today

BATCH_SIZE_HOURS=168          # Hours per batch during initial load
                              # 168 = 7 days (good balance)

CATCHUP_THRESHOLD_HOURS=2     # Auto catch-up if > 2 hours behind
CATCHUP_BATCH_HOURS=24        # Hours per batch during catch-up

TZ=UTC                        # Timezone for timestamp display
```

### Time Synchronization

OpenFloData maps historical data to "now":

- **Source data**: Starts at 2007-09-01 (Day 0)
- **START_OFFSET_DAYS**: Maps "Day N" to "Today 00:00"
- **Example**: `START_OFFSET_DAYS=365` means Day 365 (2008-08-31) becomes "Today"

```
Historical Timeline:          Simulation Timeline:
Day 0:   2007-09-01    →     Past
Day 365: 2008-08-31    →     Today 00:00  ← You are here
Day 366: 2008-09-01    →     Streaming...
```

---

## Development

### Local Development Setup

```bash
# Install dependencies using uv
uv sync --all-extras

# Activate virtual environment
source .venv/bin/activate  # On macOS/Linux
# OR
.venv\Scripts\activate  # On Windows

# Run services locally (without Docker)
python stream_service.py   # Terminal 1
python api_service.py      # Terminal 2
```

### Running Tests

```bash
# Install dev dependencies
uv sync --all-extras

# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=. --cov-report=html

# Run specific test suites
uv run pytest tests/unit/          # Unit tests only
uv run pytest tests/integration/   # Integration tests only
```

### Code Quality

```bash
# Install pre-commit hooks
uv run pre-commit install

# Run code quality checks manually
uv run ruff check .           # Linting
uv run ruff format .          # Formatting
uv run mypy api_service.py    # Type checking

# Run all pre-commit hooks
uv run pre-commit run --all-files
```

---

## Troubleshooting

### Container Issues

```bash
# Check container status
docker compose ps

# View logs
docker compose logs api       # API logs
docker compose logs streamer  # Streamer logs
docker compose logs postgres  # Database logs

# Restart services
docker compose restart api
docker compose restart streamer

# Full restart
docker compose down
docker compose up -d
```

### API Not Responding

```bash
# Check if API container is running
docker compose ps api

# Check API logs for errors
docker compose logs api --tail=50

# Test database connection
docker compose exec postgres psql -U flodata -d volve_production -c "SELECT COUNT(*) FROM production_data;"
```

### Streamer Not Streaming

```bash
# Check streamer logs
docker compose logs streamer -f

# Common issues:
# 1. DuckDB file missing → Check data/prod_hourly_data.duckdb exists
# 2. PostgreSQL not ready → Wait 10-15 seconds for DB to initialize
# 3. Already caught up → This is normal! Streamer waits for next hour
```

### Database Connection Issues

```bash
# Check if PostgreSQL is running
docker compose ps postgres

# Test connection
docker compose exec postgres pg_isready -U flodata

# Check PostgreSQL logs
docker compose logs postgres --tail=50

# Restart database (will lose data)
docker compose restart postgres
```

---

## License

### Software

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

### Data

This project uses data derived from the **Volve field dataset** released by Equinor under the **Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License (CC BY-NC-SA 4.0)**.

The original Volve dataset provides daily production data, which has been synthetically upsampled to hourly intervals for this project while preserving realistic production system characteristics.

For full details, see [LICENSE-VOLVE-DATA.md](LICENSE-VOLVE-DATA.md).

**Attribution**: This project uses data derived from the Volve field dataset released by Equinor.
**Source**: https://www.equinor.com/energy/volve-data-sharing

---

## Contributing & Support

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Acknowledgments

- **Equinor** for releasing the Volve dataset under an open license
- **The open source community** for the amazing tools and frameworks

### Questions or Issues?

- **Issues**: https://github.com/FloLabsAI/OpenFloData/issues
- **Discussions**: https://github.com/FloLabsAI/OpenFloData/discussions

---

**Happy Streaming!**
