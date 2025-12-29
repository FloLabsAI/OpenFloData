# OpenFloData - Real-Time Production Data Streaming System

A production-ready streaming system that demonstrates real-time offshore production data streaming. This open-source version focuses on local deployment and simulation, using synthetically upsampled data derived from the Equinor Volve dataset.

## 🎯 What is OpenFloData?

OpenFloData simulates a real-world SCADA (Supervisory Control and Data Acquisition) system for oil & gas production monitoring. It demonstrates:

- **Real-time data streaming** from DuckDB to PostgreSQL
- **Clock-synchronized delivery** - streams historical data as if it's happening "now"
- **Auto catch-up mode** - automatically catches up when behind schedule
- **REST API** for querying production data
- **Production-ready architecture** with FastAPI, PostgreSQL, and Docker

Perfect for learning, prototyping, and demonstrating streaming data systems without needing actual production infrastructure.

---

## 📊 About the Data

### Source: Volve Field Dataset

This project uses data derived from the **Volve field dataset** released by Equinor. The Volve field was an oil field in the North Sea operated from 2007-2016, and Equinor released the complete production dataset under an open license to support research and education.

### Data Transformation: Daily to Hourly

**Original Volve Data:**
- **Resolution**: Daily production data
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

**Why Hourly Resolution?**

- **Realistic SCADA simulation**: Real production monitoring systems typically record data every 1-15 minutes. Hourly data provides a good balance between realism and data volume
- **Streaming demonstration**: Hourly intervals allow for meaningful real-time streaming demonstrations without overwhelming data volumes
- **Educational value**: Students and developers can see data arrive in "real-time" without waiting days between updates

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

---

## 🚀 Quick Start (5 minutes)

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- [uv](https://github.com/astral-sh/uv) (Python package manager)
- Python 3.12+

### Step 1: Clone the Repository

```bash
git clone https://github.com/FloLabsAI/OpenFloData.git
cd OpenFloData
```

### Step 2: Configure Environment

```bash
# Copy the example environment file
cp .env.example .env

# The defaults work out of the box - no changes needed for local development!
```

### Step 3: Start the System

```bash
# Start PostgreSQL, API, and Streamer services
docker compose up -d

# Watch the logs to see streaming in action
docker compose logs -f flodata-streamer
```

You should see output like:
```
✓ Clock synchronized: 2008-02-29 00:00:00 (Day 181 in source)
✓ Batch loaded: 24 hours (Day 181)
✓ Streaming record: 2008-02-29 00:00:00 | NO-15/9-F-11A | Oil: 1234 bbl/day
```

### Step 4: Access the API

```bash
# API is available at http://localhost:8000
curl http://localhost:8000/health

# Get production data for a well
curl "http://localhost:8000/api/production/NO-15/9-F-11A/latest?hours=24"
```

**API Documentation**: http://localhost:8000/docs

---

## 📖 What's Happening?

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

---

## 🔧 Architecture

### Services

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

### Data Flow

```
Source (DuckDB) → Streamer → PostgreSQL → API → Users
```

---

## 🛠️ Development

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

## 📡 API Reference

### Base URL
```
http://localhost:8000
```

### Interactive Documentation
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Endpoints

#### 1. Health Check
```bash
GET /health
```

**Response:**
```json
{
  "status": "healthy",
  "description": "Real-time production data from Volve field simulation",
  "database": "connected"
}
```

#### 2. Get Latest Production Data
```bash
GET /api/production/{well_name}/latest?hours=24
```

**Parameters:**
- `well_name` (path): Well identifier (e.g., "NO-15/9-F-11A")
- `hours` (query): Number of hours to retrieve (default: 24)

**Example:**
```bash
curl "http://localhost:8000/api/production/NO-15/9-F-11A/latest?hours=24"
```

**Response:**
```json
[
  {
    "time": "2008-02-29T23:00:00",
    "well_name": "NO-15/9-F-11A",
    "avg_downhole_pressure": 234.5,
    "avg_downhole_temperature": 89.2,
    "avg_choke_size_p": 45.0,
    "...": "..."
  }
]
```

#### 3. Get Production Data for Time Range
```bash
GET /api/production/{well_name}/range?start_time={ISO8601}&end_time={ISO8601}
```

**Parameters:**
- `well_name` (path): Well identifier
- `start_time` (query): Start timestamp (ISO 8601 format)
- `end_time` (query): End timestamp (ISO 8601 format)

**Example:**
```bash
curl "http://localhost:8000/api/production/NO-15/9-F-11A/range?start_time=2008-02-29T00:00:00&end_time=2008-02-29T23:59:59"
```

#### 4. List All Wells
```bash
GET /api/wells
```

**Response:**
```json
{
  "wells": [
    "NO-15/9-F-11A",
    "NO-15/9-F-12",
    "..."
  ]
}
```

---

## ⚙️ Configuration

### Environment Variables

Edit `.env` to customize behavior:

```bash
# PostgreSQL Configuration
DB_HOST=postgres              # Database host
DB_PORT=5432                  # Database port
DB_NAME=volve_production      # Database name
DB_USER=flodata               # Database user
DB_PASSWORD=flodata123        # Database password

# Streaming Configuration
START_OFFSET_DAYS=150         # Day in source data that maps to "now"
                              # Source starts at 2007-09-01 (day 0)
                              # 150 = 2008-01-29 maps to today

BATCH_SIZE_HOURS=168          # Hours per batch during initial load
                              # 168 = 7 days (good balance)

CATCHUP_THRESHOLD_HOURS=2     # Auto catch-up if > 2 hours behind
CATCHUP_BATCH_HOURS=24        # Hours per batch during catch-up

TARGET_TIMEZONE=UTC           # Timezone for timestamp display
```

### Time Synchronization

OpenFloData maps historical data to "now":

- **Source data**: Starts at 2007-09-01 (Day 0)
- **START_OFFSET_DAYS**: Maps "Day N" to "Today 00:00"
- **Example**: `START_OFFSET_DAYS=150` means Day 150 (2008-01-29) becomes "Today"

```
Historical Timeline:          Simulation Timeline:
Day 0:   2007-09-01    →     Past
Day 150: 2008-01-29    →     Today 00:00  ← You are here
Day 151: 2008-01-30    →     Streaming...
```

---

## 🗄️ Database

### Connecting to PostgreSQL

```bash
# Using docker exec
docker compose exec postgres psql -U flodata -d volve_production

# Using local psql
psql -h localhost -p 5432 -U flodata -d volve_production
# Password: flodata123
```

### Useful SQL Queries

```sql
-- Check how many records are in the database
SELECT COUNT(*) FROM production_data;

-- Get latest records
SELECT * FROM production_data ORDER BY time DESC LIMIT 10;

-- Production by well (last 24 hours)
SELECT
    well_name,
    COUNT(*) as records,
    AVG(avg_annulus_press) as avg_pressure
FROM production_data
WHERE time > NOW() - INTERVAL '24 hours'
GROUP BY well_name;

-- Database size
SELECT pg_size_pretty(pg_database_size('volve_production'));
```

---

## 🔍 Troubleshooting

### Container Issues

```bash
# Check container status
docker compose ps

# View logs
docker compose logs flodata-api       # API logs
docker compose logs flodata-streamer  # Streamer logs
docker compose logs postgres          # Database logs

# Restart services
docker compose restart flodata-api
docker compose restart flodata-streamer

# Full restart
docker compose down
docker compose up -d
```

### API Not Responding

```bash
# Check if API container is running
docker compose ps flodata-api

# Check API logs for errors
docker compose logs flodata-api --tail=50

# Test database connection
docker compose exec postgres psql -U flodata -d volve_production -c "SELECT COUNT(*) FROM production_data;"
```

### Streamer Not Streaming

```bash
# Check streamer logs
docker compose logs flodata-streamer -f

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

## 📄 License

### Software

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

### Data

This project uses data derived from the **Volve field dataset** released by Equinor under the **Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License (CC BY-NC-SA 4.0)**.

The original Volve dataset provides daily production data, which has been synthetically upsampled to hourly intervals for this project while preserving realistic production system characteristics.

For full details, see [LICENSE-VOLVE-DATA.md](LICENSE-VOLVE-DATA.md).

**Attribution**: This project uses data derived from the Volve field dataset released by Equinor.
**Source**: https://www.equinor.com/energy/volve-data-sharing

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 🙏 Acknowledgments

- **Equinor** for releasing the Volve dataset under an open license
- **The open source community** for the amazing tools and frameworks

---

## 📞 Questions or Issues?

- **Issues**: https://github.com/FloLabsAI/OpenFloData/issues
- **Discussions**: https://github.com/FloLabsAI/OpenFloData/discussions

---

**Happy Streaming!** 🚀
