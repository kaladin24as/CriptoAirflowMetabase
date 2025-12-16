# Cryptocurrency Data Pipeline

Automated pipeline that extracts, transforms, and visualizes cryptocurrency information from the CoinGecko API.

## Features

- Automatic update every 15 minutes
- 250+ cryptocurrencies with complete market data
- Real-time trending coins
- Pre-built analytical views
- Fully dockerized
- Orchestration with Airflow

## Architecture

```
CoinGecko API → dlt → PostgreSQL → SQL → Metabase
                        ↑
                    Airflow
```

**Stack:** dlt | PostgreSQL 15 | Apache Airflow 2.10 | Metabase | Docker

## Quick Start

### Prerequisites

- Docker Desktop installed and running

### Installation

```powershell
# Clone repository
git clone https://github.com/your-username/cryptocurrency-pipeline.git
cd criptomonedas-pipeline

# Automatic setup
.\setup.ps1
```

### Service Access

| Service | URL | Credentials |
|----------|-----|--------------|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| Metabase | http://localhost:3000 | Configure on first access |
| PostgreSQL | `localhost:5432` | `postgres` / `QJ9dPbEPh6ojikGMX8kDbA` |

## Structure

```
.
├── dags/
│   └── pipeline_dag.py              # Airflow DAG
├── etl/
│   ├── extract/
│   │   └── coingecko_pipeline.py    # dlt Pipeline
│   └── transform/
│       └── crypto_transformations.sql
├── docker-compose.yml
├── setup.ps1
└── run-pipeline.ps1
```

## Data

### Tables (`crypto_raw` schema)

- `market_data` - Market data (250+ cryptos)
- `trending_coins` - Trending cryptocurrencies
- `global_stats` - Global statistics

### Analytical Views

- `crypto_market_summary` - Summary with calculated metrics
- `crypto_price_changes` - Price changes (1h, 24h, 7d, 30d)
- `crypto_top_performers` - Top 10 winners and losers
- `crypto_market_overview` - Aggregated market view
- `crypto_trending_summary` - Trending coins with market data

## Usage

### Run Pipeline

```powershell
.\run-pipeline.ps1
```

### SQL Queries

```sql
-- Top 10 by market cap
SELECT market_cap_rank, name, symbol, current_price, market_cap 
FROM crypto_raw.market_data 
ORDER BY market_cap_rank LIMIT 10;

-- General view
SELECT * FROM crypto_market_overview;
```

### Connect to PostgreSQL

```bash
docker exec -it criptomonedas-postgres-1 psql -U postgres -d warehouse
```

### Docker Commands

```bash
# Stop
docker compose down

# Restart
docker compose restart

# View logs
docker logs criptomonedas-airflow-1 -f
```

## Metabase Configuration

1. Access http://localhost:3000
2. Create administrator account
3. Configure PostgreSQL connection:
   - Host: `postgres`
   - Port: `5432`
   - Database: `warehouse`
   - User: `postgres`
   - Password: `QJ9dPbEPh6ojikGMX8kDbA`

## Advanced Configuration

### Environment Variables

Edit `.env.dev`, `.env.staging` or `.env.prod`:

```bash
DATA_DB_HOST=postgres
DATA_DB_PORT=5432
DATA_DB_NAME=warehouse
DATA_DB_USER=postgres
DATA_DB_PASSWORD=your_password

AIRFLOW__CORE__EXECUTOR=SequentialExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=false
```

### Change Frequency

Edit `dags/pipeline_dag.py`:

```python
schedule_interval='*/15 * * * *'  # Every 15 minutes
# schedule_interval='0 * * * *'   # Hourly
# schedule_interval='0 0 * * *'   # Daily
```

## Kubernetes Deployment

```bash
helm install criptomonedas ./kubernetes
```

## License

MIT
