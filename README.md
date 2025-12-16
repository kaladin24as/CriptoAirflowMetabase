# Pipeline de Datos de Criptomonedas

Pipeline automatizado que extrae, transforma y visualiza información de criptomonedas desde CoinGecko API.

## Características

- Actualización automática cada 15 minutos
- 250+ criptomonedas con datos de mercado completos
- Trending coins en tiempo real
- Vistas analíticas pre-construidas
- Totalmente dockerizado
- Orquestación con Airflow

## Arquitectura

```
CoinGecko API → dlt → PostgreSQL → SQL → Metabase
                        ↑
                    Airflow
```

**Stack:** dlt | PostgreSQL 15 | Apache Airflow 2.10 | Metabase | Docker

## Inicio Rápido

### Prerrequisitos

- Docker Desktop instalado y corriendo

### Instalación

```powershell
# Clonar repositorio
git clone https://github.com/tu-usuario/criptomonedas-pipeline.git
cd criptomonedas-pipeline

# Setup automático
.\setup.ps1
```

### Acceso a Servicios

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| Metabase | http://localhost:3000 | Configurar en primer acceso |
| PostgreSQL | `localhost:5432` | `postgres` / `QJ9dPbEPh6ojikGMX8kDbA` |

## Estructura

```
.
├── dags/
│   └── pipeline_dag.py              # DAG de Airflow
├── etl/
│   ├── extract/
│   │   └── coingecko_pipeline.py    # Pipeline dlt
│   └── transform/
│       └── crypto_transformations.sql
├── docker-compose.yml
├── setup.ps1
└── run-pipeline.ps1
```

## Datos

### Tablas (`crypto_raw` schema)

- `market_data` - Datos de mercado (250+ cryptos)
- `trending_coins` - Criptomonedas en tendencia
- `global_stats` - Estadísticas globales

### Vistas Analíticas

- `crypto_market_summary` - Resumen con métricas calculadas
- `crypto_price_changes` - Cambios de precio (1h, 24h, 7d, 30d)
- `crypto_top_performers` - Top 10 ganadores y perdedores
- `crypto_market_overview` - Vista agregada del mercado
- `crypto_trending_summary` - Trending coins con datos de mercado

## Uso

### Ejecutar Pipeline

```powershell
.\run-pipeline.ps1
```

### Consultas SQL

```sql
-- Top 10 por market cap
SELECT market_cap_rank, name, symbol, current_price, market_cap 
FROM crypto_raw.market_data 
ORDER BY market_cap_rank LIMIT 10;

-- Vista general
SELECT * FROM crypto_market_overview;
```

### Conectar a PostgreSQL

```bash
docker exec -it criptomonedas-postgres-1 psql -U postgres -d warehouse
```

### Comandos Docker

```bash
# Detener
docker compose down

# Reiniciar
docker compose restart

# Ver logs
docker logs criptomonedas-airflow-1 -f
```

## Configuración Metabase

1. Acceder a http://localhost:3000
2. Crear cuenta de administrador
3. Configurar conexión PostgreSQL:
   - Host: `postgres`
   - Puerto: `5432`
   - Database: `warehouse`
   - Usuario: `postgres`
   - Password: `QJ9dPbEPh6ojikGMX8kDbA`

## Configuración Avanzada

### Variables de Entorno

Editar `.env.dev`, `.env.staging` o `.env.prod`:

```bash
DATA_DB_HOST=postgres
DATA_DB_PORT=5432
DATA_DB_NAME=warehouse
DATA_DB_USER=postgres
DATA_DB_PASSWORD=tu_password

AIRFLOW__CORE__EXECUTOR=SequentialExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=false
```

### Cambiar Frecuencia

Editar `dags/pipeline_dag.py`:

```python
schedule_interval='*/15 * * * *'  # Cada 15 minutos
# schedule_interval='0 * * * *'   # Cada hora
# schedule_interval='0 0 * * *'   # Diario
```

## Despliegue Kubernetes

```bash
helm install criptomonedas ./kubernetes
```

## Licencia

MIT
