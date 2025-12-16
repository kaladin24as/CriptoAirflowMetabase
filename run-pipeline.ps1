# ============================================================================
# Ejecutar Pipeline de CoinGecko Manualmente
# ============================================================================
# Este script ejecuta el pipeline de extracción de datos de CoinGecko
# ============================================================================

Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "  Ejecutando Pipeline de CoinGecko" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

# Verificar que los servicios están corriendo
Write-Host "[1/3] Verificando servicios..." -ForegroundColor Yellow
$postgresRunning = docker ps --filter "name=postgres" --filter "status=running" -q
if (-not $postgresRunning) {
    Write-Host "  ✗ PostgreSQL no está corriendo" -ForegroundColor Red
    Write-Host "  Ejecuta primero: docker compose up -d" -ForegroundColor Yellow
    exit 1
}
Write-Host "  ✓ PostgreSQL está corriendo" -ForegroundColor Green
Write-Host ""

# Ejecutar el pipeline
Write-Host "[2/3] Ejecutando pipeline de extracción..." -ForegroundColor Yellow
Write-Host "  Esto tomará 1-2 minutos..." -ForegroundColor Gray
Write-Host ""

docker run --rm `
    --network criptomonedas_antigravity_net `
    -e DATA_DB_HOST=postgres `
    -e DATA_DB_PORT=5432 `
    -e DATA_DB_NAME=warehouse `
    -e DATA_DB_USER=postgres `
    -e DATA_DB_PASSWORD=QJ9dPbEPh6ojikGMX8kDbA `
    criptomonedas-dlt_ingestion:latest

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "  ✓ Pipeline ejecutado exitosamente" -ForegroundColor Green
}
else {
    Write-Host ""
    Write-Host "  ✗ Error al ejecutar el pipeline" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Ejecutar transformaciones SQL
Write-Host "[3/3] Ejecutando transformaciones SQL..." -ForegroundColor Yellow

# Obtener el nombre del contenedor de PostgreSQL
$postgresContainer = docker ps --filter "name=postgres" --format "{{.Names}}" | Select-Object -First 1

# Copiar el archivo SQL al contenedor
docker cp etl/transform/crypto_transformations.sql ${postgresContainer}:/tmp/crypto_transformations.sql

# Ejecutar el SQL
docker exec $postgresContainer psql -U postgres -d warehouse -f /tmp/crypto_transformations.sql

if ($LASTEXITCODE -eq 0) {
    Write-Host "  ✓ Transformaciones ejecutadas exitosamente" -ForegroundColor Green
}
else {
    Write-Host "  ✗ Error al ejecutar transformaciones" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "  ✓ Pipeline completado!" -ForegroundColor Green
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Verifica los datos:" -ForegroundColor White
Write-Host "  docker exec -it $postgresContainer psql -U postgres -d warehouse" -ForegroundColor Cyan
Write-Host ""
Write-Host "Consultas de ejemplo:" -ForegroundColor White
Write-Host "  SELECT COUNT(*) FROM crypto_raw.market_data;" -ForegroundColor Gray
Write-Host "  SELECT * FROM crypto_market_overview;" -ForegroundColor Gray
Write-Host "  SELECT * FROM crypto_top_performers;" -ForegroundColor Gray
Write-Host ""
Write-Host "Abre Metabase para visualizar:" -ForegroundColor White
Write-Host "  http://localhost:3000" -ForegroundColor Cyan
Write-Host ""
