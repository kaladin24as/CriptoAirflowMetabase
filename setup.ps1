# ============================================================================
# CoinGecko Cryptocurrency Pipeline - Setup Script
# ============================================================================
# Este script configura y ejecuta todo el pipeline de criptomonedas
# ============================================================================

Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "  CoinGecko Cryptocurrency Pipeline - Setup" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

# Verificar que Docker está instalado
Write-Host "[1/6] Verificando Docker..." -ForegroundColor Yellow
try {
    $dockerVersion = docker --version
    Write-Host "  ✓ Docker encontrado: $dockerVersion" -ForegroundColor Green
}
catch {
    Write-Host "  ✗ Error: Docker no está instalado o no está en el PATH" -ForegroundColor Red
    Write-Host "  Por favor instala Docker Desktop desde: https://www.docker.com/products/docker-desktop" -ForegroundColor Yellow
    exit 1
}

Write-Host ""

# Verificar que Docker Compose está disponible
Write-Host "[2/6] Verificando Docker Compose..." -ForegroundColor Yellow
try {
    $composeVersion = docker compose version
    Write-Host "  ✓ Docker Compose encontrado: $composeVersion" -ForegroundColor Green
}
catch {
    Write-Host "  ✗ Error: Docker Compose no está disponible" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Construir la imagen de ingestion
Write-Host "[3/6] Construyendo imagen de ingestion..." -ForegroundColor Yellow
Write-Host "  Esto puede tomar algunos minutos la primera vez..." -ForegroundColor Gray
docker build -t coingecko_ingestion:latest -f Dockerfile.ingestion .

if ($LASTEXITCODE -eq 0) {
    Write-Host "  ✓ Imagen construida exitosamente" -ForegroundColor Green
}
else {
    Write-Host "  ✗ Error al construir la imagen" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Iniciar servicios con Docker Compose
Write-Host "[4/6] Iniciando servicios (PostgreSQL, Airflow, Metabase)..." -ForegroundColor Yellow
docker compose up -d

if ($LASTEXITCODE -eq 0) {
    Write-Host "  ✓ Servicios iniciados" -ForegroundColor Green
}
else {
    Write-Host "  ✗ Error al iniciar servicios" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Esperar a que los servicios estén listos
Write-Host "[5/6] Esperando a que los servicios estén listos..." -ForegroundColor Yellow
Write-Host "  PostgreSQL..." -ForegroundColor Gray
Start-Sleep -Seconds 10

Write-Host "  Airflow..." -ForegroundColor Gray
Start-Sleep -Seconds 20

Write-Host "  Metabase..." -ForegroundColor Gray
Start-Sleep -Seconds 10

Write-Host "  ✓ Servicios listos" -ForegroundColor Green
Write-Host ""

# Mostrar estado de los servicios
Write-Host "[6/6] Estado de los servicios:" -ForegroundColor Yellow
docker compose ps

Write-Host ""
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "  ✓ Setup completado exitosamente!" -ForegroundColor Green
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Accede a los servicios en:" -ForegroundColor White
Write-Host "  • Airflow:   http://localhost:8080  (usuario: admin, password: admin)" -ForegroundColor Cyan
Write-Host "  • Metabase:  http://localhost:3000  (crear cuenta en primer acceso)" -ForegroundColor Cyan
Write-Host "  • PostgreSQL: localhost:5432 (usuario: postgres, password: QJ9dPbEPh6ojikGMX8kDbA)" -ForegroundColor Cyan
Write-Host ""
Write-Host "Próximos pasos:" -ForegroundColor White
Write-Host "  1. Abre Airflow en http://localhost:8080" -ForegroundColor Yellow
Write-Host "  2. Busca el DAG 'coingecko_crypto_pipeline'" -ForegroundColor Yellow
Write-Host "  3. Actívalo y ejecútalo (botón play ▶️)" -ForegroundColor Yellow
Write-Host "  4. Espera a que termine (tarda ~2-3 minutos)" -ForegroundColor Yellow
Write-Host "  5. Abre Metabase en http://localhost:3000" -ForegroundColor Yellow
Write-Host "  6. Configura la conexión a PostgreSQL" -ForegroundColor Yellow
Write-Host ""
Write-Host "Para ejecutar el pipeline manualmente:" -ForegroundColor White
Write-Host "  .\run-pipeline.ps1" -ForegroundColor Cyan
Write-Host ""
Write-Host "Para detener los servicios:" -ForegroundColor White
Write-Host "  docker compose down" -ForegroundColor Cyan
Write-Host ""
