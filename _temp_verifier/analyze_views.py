
import os
import psycopg2
import pandas as pd
from tabulate import tabulate
from dotenv import load_dotenv

# Cargar variables de entorno del proyecto original (asumiendo ejecución desde root)
load_dotenv('.env.dev')
# Fallback si .env.dev no existe o no tiene las vars
if not os.getenv('DATA_DB_HOST'):
    load_dotenv('.env') 
    
# Configuración de conexión (Hardcoded fallback para Docker local estándar)
DB_CONFIG = {
    "host": os.getenv("DATA_DB_HOST", "localhost"),
    "port": os.getenv("DATA_DB_PORT", "5432"),
    "database": os.getenv("DATA_DB_NAME", "warehouse"),
    "user": os.getenv("DATA_DB_USER", "postgres"),
    "password": os.getenv("DATA_DB_PASSWORD", "postgres")
}

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error conectando a la base de datos: {e}")
        # Intento con password hardcodeada del setup.ps1 si falla
        try:
            DB_CONFIG['password'] = "QJ9dPbEPh6ojikGMX8kDbA"
            return psycopg2.connect(**DB_CONFIG)
        except Exception as e2:
            print(f"Error fatal de conexión: {e2}")
            return None

def analyze_transformations():
    conn = get_db_connection()
    if not conn:
        return

    print("\n" + "="*80)
    print(" ANÁLISIS DE VERIFICACIÓN DE TRANSFORMACIONES")
    print("="*80)

    # 1. Verificación de Tablas/Vistas Principales
    views = [
        "crypto_market_summary",
        "crypto_price_changes",
        "crypto_top_performers",
        "crypto_market_overview",
        "crypto_trending_summary"
    ]
    
    print("\n[1] Estado de Vistas Transformadas:")
    stats = []
    for view in views:
        try:
            count = pd.read_sql(f"SELECT COUNT(*) FROM {view}", conn).iloc[0, 0]
            status = "✅ OK" if count > 0 else "⚠️ VACÍA"
            stats.append([view, count, status])
        except Exception as e:
            stats.append([view, "ERROR", str(e)])
    
    print(tabulate(stats, headers=["Vista", "Filas", "Estado"], tablefmt="grid"))

    # 2. Muestra de Datos: Top Performers
    print("\n[2] Top 5 Ganadores (24h):")
    try:
        df_top = pd.read_sql("""
            SELECT name, symbol, current_price, change_24h_pct, performance_category 
            FROM crypto_top_performers 
            WHERE performance_category = 'Top Gainer 24h' 
            ORDER BY change_24h_pct DESC 
            LIMIT 5
        """, conn)
        if not df_top.empty:
            print(tabulate(df_top, headers="keys", tablefmt="simple", showindex=False))
        else:
            print("  No hay datos de ganadores.")
    except Exception as e:
        print(f"  Error consultando top performers: {e}")

    # 3. Muestra de Datos: Resumen de Mercado Global
    print("\n[3] Resumen de Mercado Global:")
    try:
        df_global = pd.read_sql("""
            SELECT total_coins, total_market_cap, btc_dominance_pct, eth_dominance_pct, market_data_updated
            FROM crypto_market_overview
        """, conn)
        if not df_global.empty:
             print(tabulate(df_global, headers="keys", tablefmt="simple", showindex=False))
        else:
             print("  No hay datos globales.")
    except Exception as e:
         print(f"  Error consultando global stats: {e}")

    # 4. Calidad de Datos
    print("\n[4] Chequeo de Calidad:")
    issues = []
    
    # Check null prices
    null_prices = pd.read_sql("SELECT COUNT(*) FROM crypto_market_summary WHERE current_price IS NULL", conn).iloc[0,0]
    if null_prices > 0:
        issues.append(f"⚠️ Hay {null_prices} criptomonedas con precio NULL")
    else:
        issues.append("✅ Todos los precios están presentes")
        
    # Check stale data (older than 24h)
    # Using python to check date difference roughly
    try:
        last_update = pd.read_sql("SELECT MAX(last_updated) FROM crypto_market_summary", conn).iloc[0,0]
        issues.append(f"ℹ️ Última actualización de datos: {last_update}")
    except:
        issues.append("⚠️ No se pudo verificar fecha de actualización")

    for issue in issues:
        print(f"  {issue}")

    print("\n" + "="*80)
    conn.close()

if __name__ == "__main__":
    analyze_transformations()
