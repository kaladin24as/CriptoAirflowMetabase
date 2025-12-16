from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': os.getenv('ALERT_EMAIL', 'admin@example.com').split(','),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=2),
}

def slack_failure_callback(context):
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
        
        slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        if not slack_webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not configured, skipping Slack notification")
            return
        
        slack_hook = SlackWebhookHook(
            http_conn_id='slack_webhook',
            webhook_token=slack_webhook_url,
            message=f"""
:red_circle: *Task Failed*
*DAG*: {context.get('task_instance').dag_id}
*Task*: {context.get('task_instance').task_id}
*Execution Time*: {context.get('execution_date')}
*Log URL*: {context.get('task_instance').log_url}
            """,
            username='Airflow Bot'
        )
        slack_hook.execute()
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")

if os.getenv('SLACK_WEBHOOK_URL'):
    default_args['on_failure_callback'] = slack_failure_callback

with DAG(
    dag_id='coingecko_crypto_pipeline',
    default_args=default_args,
    description='CoinGecko cryptocurrency data pipeline with real-time updates',
    schedule_interval='*/5 * * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['production', 'cryptocurrency', 'coingecko', 'real-time'],
) as dag:

    def validate_environment(**context):
        required_vars = [
            'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD'
        ]
        missing = [var for var in required_vars if not os.getenv(var)]
        
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        logger.info("✓ Environment validation passed")
        return True
    
    preflight_check = PythonOperator(
        task_id='preflight_check',
        python_callable=validate_environment,
    )

    from airflow.operators.python import BranchPythonOperator
    
    def check_ingestion_status(**context):
        new_data_arrived = True 
        if new_data_arrived:
            return "transformation.dbt_deps"
        else:
            return "skip_transformation"

    with TaskGroup(group_id='ingestion') as ingestion_group:
        
        run_dlt_pipeline = DockerOperator(
            task_id='run_coingecko_extraction',
            image='coingecko_ingestion:latest',
            api_version='auto',
            auto_remove=True,
            docker_url='unix://var/run/docker.sock',
            network_mode='antigravity_net',
            mount_tmp_dir=False,
            mounts=[
                {
                    'source': os.path.abspath('./etl/extract'),
                    'target': '/app',
                    'type': 'bind'
                }
            ],
            environment={
                'DATA_DB_HOST': os.getenv('DATA_DB_HOST', 'postgres'),
                'DATA_DB_PORT': os.getenv('DATA_DB_PORT', '5432'),
                'DATA_DB_NAME': os.getenv('DATA_DB_NAME', 'warehouse'),
                'DATA_DB_USER': os.getenv('DATA_DB_USER', 'postgres'),
                'DATA_DB_PASSWORD': os.getenv('DATA_DB_PASSWORD', ''),
            },
        )
        
        def validate_ingestion_quality(**context):
            import psycopg2
            
            conn = psycopg2.connect(
                host=os.getenv('DATA_DB_HOST', 'postgres'),
                port=os.getenv('DATA_DB_PORT', '5432'),
                dbname=os.getenv('DATA_DB_NAME', 'warehouse'),
                user=os.getenv('DATA_DB_USER', 'postgres'),
                password=os.getenv('DATA_DB_PASSWORD', '')
            )
            
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM crypto_raw.market_data")
            market_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM crypto_raw.trending_coins")
            trending_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM crypto_raw.global_stats")
            global_count = cursor.fetchone()[0]
            
            if market_count == 0:
                raise ValueError("No market data ingested - pipeline aborted")
            
            logger.info(f"✓ Ingestion quality check passed:")
            logger.info(f"  - Market data: {market_count} cryptocurrencies")
            logger.info(f"  - Trending coins: {trending_count} coins")
            logger.info(f"  - Global stats: {global_count} records")
            
            cursor.close()
            conn.close()
            return {
                'market_data': market_count,
                'trending': trending_count,
                'global_stats': global_count
            }
        
        quality_check_ingestion = PythonOperator(
            task_id='quality_check',
            python_callable=validate_ingestion_quality,
        )

    with TaskGroup(group_id='transformation') as transformation_group:
        
        def run_sql_transformations(**context):
            import psycopg2
            
            conn = psycopg2.connect(
                host=os.getenv('DATA_DB_HOST', 'postgres'),
                port=os.getenv('DATA_DB_PORT', '5432'),
                dbname=os.getenv('DATA_DB_NAME', 'warehouse'),
                user=os.getenv('DATA_DB_USER', 'postgres'),
                password=os.getenv('DATA_DB_PASSWORD', '')
            )
            
            cursor = conn.cursor()
            
            sql_file = '/opt/airflow/etl/transform/crypto_transformations.sql'
            logger.info(f"Executing transformations from {sql_file}")
            
            with open(sql_file, 'r') as f:
                sql_script = f.read()
            
            cursor.execute(sql_script)
            conn.commit()
            
            logger.info("✓ SQL transformations completed successfully")
            
            cursor.close()
            conn.close()
            return True
        
        transform_crypto_data = PythonOperator(
            task_id='transform_crypto_data',
            python_callable=run_sql_transformations,
        )
        
        def validate_transformations(**context):
            import psycopg2
            
            conn = psycopg2.connect(
                host=os.getenv('DATA_DB_HOST', 'postgres'),
                port=os.getenv('DATA_DB_PORT', '5432'),
                dbname=os.getenv('DATA_DB_NAME', 'warehouse'),
                user=os.getenv('DATA_DB_USER', 'postgres'),
                password=os.getenv('DATA_DB_PASSWORD', '')
            )
            
            cursor = conn.cursor()
            
            views_to_check = [
                'crypto_market_summary',
                'crypto_price_changes',
                'crypto_top_performers',
                'crypto_market_overview',
                'crypto_trending_summary'
            ]
            
            for view in views_to_check:
                cursor.execute(f"SELECT COUNT(*) FROM {view}")
                count = cursor.fetchone()[0]
                logger.info(f"✓ View {view}: {count} rows")
                
                if count == 0 and view != 'crypto_market_overview':
                    logger.warning(f"Warning: View {view} has no data")
            
            cursor.close()
            conn.close()
            return True
        
        validate_transforms = PythonOperator(
            task_id='validate_transformations',
            python_callable=validate_transformations,
        )
        
        transform_crypto_data >> validate_transforms

    def send_success_notification(**context):
        execution_date = context['execution_date']
        logger.info(f"✓ Pipeline completed successfully for {execution_date}")
        return True
    
    success_notification = PythonOperator(
        task_id='success_notification',
        python_callable=send_success_notification,
    )

    preflight_check >> ingestion_group >> transformation_group >> success_notification
