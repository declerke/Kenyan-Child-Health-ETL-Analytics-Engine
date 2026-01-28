from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import pandas as pd
from sqlalchemy import create_engine, text
import logging

DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'kenya_health_db',
    'user': 'user',
    'password': '7510'
}

CSV_PATH = '/opt/airflow/data/raw/health_indicators_ken.csv'
STAGING_TABLE = 'stg_health_indicators_raw'

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def create_connection_string():
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def ingest_csv_to_postgres(**context):
    logger = logging.getLogger(__name__)
    
    try:
        engine = create_engine(create_connection_string())
        
        logger.info(f"Reading CSV from: {CSV_PATH}")
        
        df = pd.read_csv(
            CSV_PATH,
            encoding='utf-8',
            low_memory=False
        )
        
        logger.info(f"CSV loaded successfully: {len(df)} rows, {len(df.columns)} columns")
        
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('[^a-z0-9_]', '', regex=True)
        
        df['ingestion_timestamp'] = datetime.now()
        df['source_file'] = 'health_indicators_ken.csv'
        
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_TABLE} CASCADE"))
            conn.commit()
        
        logger.info(f"Writing to staging table: {STAGING_TABLE}")
        df.to_sql(
            name=STAGING_TABLE,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) as count FROM {STAGING_TABLE}"))
            count = result.fetchone()[0]
            logger.info(f"Successfully loaded {count} rows into {STAGING_TABLE}")
        
        logger.info(f"\n{df.head(3).to_string()}")
        
        context['ti'].xcom_push(key='rows_ingested', value=len(df))
        context['ti'].xcom_push(key='columns_count', value=len(df.columns))
        
    except FileNotFoundError:
        logger.error(f"CSV file not found at: {CSV_PATH}")
        raise
    except Exception as e:
        logger.error(f"Error during CSV ingestion: {str(e)}")
        raise

def validate_staging_data(**context):
    logger = logging.getLogger(__name__)
    engine = create_engine(create_connection_string())
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) as count FROM {STAGING_TABLE}"))
            row_count = result.fetchone()[0]
            
            if row_count == 0:
                raise ValueError("Staging table is empty!")
            
            result = conn.execute(text(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{STAGING_TABLE}'
            """))
            columns = [row[0] for row in result.fetchall()]
            
            if 'indicator' in columns and 'value' in columns:
                result = conn.execute(text(f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(indicator) as non_null_indicators,
                        COUNT(value) as non_null_values
                    FROM {STAGING_TABLE}
                """))
                stats = result.fetchone()
                logger.info(f"Data quality - Indicators: {stats[1]}/{stats[0]}, Values: {stats[2]}/{stats[0]}")
            
    except Exception as e:
        logger.error(f"Staging validation failed: {str(e)}")
        raise

def log_pipeline_completion(**context):
    logger = logging.getLogger(__name__)
    rows_ingested = context['ti'].xcom_pull(task_ids='ingest_csv', key='rows_ingested')
    
    logger.info("=" * 60)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Execution Date: {context['execution_date']}")
    logger.info(f"Rows Ingested: {rows_ingested}")
    logger.info(f"Pipeline Status: SUCCESS ✓")
    logger.info("=" * 60)

with DAG(
    'kenya_health_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Kenya WHO health indicators - Living Goods demonstration',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['kenya', 'health', 'who', 'living_goods'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_csv',
        python_callable=ingest_csv_to_postgres,
        provide_context=True,
    )

    validate_task = PythonOperator(
        task_id='validate_staging',
        python_callable=validate_staging_data,
        provide_context=True,
    )

    dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='cd /opt/airflow/dbt && dbt run --models staging --profiles-dir .',
    )

    dbt_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command='cd /opt/airflow/dbt && dbt run --models intermediate --profiles-dir .',
    )

    dbt_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command='cd /opt/airflow/dbt && dbt run --models marts --profiles-dir .',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .',
    )

    completion_task = PythonOperator(
        task_id='log_completion',
        python_callable=log_pipeline_completion,
        provide_context=True,
    )

    ingest_task >> validate_task >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test >> completion_task