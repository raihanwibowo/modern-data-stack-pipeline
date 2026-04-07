from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Import transformation functions (PYTHONPATH includes /opt/airflow/transforms)
from packages.analytics_transforms import (
    python_customer_analytics,
    python_product_cohorts,
    data_quality_checks,
    generate_summary
)
from packages.clickhouse_loader import (
    setup_clickhouse,
    load_customer_segments,
    load_customer_metrics,
    load_product_analysis
)

# Default arguments
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'analytics_pipeline',
    default_args=default_args,
    description='dbt + Python transformations orchestrated by Airflow',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['analytics', 'dbt', 'python'],
)

# Task 1: Check PostgreSQL connection
check_postgres = PostgresOperator(
    task_id='check_postgres_connection',
    postgres_conn_id='postgres_analytics',
    sql='SELECT 1;',
    dag=dag,
)

# Task 2: Load seed data with dbt
dbt_seed = BashOperator(
    task_id='dbt_seed',
    bash_command='cd /opt/airflow/dbt && dbt seed --profiles-dir .',
    dag=dag,
)

# Task 3: Run dbt models (SQL transformations)
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir .',
    dag=dag,
)

# Task 4: Test dbt models
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .',
    dag=dag,
)

# Task 5: Python transformation - Advanced customer analytics
python_analytics = PythonOperator(
    task_id='python_customer_analytics',
    python_callable=python_customer_analytics,
    dag=dag,
)

# Task 6: Python transformation - Product cohort analysis
python_cohorts = PythonOperator(
    task_id='python_product_cohorts',
    python_callable=python_product_cohorts,
    dag=dag,
)

# Task 7: Data quality checks
quality_checks = PythonOperator(
    task_id='data_quality_checks',
    python_callable=data_quality_checks,
    dag=dag,
)

# Task 8: Generate summary report
summary = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary,
    dag=dag,
)

# Task 9: Setup ClickHouse
setup_ch = PythonOperator(
    task_id='setup_clickhouse',
    python_callable=setup_clickhouse,
    dag=dag,
)

# Task 10-12: Load data to ClickHouse
load_ch_customer_segments = PythonOperator(
    task_id='load_clickhouse_customer_segments',
    python_callable=load_customer_segments,
    dag=dag,
)

load_ch_customer_metrics = PythonOperator(
    task_id='load_clickhouse_customer_metrics',
    python_callable=load_customer_metrics,
    dag=dag,
)

load_ch_product_analysis = PythonOperator(
    task_id='load_clickhouse_product_analysis',
    python_callable=load_product_analysis,
    dag=dag,
)

# Define task dependencies
check_postgres >> dbt_seed >> dbt_run >> dbt_test
dbt_test >> [python_analytics, python_cohorts]
[python_analytics, python_cohorts] >> quality_checks >> summary
summary >> setup_ch >> [load_ch_customer_segments, load_ch_customer_metrics, load_ch_product_analysis]
