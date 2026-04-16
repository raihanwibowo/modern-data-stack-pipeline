"""
Complete ELT Pipeline: Postgres → Airbyte → ClickHouse → dbt → ClickHouse
This DAG orchestrates:
1. Extract from Postgres to ClickHouse using Airbyte
2. Transform data using dbt (reads from ClickHouse raw tables)
3. Load transformed data back to ClickHouse
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.bash import BashOperator
from packages.airbyte import get_auth, trigger_airbyte_sync, check_airbyte_health, check_airbyte_job_status
from packages.clickhouse_loader import verify_clickhouse_data, transform_data_python
from packages.summary_report import generate_summary_report

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    'postgres_airbyte_dbt_clickhouse_pipeline',
    default_args=default_args,
    description='Full ELT: Postgres → Airbyte → ClickHouse → Transform → ClickHouse',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['airbyte', 'dbt', 'clickhouse', 'elt', 'postgres'],
) as dag:
    
    # Step 1: Check Airbyte health
    check_airbyte = PythonOperator(
        task_id='check_airbyte_health',
        python_callable=check_airbyte_health,
    )
    
    # Step 2: Trigger Airbyte sync (Postgres → ClickHouse)
    trigger_sync = PythonOperator(
        task_id='trigger_airbyte_sync',
        python_callable=trigger_airbyte_sync,
        op_kwargs={
            'password': "{{ var.value.AIRBYTE_PASSWORD }}",
            'connection_id': "{{ var.value.AIRBYTE_CONNECTION_ID_1 }}"
        }
    )
    
    # Step 3: Monitor Airbyte sync status
    monitor_sync = PythonSensor(
        task_id='monitor_airbyte_sync',
        python_callable=check_airbyte_job_status,
        op_kwargs={
            'password': "{{ var.value.AIRBYTE_PASSWORD }}",
            'job_id': "{{ task_instance.xcom_pull(task_ids='trigger_airbyte_sync') }}"
        },
        poke_interval=30,  # Check every 30 seconds
        timeout=3600,  # Timeout after 1 hour
        mode='reschedule',
    )
    
    # Step 4: Verify data in ClickHouse
    verify_data = PythonOperator(
        task_id='verify_clickhouse_data',
        python_callable=verify_clickhouse_data,
    )
    
    # Step 5: Transform data in ClickHouse
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data_python,
    )
    
    # Step 6: Generate summary report
    generate_summary = PythonOperator(
        task_id='generate_summary',
        python_callable=generate_summary_report,
    )
    
    # Define task dependencies
    check_airbyte >> trigger_sync >> monitor_sync >> verify_data >> transform_data >> generate_summary
