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
with DAG('regional_data_pipeline_dag',
    default_args=default_args,
    description='Regional Data Pipeline: Airbyte sync + ClickHouse transformations',
    schedule_interval='0 3 * * *',  # Run daily at 3 AM
    catchup=False,
    tags=['airbyte', 'clickhouse', 'regional', 'elt'],
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
            'connection_id': "{{ var.value.AIRBYTE_CONNECTION_ID_2 }}"
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
    
    # Step 5: Transform data in ClickHouse using Python
    transform_data = PythonOperator(
        task_id='transform_clickhouse_data',
        python_callable=transform_data_python,
    )
    
    # Define task dependencies
    check_airbyte >> trigger_sync >> monitor_sync >> verify_data >> transform_data
