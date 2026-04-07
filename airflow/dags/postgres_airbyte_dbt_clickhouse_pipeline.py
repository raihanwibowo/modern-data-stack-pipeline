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
from airflow.exceptions import AirflowException
import requests
import time
import os
from packages.airbyte import get_auth, trigger_airbyte_sync, check_airbyte_health, check_airbyte_job_status
from packages.clickhouse_loader import verify_clickhouse_data, transform_data_python

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def generate_summary_report():
    """Generate a summary report of the pipeline execution"""
    
    try:
        
        print("\n" + "="*80)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*80)
        
        # Raw data stats
        raw_count = client.query("SELECT count() FROM raw_sales").result_rows[0][0]
        print(f"\n📊 Raw Data:")
        print(f"   Total records: {raw_count}")
        
        # Transformed data stats
        customers = client.query("SELECT count() FROM customer_metrics").result_rows[0][0]
        products = client.query("SELECT count() FROM product_analysis").result_rows[0][0]
        regions = client.query("SELECT count() FROM regional_analysis").result_rows[0][0]
        
        print(f"\n📈 Transformed Data:")
        print(f"   Customer metrics: {customers} customers")
        print(f"   Product analysis: {products} products")
        print(f"   Regional analysis: {regions} region-month combinations")
        
        # Top insights
        top_customer = client.query("""
            SELECT customer_id, total_revenue 
            FROM customer_metrics 
            ORDER BY total_revenue DESC 
            LIMIT 1
        """).result_rows
        
        if top_customer:
            print(f"\n🏆 Top Customer:")
            print(f"   Customer ID: {top_customer[0][0]}")
            print(f"   Total Revenue: ${top_customer[0][1]:,.2f}")
        
        print("\n" + "="*80)
        print("✓ Pipeline completed successfully!")
        print("="*80 + "\n")
        
        return True
        
    except Exception as e:
        print(f"Warning: Could not generate summary: {str(e)}")
        return True  # Don't fail the pipeline for summary issues


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
        op_kwargs={'connection_id': POSTGRES_TO_CLICKHOUSE_CONNECTION_ID},
    )
    
    # Step 3: Monitor Airbyte sync status
    monitor_sync = PythonSensor(
        task_id='monitor_airbyte_sync',
        python_callable=check_airbyte_job_status,
        op_kwargs={
            'job_id': "{{ task_instance.xcom_pull(task_ids='trigger_airbyte_sync') }}"
        },
        poke_interval=30,  # Check every 30 seconds
        timeout=3600,  # Timeout after 1 hour
        mode='poke',
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
