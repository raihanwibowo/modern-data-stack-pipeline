from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from packages.clickhouse_loader import (
    create_analytics_tables,
    transform_customer_metrics_from_raw,
    transform_product_performance_from_raw,
    transform_daily_sales_summary_from_raw,
    validate_clickhouse_transformations,
    generate_clickhouse_summary
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'clickhouse_transform_pipeline',
    default_args=default_args,
    description='Transform raw_sales data in ClickHouse to analytics tables',
    schedule_interval='@daily',
    catchup=False,
)

# Define tasks
create_tables_task = PythonOperator(
    task_id='create_analytics_tables',
    python_callable=create_analytics_tables,
    dag=dag,
)

transform_customers_task = PythonOperator(
    task_id='transform_customer_metrics',
    python_callable=transform_customer_metrics_from_raw,
    dag=dag,
)

transform_products_task = PythonOperator(
    task_id='transform_product_performance',
    python_callable=transform_product_performance_from_raw,
    dag=dag,
)

transform_daily_task = PythonOperator(
    task_id='transform_daily_sales_summary',
    python_callable=transform_daily_sales_summary_from_raw,
    dag=dag,
)

quality_checks_task = PythonOperator(
    task_id='data_quality_checks',
    python_callable=validate_clickhouse_transformations,
    dag=dag,
)

summary_report_task = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_clickhouse_summary,
    dag=dag,
)

# Define task dependencies
create_tables_task >> [transform_customers_task, transform_products_task, transform_daily_task]
[transform_customers_task, transform_products_task, transform_daily_task] >> quality_checks_task
quality_checks_task >> summary_report_task
