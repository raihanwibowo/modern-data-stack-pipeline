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
import clickhouse_connect

# Configuration
AIRBYTE_URL = os.getenv('AIRBYTE_URL', 'http://host.docker.internal:8000')
AIRBYTE_API_URL = f"{AIRBYTE_URL}/api/v1"
AIRBYTE_USERNAME = os.getenv('AIRBYTE_USERNAME', 'raihan.wibowo@gmail.com')
AIRBYTE_PASSWORD = os.getenv('AIRBYTE_PASSWORD', 'Du0T3bc411nGMeaqR8KcIVU2h01c0ETt')

# Airbyte Connection ID for Postgres → ClickHouse
POSTGRES_TO_CLICKHOUSE_CONNECTION_ID = os.getenv(
    'AIRBYTE_POSTGRES_TO_CLICKHOUSE_CONNECTION_ID',
    'b706dc46-cc44-4be4-b24f-92ac889db648'
)

client = clickhouse_connect.get_client(
            host=os.getenv('CLICKHOUSE_HOST', 'host.docker.internal'),
            port=int(os.getenv('CLICKHOUSE_HTTP_PORT', 8123)),
            username=os.getenv('CLICKHOUSE_USER', 'default'),
            password=os.getenv('CLICKHOUSE_PASSWORD', '1234'),
            database=os.getenv('CLICKHOUSE_DATABASE', 'analytics')
        )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def get_auth():
    """Get Airbyte authentication credentials"""
    if AIRBYTE_USERNAME and AIRBYTE_PASSWORD:
        return (AIRBYTE_USERNAME, AIRBYTE_PASSWORD)
    return None


def check_airbyte_health():
    """Check if Airbyte API is accessible"""
    try:
        response = requests.get(f"{AIRBYTE_URL}/api/v1/health", timeout=10)
        response.raise_for_status()
        print("✓ Airbyte API is healthy")
        return True
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Airbyte API is not accessible: {str(e)}")


def trigger_airbyte_sync(connection_id: str):
    """
    Trigger Airbyte sync from Postgres to ClickHouse
    
    Args:
        connection_id: Airbyte connection ID
    
    Returns:
        job_id: The Airbyte job ID for status checking
    """
    print(f"Triggering Airbyte sync: Postgres → ClickHouse")
    print(f"Connection ID: {connection_id}")
    
    url = f"{AIRBYTE_API_URL}/connections/sync"
    payload = {"connectionId": connection_id}
    auth = get_auth()
    
    try:
        response = requests.post(url, json=payload, auth=auth, timeout=30)
        response.raise_for_status()
        
        job_data = response.json()
        job_id = job_data.get('job', {}).get('id')
        
        if not job_id:
            raise AirflowException("No job ID returned from Airbyte")
        
        print(f"✓ Successfully triggered sync. Job ID: {job_id}")
        return job_id
        
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Failed to trigger Airbyte sync: {str(e)}")


def check_airbyte_job_status(job_id: str):
    """
    Check the status of an Airbyte job
    
    Args:
        job_id: Airbyte job ID
    
    Returns:
        bool: True if job is complete, False if still running
    """
    if not job_id or job_id == 'None':
        raise AirflowException("Invalid job ID")
    
    print(f"Checking Airbyte job status: {job_id}")
    
    url = f"{AIRBYTE_API_URL}/jobs/get"
    payload = {"id": job_id}
    auth = get_auth()
    
    try:
        response = requests.post(url, json=payload, auth=auth, timeout=30)
        response.raise_for_status()
        
        job_data = response.json()
        status = job_data.get('job', {}).get('status')
        
        print(f"Current status: {status}")
        
        if status == 'succeeded':
            print(f"✓ Airbyte sync completed successfully!")
            
            # Print sync stats if available
            job_info = job_data.get('job', {})
            attempts = job_info.get('attempts', [])
            if attempts:
                last_attempt = attempts[-1]
                records_synced = last_attempt.get('recordsSynced', 0)
                bytes_synced = last_attempt.get('bytesSynced', 0)
                print(f"  Records synced: {records_synced}")
                print(f"  Bytes synced: {bytes_synced}")
            
            return True
            
        elif status == 'failed':
            raise AirflowException(f"Airbyte job {job_id} failed")
        elif status == 'cancelled':
            raise AirflowException(f"Airbyte job {job_id} was cancelled")
        elif status in ['pending', 'running', 'incomplete']:
            print(f"Job still in progress: {status}")
            return False
        else:
            raise AirflowException(f"Unknown job status: {status}")
            
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Failed to check Airbyte job status: {str(e)}")


def verify_clickhouse_data():
    """Verify that data was loaded into ClickHouse by Airbyte"""
    import clickhouse_connect
    
    try:
        # Try to connect to the analytics database first
        databases_to_check = ['analytics', 'default', os.getenv('CLICKHOUSE_DATABASE', 'default')]
        found_database = None
        
        for db in databases_to_check:
            try:
                # Check if raw_sales exists in this database
                tables = client.query("SHOW TABLES").result_rows
                table_names = [row[0] for row in tables]
                if 'raw_sales' in table_names:
                    found_database = db
                    print(f"✓ Found raw_sales table in database: {db}")
                    break
            except:
                continue
        
        if not client:
            raise AirflowException(
                f"Could not find raw_sales table in databases: {databases_to_check}. "
                f"Please check your Airbyte connection configuration."
            )
        
        # Get table structure
        describe_result = client.query("DESCRIBE TABLE raw_sales")
        columns = [(row[0], row[1]) for row in describe_result.result_rows]
        print(f"Table columns: {[col[0] for col in columns]}")
        
        # Check if table has data
        result = client.query("SELECT count() FROM raw_sales")
        count = result.result_rows[0][0]
        
        print(f"✓ Found {count} records in {found_database}.raw_sales table")
        
        if count == 0:
            raise AirflowException(f"No data found in raw_sales table after Airbyte sync")
        
        return {'database': found_database, 'columns': [col[0] for col in columns], 'count': count}
        
    except Exception as e:
        raise AirflowException(f"Failed to verify ClickHouse data: {str(e)}")


def run_dbt_on_clickhouse():
    """
    Run dbt transformations on ClickHouse data
    Note: This assumes you have a ClickHouse profile configured in dbt
    """
    print("Running dbt transformations on ClickHouse data...")
    
    # This is a placeholder - you'll need to configure dbt for ClickHouse
    # For now, we'll use a Python-based transformation approach
    print("⚠ Note: dbt native ClickHouse support requires dbt-clickhouse adapter")
    print("  Falling back to Python-based transformations")
    
    return True


def transform_data_python():
    """
    Transform data in ClickHouse using Python
    Columns in raw_sales: order_id, price, quantity, order_date, product_id, customer_id
    """    
    try:
        
        print("Creating staging view...")
        # Create staging view with standardized column names
        client.command("""
            CREATE OR REPLACE VIEW stg_sales AS
            SELECT 
                order_id,
                customer_id,
                product_id,
                order_date as sale_date,
                quantity,
                price as unit_price,
                quantity * price as total_amount
            FROM raw_sales
            WHERE quantity > 0 AND price > 0
        """)
        
        print("Creating customer metrics table...")
        # Customer analytics
        client.command("DROP TABLE IF EXISTS customer_metrics")
        client.command("""
            CREATE TABLE customer_metrics
            ENGINE = MergeTree()
            ORDER BY tuple()
            AS
            SELECT 
                customer_id,
                count(*) as total_orders,
                sum(total_amount) as total_revenue,
                avg(total_amount) as avg_order_value,
                min(sale_date) as first_order_date,
                max(sale_date) as last_order_date,
                count(DISTINCT product_id) as unique_products,
                sum(quantity) as total_items_purchased
            FROM stg_sales
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
        """)
        
        print("Creating product analysis table...")
        # Product analytics with tier labeling
        client.command("DROP TABLE IF EXISTS product_analysis")
        client.command("""
            CREATE TABLE product_analysis
            ENGINE = MergeTree()
            ORDER BY tuple()
            AS
            WITH product_stats AS (
                SELECT 
                    product_id,
                    count(*) as total_orders,
                    sum(quantity) as total_quantity_sold,
                    sum(total_amount) as total_revenue,
                    avg(unit_price) as avg_price,
                    min(unit_price) as min_price,
                    max(unit_price) as max_price,
                    count(DISTINCT customer_id) as unique_customers
                FROM stg_sales
                WHERE product_id IS NOT NULL
                GROUP BY product_id
            ),
            revenue_percentiles AS (
                SELECT 
                    quantile(0.33)(total_revenue) as p33,
                    quantile(0.67)(total_revenue) as p67
                FROM product_stats
            )
            SELECT 
                ps.product_id,
                ps.total_orders,
                ps.total_quantity_sold,
                ps.total_revenue,
                ps.avg_price,
                ps.min_price,
                ps.max_price,
                ps.unique_customers,
                CASE 
                    WHEN ps.total_revenue >= rp.p67 THEN 'Gold'
                    WHEN ps.total_revenue >= rp.p33 THEN 'Silver'
                    ELSE 'Bronze'
                END as product_tier
            FROM product_stats ps
            CROSS JOIN revenue_percentiles rp
        """)
        
        print("Creating daily sales summary table...")
        # Daily sales summary
        client.command("DROP TABLE IF EXISTS daily_sales_summary")
        client.command("""
            CREATE TABLE daily_sales_summary
            ENGINE = MergeTree()
            ORDER BY tuple()
            AS
            SELECT 
                sale_date,
                count(*) as total_orders,
                sum(total_amount) as total_revenue,
                avg(total_amount) as avg_order_value,
                sum(quantity) as total_items_sold,
                count(DISTINCT customer_id) as unique_customers,
                count(DISTINCT product_id) as unique_products
            FROM stg_sales
            WHERE sale_date IS NOT NULL
            GROUP BY sale_date
        """)
        
        print("Creating monthly sales summary table...")
        # Monthly sales summary
        client.command("DROP TABLE IF EXISTS monthly_sales_summary")
        client.command("""
            CREATE TABLE monthly_sales_summary
            ENGINE = MergeTree()
            ORDER BY tuple()
            AS
            SELECT 
                toStartOfMonth(sale_date) as sale_month,
                count(*) as total_orders,
                sum(total_amount) as total_revenue,
                avg(total_amount) as avg_order_value,
                sum(quantity) as total_items_sold,
                count(DISTINCT customer_id) as unique_customers,
                count(DISTINCT product_id) as unique_products
            FROM stg_sales
            WHERE sale_date IS NOT NULL
            GROUP BY sale_month
        """)
        
        print("Creating customer product affinity table...")
        # Customer-Product affinity
        client.command("DROP TABLE IF EXISTS customer_product_affinity")
        client.command("""
            CREATE TABLE customer_product_affinity
            ENGINE = MergeTree()
            ORDER BY tuple()
            AS
            SELECT 
                customer_id,
                product_id,
                count(*) as purchase_count,
                sum(quantity) as total_quantity,
                sum(total_amount) as total_spent,
                min(sale_date) as first_purchase_date,
                max(sale_date) as last_purchase_date
            FROM stg_sales
            WHERE customer_id IS NOT NULL AND product_id IS NOT NULL
            GROUP BY customer_id, product_id
        """)
        
        # Verify transformations
        metrics_count = client.query("SELECT count() FROM customer_metrics").result_rows[0][0]
        products_count = client.query("SELECT count() FROM product_analysis").result_rows[0][0]
        daily_count = client.query("SELECT count() FROM daily_sales_summary").result_rows[0][0]
        monthly_count = client.query("SELECT count() FROM monthly_sales_summary").result_rows[0][0]
        affinity_count = client.query("SELECT count() FROM customer_product_affinity").result_rows[0][0]
        
        print(f"✓ Transformation complete:")
        print(f"  - Customer metrics: {metrics_count} customers")
        print(f"  - Product analysis: {products_count} products")
        print(f"  - Daily sales summary: {daily_count} days")
        print(f"  - Monthly sales summary: {monthly_count} months")
        print(f"  - Customer-Product affinity: {affinity_count} combinations")
        
        return True
        
    except Exception as e:
        raise AirflowException(f"Failed to transform data: {str(e)}")


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
    # check_airbyte >> trigger_sync >> monitor_sync >> 
    verify_data >> transform_data >> generate_summary
