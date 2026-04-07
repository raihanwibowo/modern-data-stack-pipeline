import pandas as pd
from sqlalchemy import create_engine
import clickhouse_connect
import logging
import os

logger = logging.getLogger(__name__)

client = clickhouse_connect.get_client(
            host=os.getenv('CLICKHOUSE_HOST', 'host.docker.internal'),
            port=int(os.getenv('CLICKHOUSE_HTTP_PORT', 8123)),
            username=os.getenv('CLICKHOUSE_USER', 'default'),
            password=os.getenv('CLICKHOUSE_PASSWORD', ''),
            database=os.getenv('CLICKHOUSE_DATABASE', 'default')
        )

def setup_clickhouse():
    """Setup ClickHouse database and tables"""
    logger.info("Setting up ClickHouse...")
    
    try:
        # Create database
        client.command(f"CREATE DATABASE IF NOT EXISTS {CH_CONFIG['database']}")
        
        # Create tables
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {CH_CONFIG['database']}.customer_segments (
                customer_id Int32,
                total_orders Int32,
                total_revenue Float64,
                avg_order_value Float64,
                first_order_date Date,
                last_order_date Date,
                customer_segment String,
                days_since_last_order Int32,
                is_active Bool
            ) ENGINE = MergeTree()
            ORDER BY customer_id
        """)
        
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {CH_CONFIG['database']}.customer_metrics (
                customer_id Int32,
                total_orders Int32,
                total_revenue Float64,
                avg_order_value Float64,
                first_order_date Date,
                last_order_date Date
            ) ENGINE = MergeTree()
            ORDER BY customer_id
        """)
        
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {CH_CONFIG['database']}.product_analysis (
                product_id Int32,
                order_count Int32,
                total_quantity_sold Int32,
                total_revenue Float64,
                avg_order_value Float64,
                revenue_std Nullable(Float64),
                revenue_percentile Float64,
                product_tier String
            ) ENGINE = MergeTree()
            ORDER BY product_id
        """)
        
        logger.info("ClickHouse setup complete")
    finally:
        client.close()

def verify_clickhouse_data():
    """Verify that data was loaded into ClickHouse by Airbyte"""
    
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
