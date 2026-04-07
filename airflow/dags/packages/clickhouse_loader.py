import pandas as pd
from sqlalchemy import create_engine
import clickhouse_connect
import logging

logger = logging.getLogger(__name__)

# PostgreSQL connection
PG_CONN = "postgresql://postgres:postgres@host.docker.internal:5432/analytics"

# ClickHouse connection
CH_CONFIG = {
    'host': 'host.docker.internal',
    'port': 8123,
    'database': 'analytics',
    'user': 'default',
    'password': '1234'
}

def setup_clickhouse():
    """Setup ClickHouse database and tables"""
    logger.info("Setting up ClickHouse...")
    
    client = clickhouse_connect.get_client(
        host=CH_CONFIG['host'],
        port=CH_CONFIG['port'],
        username=CH_CONFIG['user'],
        password=CH_CONFIG['password']
    )
    
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

def load_to_clickhouse(table_name):
    """Load a table from PostgreSQL to ClickHouse"""
    logger.info(f"Loading {table_name} to ClickHouse...")
    
    # Extract from PostgreSQL
    pg_engine = create_engine(PG_CONN)
    df = pd.read_sql(f"SELECT * FROM {table_name}", pg_engine)
    logger.info(f"Extracted {len(df)} rows from PostgreSQL")
    
    # Convert date columns
    date_columns = df.select_dtypes(include=['datetime64']).columns
    for col in date_columns:
        df[col] = pd.to_datetime(df[col]).dt.date
    
    # Connect to ClickHouse
    client = clickhouse_connect.get_client(
        host=CH_CONFIG['host'],
        port=CH_CONFIG['port'],
        username=CH_CONFIG['user'],
        password=CH_CONFIG['password']
    )
    
    try:
        # Load to ClickHouse
        client.insert_df(f"{CH_CONFIG['database']}.{table_name}", df)
        logger.info(f"Loaded {len(df)} rows to ClickHouse table: {table_name}")
        
        # Verify
        result = client.query(f"SELECT COUNT(*) FROM {CH_CONFIG['database']}.{table_name}")
        count = result.result_rows[0][0]
        logger.info(f"Verified: {count} rows in ClickHouse")
    finally:
        client.close()

def load_customer_segments():
    """Load customer_segments to ClickHouse"""
    load_to_clickhouse('customer_segments')

def load_customer_metrics():
    """Load customer_metrics to ClickHouse"""
    load_to_clickhouse('customer_metrics')

def load_product_analysis():
    """Load product_analysis to ClickHouse"""
    load_to_clickhouse('product_analysis')

def get_clickhouse_client():
    """Get ClickHouse client connection"""
    return clickhouse_connect.get_client(
        host=CH_CONFIG['host'],
        port=CH_CONFIG['port'],
        username=CH_CONFIG['user'],
        password=CH_CONFIG['password'],
        database=CH_CONFIG['database']
    )

def create_analytics_tables():
    """Create analytics tables for ClickHouse transformations"""
    logger.info("Creating analytics tables in ClickHouse...")
    client = get_clickhouse_client()
    
    try:
        # Product performance table
        client.command("""
            CREATE TABLE IF NOT EXISTS product_performance (
                product_id UInt32,
                total_quantity_sold UInt32,
                total_revenue Float64,
                order_count UInt32,
                avg_quantity_per_order Float64,
                avg_price Float64
            ) ENGINE = MergeTree()
            ORDER BY product_id
        """)
        
        # Daily sales summary table
        client.command("""
            CREATE TABLE IF NOT EXISTS daily_sales_summary (
                order_date Date,
                total_orders UInt32,
                total_revenue Float64,
                total_quantity UInt32,
                unique_customers UInt32,
                unique_products UInt32,
                avg_order_value Float64
            ) ENGINE = MergeTree()
            ORDER BY order_date
        """)
        
        logger.info("Analytics tables created successfully")
    finally:
        client.close()

def transform_customer_metrics_from_raw():
    """Transform raw_sales into customer_metrics in ClickHouse"""
    logger.info("Transforming customer metrics from raw_sales...")
    client = get_clickhouse_client()
    
    try:
        # Clear existing data
        client.command("TRUNCATE TABLE IF EXISTS customer_metrics")
        
        # Insert transformed data
        client.command("""
            INSERT INTO customer_metrics
            SELECT 
                customer_id,
                COUNT(*) as total_orders,
                SUM(quantity * price) as total_revenue,
                AVG(quantity * price) as avg_order_value,
                MIN(order_date) as first_order_date,
                MAX(order_date) as last_order_date
            FROM raw_sales
            GROUP BY customer_id
        """)
        
        result = client.query("SELECT COUNT(*) as count FROM customer_metrics")
        count = result.result_rows[0][0]
        logger.info(f"Transformed {count} customer records")
        return count
    finally:
        client.close()

def transform_product_performance_from_raw():
    """Transform raw_sales into product_performance in ClickHouse"""
    logger.info("Transforming product performance from raw_sales...")
    client = get_clickhouse_client()
    
    try:
        # Clear existing data
        client.command("TRUNCATE TABLE IF EXISTS product_performance")
        
        # Insert transformed data
        client.command("""
            INSERT INTO product_performance
            SELECT 
                product_id,
                SUM(quantity) as total_quantity_sold,
                SUM(quantity * price) as total_revenue,
                COUNT(*) as order_count,
                AVG(quantity) as avg_quantity_per_order,
                AVG(price) as avg_price
            FROM raw_sales
            GROUP BY product_id
        """)
        
        result = client.query("SELECT COUNT(*) as count FROM product_performance")
        count = result.result_rows[0][0]
        logger.info(f"Transformed {count} product records")
        return count
    finally:
        client.close()

def transform_daily_sales_summary_from_raw():
    """Transform raw_sales into daily_sales_summary in ClickHouse"""
    logger.info("Transforming daily sales summary from raw_sales...")
    client = get_clickhouse_client()
    
    try:
        # Clear existing data
        client.command("TRUNCATE TABLE IF EXISTS daily_sales_summary")
        
        # Insert transformed data
        client.command("""
            INSERT INTO daily_sales_summary
            SELECT 
                order_date,
                COUNT(*) as total_orders,
                SUM(quantity * price) as total_revenue,
                SUM(quantity) as total_quantity,
                COUNT(DISTINCT customer_id) as unique_customers,
                COUNT(DISTINCT product_id) as unique_products,
                AVG(quantity * price) as avg_order_value
            FROM raw_sales
            GROUP BY order_date
            ORDER BY order_date
        """)
        
        result = client.query("SELECT COUNT(*) as count FROM daily_sales_summary")
        count = result.result_rows[0][0]
        logger.info(f"Transformed {count} daily summary records")
        return count
    finally:
        client.close()

def validate_clickhouse_transformations():
    """Run data quality checks on ClickHouse transformations"""
    logger.info("Running ClickHouse data quality checks...")
    client = get_clickhouse_client()
    
    try:
        # Check raw_sales count
        raw_count = client.query("SELECT COUNT(*) FROM raw_sales").result_rows[0][0]
        logger.info(f"Raw sales records: {raw_count}")
        
        if raw_count == 0:
            raise ValueError("raw_sales table is empty!")
        
        # Check customer_metrics
        customer_count = client.query("SELECT COUNT(*) FROM customer_metrics").result_rows[0][0]
        logger.info(f"Customer metrics records: {customer_count}")
        
        # Check product_performance
        product_count = client.query("SELECT COUNT(*) FROM product_performance").result_rows[0][0]
        logger.info(f"Product performance records: {product_count}")
        
        # Check daily_sales_summary
        daily_count = client.query("SELECT COUNT(*) FROM daily_sales_summary").result_rows[0][0]
        logger.info(f"Daily sales summary records: {daily_count}")
        
        # Validate revenue consistency
        raw_revenue = client.query("SELECT SUM(quantity * price) FROM raw_sales").result_rows[0][0]
        customer_revenue = client.query("SELECT SUM(total_revenue) FROM customer_metrics").result_rows[0][0]
        
        # Handle None values and convert to float
        raw_revenue = float(raw_revenue) if raw_revenue is not None else 0.0
        customer_revenue = float(customer_revenue) if customer_revenue is not None else 0.0
        
        if abs(raw_revenue - customer_revenue) < 0.01:
            logger.info(f"✓ Revenue validation passed: {raw_revenue:.2f}")
        else:
            raise ValueError(f"Revenue mismatch: raw={raw_revenue}, customer={customer_revenue}")
        
        return {
            'raw_count': raw_count,
            'customer_count': customer_count,
            'product_count': product_count,
            'daily_count': daily_count,
            'total_revenue': raw_revenue
        }
    finally:
        client.close()

def generate_clickhouse_summary():
    """Generate summary report of ClickHouse transformations"""
    logger.info("Generating ClickHouse summary report...")
    client = get_clickhouse_client()
    
    try:
        # Top 5 customers by revenue
        top_customers = client.query("""
            SELECT customer_id, total_revenue, total_orders
            FROM customer_metrics
            ORDER BY total_revenue DESC
            LIMIT 5
        """)
        
        logger.info("\n=== Top 5 Customers by Revenue ===")
        for row in top_customers.result_rows:
            logger.info(f"Customer {row[0]}: ${row[1]:.2f} ({row[2]} orders)")
        
        # Top 5 products by revenue
        top_products = client.query("""
            SELECT product_id, total_revenue, total_quantity_sold
            FROM product_performance
            ORDER BY total_revenue DESC
            LIMIT 5
        """)
        
        logger.info("\n=== Top 5 Products by Revenue ===")
        for row in top_products.result_rows:
            logger.info(f"Product {row[0]}: ${row[1]:.2f} ({row[2]} units sold)")
        
        # Recent daily trends
        recent_trends = client.query("""
            SELECT order_date, total_orders, total_revenue
            FROM daily_sales_summary
            ORDER BY order_date DESC
            LIMIT 7
        """)
        
        logger.info("\n=== Last 7 Days Sales ===")
        for row in recent_trends.result_rows:
            logger.info(f"{row[0]}: {row[1]} orders, ${row[2]:.2f} revenue")
        
        return "Summary report generated successfully"
    finally:
        client.close()
