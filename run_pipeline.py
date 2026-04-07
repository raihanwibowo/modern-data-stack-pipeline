import subprocess
import sys
import pandas as pd
from sqlalchemy import create_engine
import clickhouse_connect
import logging
from pathlib import Path
from dotenv import load_dotenv
import os

# Load environment variables from dbt/.env
env_path = Path(__file__).parent / 'dbt' / '.env'
load_dotenv(env_path)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL connection settings (from environment variables)
PG_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'analytics'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

# ClickHouse connection settings
CH_CONFIG = {
    'host': 'localhost',
    'port': 8123,
    'database': 'analytics',
    'user': 'default',
    'password': '1234'
}

def create_pg_engine():
    """Create SQLAlchemy engine for PostgreSQL"""
    conn_string = f"postgresql://{PG_CONFIG['user']}:{PG_CONFIG['password']}@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"
    return create_engine(conn_string)

def get_clickhouse_client():
    """Create ClickHouse client"""
    return clickhouse_connect.get_client(
        host=CH_CONFIG['host'],
        port=CH_CONFIG['port'],
        username=CH_CONFIG['user'],
        password=CH_CONFIG['password']
    )

def setup_clickhouse(client):
    """Setup ClickHouse database and tables"""
    logger.info("Setting up ClickHouse...")
    
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

def load_table_to_clickhouse(client, table_name):
    """Load a table from PostgreSQL to ClickHouse"""
    logger.info(f"Loading {table_name} to ClickHouse...")
    
    # Extract from PostgreSQL
    pg_engine = create_pg_engine()
    df = pd.read_sql(f"SELECT * FROM {table_name}", pg_engine)
    
    # Convert date columns
    date_columns = df.select_dtypes(include=['datetime64']).columns
    for col in date_columns:
        df[col] = pd.to_datetime(df[col]).dt.date
    
    # Load to ClickHouse
    client.insert_df(f"{CH_CONFIG['database']}.{table_name}", df)
    logger.info(f"Loaded {len(df)} rows to ClickHouse table: {table_name}")

def run_dbt_command(command):
    """Execute dbt command"""
    logger.info(f"Running: dbt {command}")
    result = subprocess.run(
        f"dbt {command} --profiles-dir . --project-dir .",
        shell=True,
        capture_output=True,
        text=True,
        cwd="dbt"  # Run from dbt directory
    )
    
    if result.returncode != 0:
        logger.error(f"dbt command failed: {result.stderr}")
        sys.exit(1)
    
    logger.info(f"Success: {result.stdout}")
    return result

def main():
    """Main ETL pipeline"""
    ch_client = None
    try:
        # Step 1: Load seed data
        logger.info("Step 1: Loading seed data...")
        run_dbt_command("seed")
        
        # Step 2: Run dbt transformations
        logger.info("Step 2: Running dbt transformations...")
        run_dbt_command("run")
        
        # Step 3: Run tests
        logger.info("Step 3: Running dbt tests...")
        run_dbt_command("test")
        
        # Step 4: Setup ClickHouse
        logger.info("Step 4: Setting up ClickHouse...")
        ch_client = get_clickhouse_client()
        # setup_clickhouse(ch_client)
        
        # Step 5: Load data to ClickHouse
        logger.info("Step 5: Loading data to ClickHouse...")
        tables = ['customer_segments', 'customer_metrics', 'product_analysis']
        # for table in tables:
        #     load_table_to_clickhouse(ch_client, table)
        
        logger.info("\n✓ Pipeline completed successfully!")
        logger.info(f"PostgreSQL database: {PG_CONFIG['database']}")
        logger.info(f"ClickHouse database: {CH_CONFIG['database']}")
        logger.info(f"Tables: {', '.join(tables)}")
        
        # Optional: Query and display results from ClickHouse
        logger.info("\nSample data from ClickHouse customer_segments:")
        result = ch_client.query(f"SELECT * FROM {CH_CONFIG['database']}.customer_segments LIMIT 5")
        for row in result.result_rows:
            print(row)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)
    finally:
        if ch_client:
            ch_client.close()

if __name__ == "__main__":
    main()
