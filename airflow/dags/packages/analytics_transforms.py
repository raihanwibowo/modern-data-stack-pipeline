import pandas as pd
import logging
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


def python_customer_analytics(**context):
    """
    Python transformation with pandas - Advanced customer analytics
    
    Performs:
    - Customer lifetime calculation
    - Order frequency analysis
    - RFM (Recency, Frequency, Monetary) scoring
    """
    logging.info("Running Python customer analytics...")
    
    # Get data from PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_analytics')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Read from dbt-created table
    df = pd.read_sql("SELECT * FROM customer_metrics", engine)
    
    # Advanced Python transformations
    df['customer_lifetime_days'] = (
        pd.to_datetime(df['last_order_date']) - pd.to_datetime(df['first_order_date'])
    ).dt.days
    
    df['order_frequency'] = df['total_orders'] / (df['customer_lifetime_days'] + 1)
    
    # RFM scoring
    df['recency_score'] = pd.qcut(
        (datetime.now() - pd.to_datetime(df['last_order_date'])).dt.days,
        q=5,
        labels=[5, 4, 3, 2, 1],
        duplicates='drop'
    )
    
    df['frequency_score'] = pd.qcut(
        df['total_orders'].rank(method='first'),
        q=5,
        labels=[1, 2, 3, 4, 5],
        duplicates='drop'
    )
    
    df['monetary_score'] = pd.qcut(
        df['total_revenue'].rank(method='first'),
        q=5,
        labels=[1, 2, 3, 4, 5],
        duplicates='drop'
    )
    
    df['rfm_score'] = (
        df['recency_score'].astype(int) +
        df['frequency_score'].astype(int) +
        df['monetary_score'].astype(int)
    )
    
    # Load back to PostgreSQL
    df.to_sql(
        'customer_rfm_analysis',
        engine,
        if_exists='replace',
        index=False
    )
    
    logging.info(f"Loaded {len(df)} rows to customer_rfm_analysis")
    return len(df)


def python_product_cohorts(**context):
    """
    Python transformation for cohort analysis
    
    Creates monthly product cohorts with:
    - Order counts per cohort
    - Revenue per cohort
    - Units sold per cohort
    """
    logging.info("Running product cohort analysis...")
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_analytics')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Read staging data
    df = pd.read_sql("SELECT * FROM stg_sales", engine)
    df['order_date'] = pd.to_datetime(df['order_date'])
    
    # Create monthly cohorts
    df['order_month'] = df['order_date'].dt.to_period('M')
    
    cohort_data = df.groupby(['product_id', 'order_month']).agg({
        'order_id': 'count',
        'total_amount': 'sum',
        'quantity': 'sum'
    }).reset_index()
    
    cohort_data.columns = [
        'product_id',
        'cohort_month',
        'orders',
        'revenue',
        'units_sold'
    ]
    
    cohort_data['cohort_month'] = cohort_data['cohort_month'].astype(str)
    
    # Load to PostgreSQL
    cohort_data.to_sql(
        'product_cohorts',
        engine,
        if_exists='replace',
        index=False
    )
    
    logging.info(f"Loaded {len(cohort_data)} cohort records")
    return len(cohort_data)


def data_quality_checks(**context):
    """
    Validate data quality across all tables
    
    Checks:
    - Row counts for each table
    - Ensures no empty tables
    - Pushes results to XCom for downstream tasks
    """
    logging.info("Running data quality checks...")
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_analytics')
    engine = pg_hook.get_sqlalchemy_engine()
    
    checks = {
        'customer_segments': 'SELECT COUNT(*) FROM customer_segments',
        'product_analysis': 'SELECT COUNT(*) FROM product_analysis',
        'customer_rfm_analysis': 'SELECT COUNT(*) FROM customer_rfm_analysis',
        'product_cohorts': 'SELECT COUNT(*) FROM product_cohorts',
    }
    
    results = {}
    for table, query in checks.items():
        count = pd.read_sql(query, engine).iloc[0, 0]
        results[table] = count
        logging.info(f"{table}: {count} rows")
        
        if count == 0:
            raise ValueError(f"Data quality check failed: {table} has 0 rows")
    
    # Push to XCom for downstream tasks
    context['task_instance'].xcom_push(key='row_counts', value=results)
    return results


def generate_summary(**context):
    """
    Generate pipeline summary report
    
    Pulls row counts from data quality checks and logs summary
    """
    ti = context['task_instance']
    row_counts = ti.xcom_pull(task_ids='data_quality_checks', key='row_counts')
    
    logging.info("=" * 50)
    logging.info("PIPELINE SUMMARY")
    logging.info("=" * 50)
    for table, count in row_counts.items():
        logging.info(f"  {table}: {count} rows")
    logging.info("=" * 50)
    logging.info("Pipeline completed successfully!")
    
    return row_counts
