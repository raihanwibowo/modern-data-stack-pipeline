"""
Summary report generation for the data pipeline
"""
import clickhouse_connect
from airflow.exceptions import AirflowException
from .config import ClickHouseConfig


def get_clickhouse_client():
    """Get ClickHouse client from config"""
    config = ClickHouseConfig.from_env()
    return clickhouse_connect.get_client(
        host=config.host,
        port=config.http_port,
        username=config.user,
        password=config.password,
        database=config.database
    )


def generate_summary_report(**context):
    """
    Generate a summary report of the pipeline execution
    
    Args:
        context: Airflow context (optional, for accessing XCom)
    """
    try:
        client = get_clickhouse_client()
        
        print("\n" + "="*80)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*80)
        
        # Raw data stats
        raw_count = client.query("SELECT count() FROM raw_sales").result_rows[0][0]
        print(f"\n📊 Raw Data:")
        print(f"   Total records: {raw_count:,}")
        
        # Check which tables exist
        tables = client.query("SHOW TABLES").result_rows
        table_names = [row[0] for row in tables]
        
        print(f"\n📈 Transformed Data:")
        
        # Customer metrics
        if 'customer_metrics' in table_names:
            customers = client.query("SELECT count() FROM customer_metrics").result_rows[0][0]
            print(f"   Customer metrics: {customers:,} customers")
        
        # Product analysis
        if 'product_analysis' in table_names:
            products = client.query("SELECT count() FROM product_analysis").result_rows[0][0]
            print(f"   Product analysis: {products:,} products")
            
            # Product tier breakdown
            tier_breakdown = client.query("""
                SELECT product_tier, count(*) as count
                FROM product_analysis
                GROUP BY product_tier
                ORDER BY product_tier
            """).result_rows
            
            if tier_breakdown:
                print(f"   Product tiers:")
                for tier, count in tier_breakdown:
                    print(f"     - {tier}: {count:,} products")
        
        # Daily sales
        if 'daily_sales_summary' in table_names:
            daily_count = client.query("SELECT count() FROM daily_sales_summary").result_rows[0][0]
            print(f"   Daily sales summary: {daily_count:,} days")
        
        # Monthly sales
        if 'monthly_sales_summary' in table_names:
            monthly_count = client.query("SELECT count() FROM monthly_sales_summary").result_rows[0][0]
            print(f"   Monthly sales summary: {monthly_count:,} months")
        
        # Customer-Product affinity
        if 'customer_product_affinity' in table_names:
            affinity_count = client.query("SELECT count() FROM customer_product_affinity").result_rows[0][0]
            print(f"   Customer-Product affinity: {affinity_count:,} combinations")
        
        # Top insights
        if 'customer_metrics' in table_names:
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
        
        # Top product
        if 'product_analysis' in table_names:
            top_product = client.query("""
                SELECT product_id, product_tier, total_revenue 
                FROM product_analysis 
                ORDER BY total_revenue DESC 
                LIMIT 1
            """).result_rows
            
            if top_product:
                print(f"\n🥇 Top Product:")
                print(f"   Product ID: {top_product[0][0]}")
                print(f"   Tier: {top_product[0][1]}")
                print(f"   Total Revenue: ${top_product[0][2]:,.2f}")
        
        print("\n" + "="*80)
        print("✓ Pipeline completed successfully!")
        print("="*80 + "\n")
        
        return True
        
    except Exception as e:
        print(f"Warning: Could not generate summary: {str(e)}")
        # Don't fail the pipeline for summary issues
        return True
