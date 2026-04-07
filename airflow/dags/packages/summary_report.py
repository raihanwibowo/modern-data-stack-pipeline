import requests
import time
import os
import clickhouse_connect

client = clickhouse_connect.get_client(
            host=os.getenv('CLICKHOUSE_HOST', 'host.docker.internal'),
            port=int(os.getenv('CLICKHOUSE_HTTP_PORT', 8123)),
            username=os.getenv('CLICKHOUSE_USER', 'default'),
            password=os.getenv('CLICKHOUSE_PASSWORD', ''),
            database=os.getenv('CLICKHOUSE_DATABASE', 'default')
        )

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

