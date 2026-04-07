# Modern Data Stack Pipeline

A complete ELT (Extract, Load, Transform) analytics pipeline using modern data tools: Airbyte, ClickHouse, dbt, Airflow, and Apache Superset.

## 🏗️ Architecture

```
PostgreSQL → Airbyte → ClickHouse → Transformations → Analytics Tables → Superset
                          ↓
                       Airflow (Orchestration)
```

## 🛠️ Tech Stack

- **Source Database**: PostgreSQL
- **Data Integration**: Airbyte (EL - Extract & Load)
- **Data Warehouse**: ClickHouse (OLAP)
- **Transformation**: Python + SQL (dbt-style transformations)
- **Orchestration**: Apache Airflow
- **Visualization**: Apache Superset
- **Containerization**: Docker & Docker Compose

## 📊 Features

- **Automated ELT Pipeline**: Airbyte syncs data from Postgres to ClickHouse
- **Smart Transformations**: Creates staging views and analytics tables
- **Product Tiering**: Automatic Bronze/Silver/Gold classification
- **Customer Analytics**: RFM analysis, lifetime value, segmentation
- **Time-Series Analysis**: Daily and monthly sales summaries
- **Orchestration**: Scheduled DAGs with monitoring and retry logic
- **Data Quality**: Built-in validation and error handling
- **Visualization**: Pre-configured Superset dashboards

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- 5 CPU cores, 6GB RAM minimum
- Git

### 1. Clone Repository

```bash
git clone <your-repo-url>
cd modern-data-stack-pipeline
```

### 2. Start Services

```bash
# Start ClickHouse
docker-compose up -d

# Start Airflow
cd airflow
docker-compose up -d

# Start Superset (optional)
cd ../superset
docker-compose -f superset-compose.yml up -d
```

### 3. Configure Airbyte

1. Open Airbyte UI: http://localhost:8000
2. Create Source: PostgreSQL connection
3. Create Destination: ClickHouse connection
4. Create Connection: Postgres → ClickHouse
5. Copy the Connection ID from the URL

### 4. Update Airflow Configuration

Edit `airflow/.env`:

```bash
# Airbyte credentials
AIRBYTE_USERNAME=your-username
AIRBYTE_PASSWORD=your-password

# Connection ID from Airbyte
AIRBYTE_POSTGRES_TO_CLICKHOUSE_CONNECTION_ID=your-connection-id

# ClickHouse credentials
CLICKHOUSE_HOST=host.docker.internal
CLICKHOUSE_PASSWORD=your-password
```

### 5. Restart Airflow

```bash
cd airflow
docker-compose restart
```

### 6. Run the Pipeline

1. Open Airflow UI: http://localhost:8080
2. Enable the `postgres_airbyte_dbt_clickhouse_pipeline` DAG
3. Trigger manually or wait for scheduled run (daily at 2 AM)

## 📁 Project Structure

```
.
├── airflow/
│   ├── dags/
│   │   ├── postgres_airbyte_dbt_clickhouse_pipeline.py  # Main ELT DAG
│   │   ├── airbyte_sync_dag.py                          # Airbyte sync utilities
│   │   └── packages/                                     # Helper modules
│   ├── docker-compose.yml
│   ├── Dockerfile
│   └── requirements.txt
├── dbt/
│   ├── models/                                           # dbt models (reference)
│   ├── seeds/                                            # Sample data
│   └── profiles.yml
├── superset/
│   ├── superset_config.py                                # Optimized config
│   └── superset-compose.yml
├── clickhouse-init.sql                                   # ClickHouse schema
├── docker-compose.yml                                    # ClickHouse service
├── get_airbyte_connections.py                            # Helper script
├── AIRBYTE_SETUP.md                                      # Detailed Airbyte guide
├── CLICKHOUSE_SETUP.md                                   # ClickHouse guide
└── README.md
```

## 🔄 Pipeline Flow

### 1. Extract & Load (Airbyte)
- Connects to PostgreSQL source
- Extracts `raw_sales` table
- Loads to ClickHouse `analytics` database

### 2. Transform (Python in Airflow)

**Staging Layer:**
- `stg_sales` - Cleaned and standardized sales data

**Analytics Tables:**
- `customer_metrics` - Customer behavior, lifetime value, purchase patterns
- `product_analysis` - Product performance with Bronze/Silver/Gold tiers
- `daily_sales_summary` - Daily aggregated metrics
- `monthly_sales_summary` - Monthly trends and patterns
- `customer_product_affinity` - Customer-product purchase relationships

### 3. Visualize (Superset)
- Connect to ClickHouse
- Create charts and dashboards
- Share insights with stakeholders

## 📊 Analytics Tables

### Customer Metrics
```sql
SELECT 
    customer_id,
    total_orders,
    total_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,
    unique_products,
    total_items_purchased
FROM customer_metrics
ORDER BY total_revenue DESC;
```

### Product Analysis with Tiers
```sql
SELECT 
    product_id,
    product_tier,  -- Bronze, Silver, Gold
    total_revenue,
    total_quantity_sold,
    unique_customers
FROM product_analysis
WHERE product_tier = 'Gold'
ORDER BY total_revenue DESC;
```

### Monthly Sales Trends
```sql
SELECT 
    sale_month,
    total_orders,
    total_revenue,
    unique_customers,
    unique_products
FROM monthly_sales_summary
ORDER BY sale_month DESC;
```

## 🔧 Configuration

### Airflow DAG Settings

Edit `airflow/dags/postgres_airbyte_dbt_clickhouse_pipeline.py`:

```python
# Schedule (default: daily at 2 AM)
schedule_interval='0 2 * * *'

# Retry settings
'retries': 2,
'retry_delay': timedelta(minutes=5),

# Timeout for Airbyte sync
timeout=3600  # 1 hour
```

### Superset Configuration

The `superset_config.py` is optimized for low-resource environments:
- File-based caching (no Redis required)
- Reduced workers and connection pools
- Minimal feature set for 5 CPU / 6GB RAM

## 🐛 Troubleshooting

### Airbyte Connection Issues

```bash
# Check Airbyte health
curl http://localhost:8000/api/v1/health

# Get connection IDs
python get_airbyte_connections.py
```

See [AIRBYTE_SETUP.md](AIRBYTE_SETUP.md) for detailed troubleshooting.

### ClickHouse Connection Issues

```bash
# Test ClickHouse connection
docker exec -it clickhouse-server clickhouse-client

# Check tables
SHOW TABLES FROM analytics;

# Verify data
SELECT count() FROM analytics.raw_sales;
```

See [CLICKHOUSE_SETUP.md](CLICKHOUSE_SETUP.md) for more details.

### Airflow DAG Failures

1. Check Airflow logs: http://localhost:8080
2. Verify environment variables in `airflow/.env`
3. Ensure all services are running:
   ```bash
   docker ps
   ```

## 📈 Monitoring

### Airflow UI
- DAG runs: http://localhost:8080
- Task logs and retry history
- XCom data between tasks

### Pipeline Summary
Each DAG run generates a summary report:
```
================================================================================
PIPELINE EXECUTION SUMMARY
================================================================================

📊 Raw Data: 10,000 records

📈 Transformed Data:
   Customer metrics: 500 customers
   Product analysis: 100 products
   Daily sales summary: 365 days
   Monthly sales summary: 12 months
   Customer-Product affinity: 2,500 combinations

🏆 Top Customer: CUST_123 ($45,678.90)

================================================================================
✓ Pipeline completed successfully!
================================================================================
```

## 🔐 Security Notes

- Change default passwords in production
- Use secrets management (Airflow Variables/Connections)
- Enable HTTPS for Superset
- Restrict network access to services
- Never commit `.env` files with real credentials

## 📚 Additional Resources

- [Airbyte Documentation](https://docs.airbyte.com/)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Superset Documentation](https://superset.apache.org/docs/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test the pipeline end-to-end
5. Submit a pull request

## 📝 License

MIT License - feel free to use this project for learning and production use.

## 🙏 Acknowledgments

Built with modern data engineering best practices and optimized for resource-constrained environments.

---

**Need Help?** Check the setup guides:
- [AIRBYTE_SETUP.md](AIRBYTE_SETUP.md) - Airbyte configuration
- [CLICKHOUSE_SETUP.md](CLICKHOUSE_SETUP.md) - ClickHouse setup

**Stack:** PostgreSQL • Airbyte • ClickHouse • Airflow • Superset • Docker
