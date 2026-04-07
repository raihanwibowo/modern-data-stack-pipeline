# Airflow + dbt + Python Pipeline

Complete orchestration of dbt SQL transformations and Python analytics using Apache Airflow.

## Architecture

```
Airflow (Orchestration)
  ├── dbt (SQL Transformations)
  │   ├── Seeds (raw data)
  │   ├── Staging models
  │   └── Mart models
  ├── Python (Advanced Analytics)
  │   ├── RFM scoring
  │   ├── Cohort analysis
  │   └── Statistical calculations
  └── PostgreSQL (Data Warehouse)
```

## Setup

```bash
cd airflow

# Create required directories
mkdir -p dags logs plugins

# Set Airflow UID (Linux/Mac)
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Start all services
docker-compose up -d

# Wait for services to be healthy (30-60 seconds)
docker-compose ps
```

## Access

- Airflow UI: http://localhost:8080
  - Username: `airflow`
  - Password: `airflow`
- PostgreSQL Airflow (metadata): `localhost:5433`
- PostgreSQL Analytics: Your existing instance on `localhost:5432`

## Configure Airflow Connection

1. Go to Airflow UI → Admin → Connections
2. Add new connection:
   - Connection Id: `postgres_analytics`
   - Connection Type: `Postgres`
   - Host: `host.docker.internal` (Mac/Windows) or `172.17.0.1` (Linux)
   - Schema: `analytics`
   - Login: `postgres`
   - Password: `postgres`
   - Port: `5432`

## Run the Pipeline

### Via Airflow UI
1. Go to http://localhost:8080
2. Find DAG: `analytics_pipeline`
3. Toggle it ON
4. Click "Trigger DAG" (play button)

### Via CLI
```bash
# Trigger the DAG manually
docker-compose exec airflow-webserver airflow dags trigger analytics_pipeline

# View DAG runs
docker-compose exec airflow-webserver airflow dags list-runs -d analytics_pipeline

# View task logs
docker-compose exec airflow-webserver airflow tasks logs analytics_pipeline dbt_run <execution_date>
```

## Pipeline Tasks

1. `check_postgres_connection` - Verify database connectivity
2. `dbt_seed` - Load raw CSV data
3. `dbt_run` - Execute dbt SQL models
4. `dbt_test` - Run dbt tests
5. `python_customer_analytics` - RFM scoring with pandas
6. `python_product_cohorts` - Cohort analysis
7. `data_quality_checks` - Validate row counts
8. `generate_summary` - Pipeline summary report

## Task Dependencies

```
check_postgres → dbt_seed → dbt_run → dbt_test
                                         ├→ python_customer_analytics
                                         └→ python_product_cohorts
                                                    ↓
                                            quality_checks → summary
```

## Output Tables

### dbt SQL Models
- `stg_sales` - Staging data
- `customer_metrics` - Customer aggregations
- `customer_segments` - Customer segmentation
- `product_analysis` - Product analytics

### Python Models
- `customer_rfm_analysis` - RFM scores and segments
- `product_cohorts` - Monthly product cohorts

## Monitoring

```bash
# View logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver

# Check service health
docker-compose ps

# Access PostgreSQL
docker exec -it postgres_analytics psql -U postgres -d analytics
```

## Troubleshooting

### DAG not appearing
```bash
# Check DAG for errors
docker-compose exec airflow-webserver airflow dags list
docker-compose exec airflow-webserver python /opt/airflow/dags/analytics_pipeline_dag.py
```

### Connection issues
- Ensure `postgres_analytics` connection is configured in Airflow UI
- Use container name `postgres-analytics` as host (not `localhost`)

### dbt errors
```bash
# Test dbt manually
docker-compose exec airflow-webserver bash
cd /opt/airflow/dbt
dbt debug --profiles-dir .
dbt run --profiles-dir .
```

## Stop Services

```bash
# Stop Airflow
cd airflow
docker-compose down

# Stop PostgreSQL
cd ../
docker-compose down

# To remove all data (clean slate)
cd airflow
docker-compose down -v
cd ../
docker-compose down -v
```

## Best Practices

1. **Idempotency**: All tasks can be re-run safely
2. **Incremental loads**: Use dbt incremental models for large datasets
3. **Data quality**: Add dbt tests and Python validation
4. **Monitoring**: Set up alerts for task failures
5. **Scheduling**: Adjust `schedule_interval` based on data freshness needs
6. **Resource management**: Use task pools for parallel execution limits
