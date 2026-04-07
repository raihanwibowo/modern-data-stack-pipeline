# Setup Guide: Airflow with Existing PostgreSQL

## Prerequisites

You already have PostgreSQL running with the analytics database. This setup only runs Airflow services.

## Architecture

```
Your Host Machine
├── PostgreSQL (existing)
│   └── analytics database (port 5432)
└── Docker Containers
    ├── Airflow Webserver (port 8080)
    ├── Airflow Scheduler
    └── PostgreSQL for Airflow metadata (port 5433)
```

## Step 1: Start Airflow

```bash
cd dbt/airflow

# Create directories
mkdir -p dags logs plugins

# Set Airflow UID
echo "AIRFLOW_UID=$(id -u)" > .env

# Start Airflow (without analytics PostgreSQL)
docker-compose up -d

# Check status
docker-compose ps
```

## Step 2: Configure PostgreSQL Connection

1. Open Airflow UI: http://localhost:8080
2. Login: `airflow` / `airflow`
3. Go to Admin → Connections
4. Click the "+" button to add a new connection
5. Fill in:

```
Connection Id: postgres_analytics
Connection Type: Postgres
Host: host.docker.internal    # For Mac/Windows
                               # Use 172.17.0.1 for Linux
                               # Or use container name if PostgreSQL is in Docker
Schema: analytics
Login: postgres
Password: postgres
Port: 5432
```

6. Click "Test" to verify connection
7. Click "Save"

## Step 3: Run the Pipeline

1. In Airflow UI, find the `analytics_pipeline` DAG
2. Toggle it ON (switch on the left)
3. Click the "Play" button to trigger it manually

## Troubleshooting

### Connection refused to PostgreSQL

If Airflow can't connect to your PostgreSQL:

**For Mac/Windows:**
```
Host: host.docker.internal
```

**For Linux:**
```
Host: 172.17.0.1
```

**If PostgreSQL is in Docker:**
```bash
# Find the container name
docker ps | grep postgres

# Use the container name as host
Host: <container_name>

# Or add to the same network
docker network connect airflow_default <postgres_container_name>
```

### Check PostgreSQL is accessible

```bash
# From your host
psql -h localhost -U postgres -d analytics -c "SELECT 1"

# From Airflow container
docker-compose exec airflow-webserver bash
apt-get update && apt-get install -y postgresql-client
psql -h host.docker.internal -U postgres -d analytics -c "SELECT 1"
```

### PostgreSQL needs to accept connections

Edit your PostgreSQL config to allow Docker connections:

```bash
# Find postgresql.conf
# Add or modify:
listen_addresses = '*'

# Find pg_hba.conf
# Add:
host    all             all             172.17.0.0/16           md5
```

Then restart PostgreSQL.

## Clean Up

```bash
# Stop Airflow
docker-compose down

# Remove volumes (clean slate)
docker-compose down -v
```
