# Apache Superset with ClickHouse Support

## Quick Start

1. Build and start Superset:
```bash
cd "Docker Compose"
docker-compose -f superset-compose.yml up -d --build
```

2. Wait for initialization (check logs):
```bash
docker-compose -f superset-compose.yml logs -f
```

3. Access Superset at: http://localhost:8088
   - Username: `admin`
   - Password: `admin`

4. Connect to ClickHouse:
   - Go to Data → Databases → + Database
   - Select "ClickHouse Connect" from the supported databases dropdown
   - Use SQLAlchemy URI format: `clickhousedb://username:password@host:port/database`
   - Example: `clickhousedb://default:@clickhouse-server:9000/default`

## Resource Configuration

- CPU: 3 cores (limit), 1 core (reservation)
- RAM: 4GB (limit), 1GB (reservation)

This leaves ~2GB RAM and 2 CPU cores for your OS and other processes.

## Installed Plugins

- `psycopg2-binary`: PostgreSQL connector
- `clickhouse-connect`: Official ClickHouse connector for Superset
- `clickhouse-driver`: ClickHouse native protocol driver
- `clickhouse-sqlalchemy`: SQLAlchemy dialect for ClickHouse

## Database Connection Examples

### PostgreSQL
```
postgresql://username:password@host:port/database
```
Example: `postgresql://postgres:password@host.docker.internal:5432/mydb`

### ClickHouse
```
clickhouse+native://username:password@host:port/database
```
Example: `clickhouse+native://default:1234@host.docker.internal:9000/analytics`

## Important Notes

- Change `SUPERSET_SECRET_KEY` in production
- Default admin credentials should be changed after first login
- Data persists in the `superset_home` Docker volume
- First startup takes longer due to image build

## Troubleshooting

If ClickHouse doesn't appear in the database list:
1. Check if drivers are installed:
```bash
docker exec -it superset pip list | grep clickhouse
```

2. Restart the container:
```bash
docker-compose -f superset-compose.yml restart
```

## Useful Commands

```bash
# View logs
docker-compose -f superset-compose.yml logs -f

# Stop Superset
docker-compose -f superset-compose.yml down

# Stop and remove volumes (fresh start)
docker-compose -f superset-compose.yml down -v

# Rebuild and restart
docker-compose -f superset-compose.yml up -d --build

# Access container shell
docker exec -it superset bash
```
