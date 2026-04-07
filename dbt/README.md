# dbt Project Configuration

## Environment Variables

This project uses environment variables for database configuration. This approach:
- Keeps credentials out of version control
- Makes it easy to switch between environments (dev/staging/prod)
- Works seamlessly with both local runs and Airflow

## Setup

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Update `.env` with your actual credentials:
```bash
# For local development
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=analytics
POSTGRES_SCHEMA=public
```

## Usage

### Local Development

The environment variables are automatically loaded from `dbt/.env`:

```bash
# From project root
python run_pipeline.py

# Or run dbt directly
cd dbt
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### Airflow

Environment variables are defined in `airflow/docker-compose.yml` and automatically passed to dbt commands:

```yaml
environment:
  POSTGRES_HOST: host.docker.internal
  POSTGRES_PORT: 5432
  POSTGRES_USER: postgres
  POSTGRES_PASSWORD: postgres
  POSTGRES_DB: analytics
  POSTGRES_SCHEMA: public
```

Note: Airflow uses `host.docker.internal` to access the host machine's PostgreSQL from within the container.

## profiles.yml

The `profiles.yml` file references environment variables using dbt's `env_var()` function:

```yaml
analytics:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST') }}"
      port: "{{ env_var('POSTGRES_PORT', '5432') | int }}"
      user: "{{ env_var('POSTGRES_USER') }}"
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      dbname: "{{ env_var('POSTGRES_DB') }}"
      schema: "{{ env_var('POSTGRES_SCHEMA', 'public') }}"
      threads: 4
```

## Security Best Practices

1. Never commit `.env` files to version control
2. Use different credentials for each environment
3. Rotate passwords regularly
4. Use read-only credentials where possible
5. Consider using secret management tools (AWS Secrets Manager, HashiCorp Vault, etc.) for production

## Troubleshooting

### Environment variables not loading

Make sure:
- The `.env` file exists in the `dbt/` directory
- You've installed `python-dotenv`: `pip install python-dotenv`
- Environment variables are exported in your shell (for manual dbt commands)

### Connection errors in Airflow

- Verify `host.docker.internal` resolves correctly
- Check that PostgreSQL is accessible from Docker containers
- Review Airflow logs: `docker-compose logs airflow-scheduler`
