# Airbyte Connection ID Setup Guide

## Method 1: Get Connection ID from Airbyte UI (Easiest)

1. Open Airbyte UI in your browser: `http://localhost:8000`
2. Click on **Connections** in the left sidebar
3. Click on the connection you want to use
4. Look at the URL in your browser address bar:
   ```
   http://localhost:8000/workspaces/{workspace-id}/connections/{connection-id}
   ```
5. Copy the `{connection-id}` (it's a UUID like `12345678-1234-1234-1234-123456789abc`)

## Method 2: Use the Python Script (With Authentication)

If your Airbyte requires authentication:

```bash
# Set credentials
export AIRBYTE_USERNAME='airbyte'
export AIRBYTE_PASSWORD='password'

# Run the script
python get_airbyte_connections.py
```

The script will:
- List all your connections
- Show connection IDs
- Generate configuration files

## Method 3: Browser Developer Tools

1. Open Airbyte UI: `http://localhost:8000`
2. Open browser Developer Tools (F12)
3. Go to **Network** tab
4. Click on a connection in Airbyte UI
5. Look for API calls to `/connections/get`
6. Check the request payload or response - it contains the connection ID

## Method 4: Direct API Call (with curl)

```bash
# Without authentication
curl -X POST http://localhost:8000/api/v1/workspaces/list

# With authentication
curl -X POST http://localhost:8000/api/v1/workspaces/list \
  -u airbyte:password

# List connections for a workspace
curl -X POST http://localhost:8000/api/v1/connections/list \
  -u airbyte:password \
  -H "Content-Type: application/json" \
  -d '{"workspaceId": "your-workspace-id"}'
```

## Configure Airflow DAG

Once you have the connection IDs, update the DAG:

### Option A: Update the DAG file directly

Edit `airflow/dags/airbyte_sync_dag.py`:

```python
AIRBYTE_CONNECTIONS = {
    'postgres_to_clickhouse': '12345678-1234-1234-1234-123456789abc',
    'api_to_postgres': '87654321-4321-4321-4321-cba987654321',
}
```

### Option B: Use environment variables

Add to `airflow/.env`:

```bash
# Airbyte Configuration
AIRBYTE_URL=http://localhost:8000
AIRBYTE_USERNAME=airbyte
AIRBYTE_PASSWORD=password

# Connection IDs
AIRBYTE_CONNECTION_ID_1=12345678-1234-1234-1234-123456789abc
AIRBYTE_CONNECTION_ID_2=87654321-4321-4321-4321-cba987654321
```

Then restart Airflow:

```bash
cd airflow
docker-compose restart
```

## Test the DAG

1. Open Airflow UI: `http://localhost:8080`
2. Find the `airbyte_sync_pipeline` DAG
3. Enable it and trigger manually
4. Check the logs to verify it's working

## Troubleshooting

### 401 Unauthorized Error
- Set `AIRBYTE_USERNAME` and `AIRBYTE_PASSWORD` environment variables
- Default credentials are usually `airbyte` / `password`

### Connection Not Found
- Verify the connection ID is correct
- Check that the connection exists in Airbyte UI
- Ensure Airbyte is running

### Timeout Errors
- Increase timeout values in the DAG
- Check network connectivity between Airflow and Airbyte
- Verify Airbyte is accessible from Airflow container

## DAG Features

The created DAG includes:

- **Health Check**: Verifies Airbyte API is accessible
- **Connection Info**: Fetches connection details
- **Trigger Sync**: Starts the Airbyte sync job
- **Monitor Status**: Polls job status every 30 seconds
- **Error Handling**: Fails the task if sync fails
- **Parallel Execution**: Can run multiple connections in parallel

## Schedule

Default schedule: Every 6 hours (`0 */6 * * *`)

To change, edit the `schedule_interval` in the DAG file.
