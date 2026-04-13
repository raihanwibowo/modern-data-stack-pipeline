"""
Airbyte integration module for triggering and monitoring syncs
"""
import requests
from airflow.exceptions import AirflowException
from .config import AirbyteConfig


def get_airbyte_config():
    """Get Airbyte configuration from environment"""
    return AirbyteConfig.from_env()


def get_auth():
    """Get Airbyte authentication credentials"""
    config = get_airbyte_config()
    if config.username and config.password:
        return (config.username, config.password)
    return None


def check_airbyte_health():
    """Check if Airbyte API is accessible"""
    config = get_airbyte_config()
    
    try:
        response = requests.get(f"{config.url}/api/v1/health", timeout=10)
        response.raise_for_status()
        print("✓ Airbyte API is healthy")
        return True
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Airbyte API is not accessible: {str(e)}")


def trigger_airbyte_sync(connection_id: str = None) -> str:
    """
    Trigger Airbyte sync from Postgres to ClickHouse
    
    Returns:
        str: The Airbyte job ID for status checking
    
    Raises:
        AirflowException: If sync trigger fails
    """
    config = get_airbyte_config()
    
    if not connection_id:
        raise AirflowException(
            "AIRBYTE_POSTGRES_TO_CLICKHOUSE_CONNECTION_ID not set. "
            "Please configure it in airflow/.env"
        )
    
    print(f"Triggering Airbyte sync: Postgres → ClickHouse")
    print(f"Connection ID: {connection_id}")
    
    url = f"{config.url}/api/v1/connections/sync"
    payload = {"connectionId": connection_id}
    auth = get_auth()
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        
        job_data = response.json()
        job_id = job_data.get('job', {}).get('id')
        
        if not job_id:
            raise AirflowException("No job ID returned from Airbyte")
        
        print(f"✓ Successfully triggered sync. Job ID: {job_id}")
        return job_id
        
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Failed to trigger Airbyte sync: {str(e)}")


def check_airbyte_job_status(job_id: str) -> bool:
    """
    Check the status of an Airbyte job
    
    Args:
        job_id: Airbyte job ID
    
    Returns:
        bool: True if job is complete, False if still running
    
    Raises:
        AirflowException: If job fails or is cancelled
    """
    if not job_id or job_id == 'None':
        raise AirflowException("Invalid job ID")
    
    config = get_airbyte_config()
    print(f"Checking Airbyte job status: {job_id}")
    
    url = f"{config.url}/api/v1/jobs/get"
    payload = {"id": job_id}
    auth = get_auth()
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        
        job_data = response.json()
        status = job_data.get('job', {}).get('status')
        
        print(f"Current status: {status}")
        
        if status == 'succeeded':
            print(f"✓ Airbyte sync completed successfully!")
            
            # Print sync stats if available
            job_info = job_data.get('job', {})
            attempts = job_info.get('attempts', [])
            if attempts:
                last_attempt = attempts[-1]
                records_synced = last_attempt.get('recordsSynced', 0)
                bytes_synced = last_attempt.get('bytesSynced', 0)
                print(f"  Records synced: {records_synced:,}")
                print(f"  Bytes synced: {bytes_synced:,}")
            
            return True
            
        elif status == 'failed':
            raise AirflowException(f"Airbyte job {job_id} failed")
        elif status == 'cancelled':
            raise AirflowException(f"Airbyte job {job_id} was cancelled")
        elif status in ['pending', 'running', 'incomplete']:
            print(f"Job still in progress: {status}")
            return False
        else:
            raise AirflowException(f"Unknown job status: {status}")
            
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Failed to check Airbyte job status: {str(e)}")