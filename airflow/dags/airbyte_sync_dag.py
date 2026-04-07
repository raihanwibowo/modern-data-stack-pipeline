"""
Airbyte Sync DAG
Triggers Airbyte connection syncs and monitors their status
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowException
import requests
import time
import os

# Airbyte configuration
AIRBYTE_URL = os.getenv('AIRBYTE_URL', 'http://host.docker.internal:8000')
AIRBYTE_API_URL = f"{AIRBYTE_URL}/api/v1"
AIRBYTE_USERNAME = os.getenv('AIRBYTE_USERNAME', 'raihan.wibowo@gmail.com')
AIRBYTE_PASSWORD = os.getenv('AIRBYTE_PASSWORD', '0PVjlGsQkdfmmlxSvnsu93HbZ7XpYZqO')

# Connection IDs - replace with your actual Airbyte connection IDs
AIRBYTE_CONNECTIONS = {
    'postgres_to_clickhouse': os.getenv('AIRBYTE_CONNECTION_ID_1', 'b706dc46-cc44-4be4-b24f-92ac889db648'),
    'api_to_postgres': os.getenv('AIRBYTE_CONNECTION_ID_2', 'your-connection-id-2'),
}


def get_auth():
    """Get authentication credentials"""
    if AIRBYTE_USERNAME and AIRBYTE_PASSWORD:
        return (AIRBYTE_USERNAME, AIRBYTE_PASSWORD)
    return None

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def trigger_airbyte_sync(connection_id: str, connection_name: str):
    """
    Trigger an Airbyte connection sync
    
    Args:
        connection_id: Airbyte connection ID
        connection_name: Human-readable connection name
    
    Returns:
        job_id: The Airbyte job ID for status checking
    """
    print(f"Triggering Airbyte sync for connection: {connection_name}")
    print(f"Connection ID: {connection_id}")
    
    url = f"{AIRBYTE_API_URL}/connections/sync"
    payload = {
        "connectionId": connection_id
    }
    auth = get_auth()
    
    try:
        response = requests.post(url, json=payload, auth=auth, timeout=30)
        response.raise_for_status()
        
        job_data = response.json()
        job_id = job_data.get('job', {}).get('id')
        
        if not job_id:
            raise AirflowException(f"No job ID returned from Airbyte for {connection_name}")
        
        print(f"Successfully triggered sync. Job ID: {job_id}")
        return job_id
        
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Failed to trigger Airbyte sync for {connection_name}: {str(e)}")


def check_airbyte_job_status(job_id: str, connection_name: str):
    """
    Check the status of an Airbyte job
    
    Args:
        job_id: Airbyte job ID
        connection_name: Human-readable connection name
    
    Returns:
        bool: True if job is complete, False if still running
    """
    print(f"Checking status for job: {job_id} ({connection_name})")
    
    url = f"{AIRBYTE_API_URL}/jobs/get"
    payload = {
        "id": job_id
    }
    auth = get_auth()
    
    try:
        response = requests.post(url, json=payload, auth=auth, timeout=30)
        response.raise_for_status()
        
        job_data = response.json()
        status = job_data.get('job', {}).get('status')
        
        print(f"Current status: {status}")
        
        if status == 'succeeded':
            print(f"Job {job_id} completed successfully!")
            return True
        elif status == 'failed':
            raise AirflowException(f"Airbyte job {job_id} failed for {connection_name}")
        elif status == 'cancelled':
            raise AirflowException(f"Airbyte job {job_id} was cancelled for {connection_name}")
        elif status in ['pending', 'running', 'incomplete']:
            print(f"Job still in progress: {status}")
            return False
        else:
            raise AirflowException(f"Unknown job status: {status}")
            
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Failed to check Airbyte job status: {str(e)}")


def get_airbyte_connection_info(connection_id: str):
    """
    Get information about an Airbyte connection
    
    Args:
        connection_id: Airbyte connection ID
    
    Returns:
        dict: Connection information
    """
    url = f"{AIRBYTE_API_URL}/connections/get"
    payload = {
        "connectionId": connection_id
    }
    auth = get_auth()
    
    try:
        response = requests.post(url, json=payload, auth=auth, timeout=30)
        response.raise_for_status()
        
        connection_data = response.json()
        print(f"Connection Name: {connection_data.get('name')}")
        print(f"Status: {connection_data.get('status')}")
        print(f"Source ID: {connection_data.get('sourceId')}")
        print(f"Destination ID: {connection_data.get('destinationId')}")
        
        return connection_data
        
    except requests.exceptions.RequestException as e:
        print(f"Warning: Could not fetch connection info: {str(e)}")
        return {}


def check_airbyte_health():
    """Check if Airbyte API is accessible"""
    try:
        response = requests.get(f"{AIRBYTE_URL}/api/v1/health", timeout=10)
        response.raise_for_status()
        print("Airbyte API is healthy")
        return True
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Airbyte API is not accessible: {str(e)}")


# Create DAG
with DAG(
    'airbyte_sync_pipeline',
    default_args=default_args,
    description='Trigger and monitor Airbyte connection syncs',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    catchup=False,
    tags=['airbyte', 'sync', 'etl'],
) as dag:
    
    # Check Airbyte health
    health_check = PythonOperator(
        task_id='check_airbyte_health',
        python_callable=check_airbyte_health,
    )
    
    # Create tasks for each connection
    for conn_name, conn_id in AIRBYTE_CONNECTIONS.items():
        
        # Get connection info
        info_task = PythonOperator(
            task_id=f'get_info_{conn_name}',
            python_callable=get_airbyte_connection_info,
            op_kwargs={'connection_id': conn_id},
        )
        
        # Trigger sync
        trigger_task = PythonOperator(
            task_id=f'trigger_sync_{conn_name}',
            python_callable=trigger_airbyte_sync,
            op_kwargs={
                'connection_id': conn_id,
                'connection_name': conn_name,
            },
        )
        
        # Monitor sync status
        monitor_task = PythonSensor(
            task_id=f'monitor_sync_{conn_name}',
            python_callable=lambda job_id, name: check_airbyte_job_status(job_id, name),
            op_kwargs={
                'job_id': "{{ task_instance.xcom_pull(task_ids='trigger_sync_" + conn_name + "') }}",
                'connection_name': conn_name,
            },
            poke_interval=30,  # Check every 30 seconds
            timeout=3600,  # Timeout after 1 hour
            mode='poke',
        )
        
        # Set dependencies
        health_check >> info_task >> trigger_task >> monitor_task


# Alternative DAG with manual connection ID specification
with DAG(
    'airbyte_single_sync',
    default_args=default_args,
    description='Trigger a single Airbyte connection sync (manual)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['airbyte', 'sync', 'manual'],
) as single_sync_dag:
    
    def trigger_and_wait(connection_id: str):
        """Trigger sync and wait for completion"""
        # Trigger
        job_id = trigger_airbyte_sync(connection_id, "Manual Sync")
        
        # Wait for completion
        max_wait = 3600  # 1 hour
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            if check_airbyte_job_status(job_id, "Manual Sync"):
                return "Sync completed successfully"
            time.sleep(30)
        
        raise AirflowException("Sync timed out after 1 hour")
    
    check_health = PythonOperator(
        task_id='check_airbyte_health',
        python_callable=check_airbyte_health,
    )
    
    run_sync = PythonOperator(
        task_id='run_airbyte_sync',
        python_callable=trigger_and_wait,
        op_kwargs={
            'connection_id': '{{ dag_run.conf.get("connection_id", "b706dc46-cc44-4be4-b24f-92ac889db648") }}'
        },
    )
    
    check_health >> run_sync
