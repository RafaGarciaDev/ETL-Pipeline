"""
Daily ETL Pipeline DAG
Executes the complete ETL pipeline daily.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Add src to Python path
sys.path.append('/opt/airflow/src')

# Default arguments
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email': ['data-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'etl_daily_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for data warehouse',
    schedule_interval='0 2 * * *',  # 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'daily', 'production'],
)


def check_source_data_freshness(**context):
    """Check if source data is fresh enough to process."""
    from src.extract.csv_extractor import CSVExtractor
    import pandas as pd
    from datetime import datetime, timedelta
    
    print("Checking data freshness...")
    
    # Example: Check if we have data from last 24 hours
    # Implement your actual freshness logic here
    
    return True


def extract_csv_data(**context):
    """Extract data from CSV files."""
    from src.extract.csv_extractor import CSVExtractor
    import pandas as pd
    
    print("Extracting CSV data...")
    
    extractor = CSVExtractor()
    
    # Extract from configured files
    csv_files = [
        '/opt/airflow/data/raw/sales.csv',
        '/opt/airflow/data/raw/customers.csv'
    ]
    
    all_data = []
    for file in csv_files:
        if os.path.exists(file):
            df = extractor.extract(file)
            df['source_file'] = os.path.basename(file)
            all_data.append(df)
    
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        output_path = f"/opt/airflow/data/raw/extracted_{datetime.now().strftime('%Y%m%d')}.csv"
        combined.to_csv(output_path, index=False)
        
        # Push file path to XCom
        context['task_instance'].xcom_push(key='extracted_file', value=output_path)
        print(f"Extracted {len(combined)} rows")
        return output_path
    else:
        raise ValueError("No data extracted")


def extract_api_data(**context):
    """Extract data from API."""
    from src.extract.api_extractor import APIExtractor
    import pandas as pd
    import os
    
    print("Extracting API data...")
    
    # Get API key from environment
    api_key = os.getenv('WEATHER_API_KEY', 'demo_key')
    
    extractor = APIExtractor(
        base_url="https://api.openweathermap.org/data/2.5",
        api_key=api_key
    )
    
    # Extract weather data for multiple cities
    cities = ['London', 'New York', 'Tokyo']
    all_data = []
    
    for city in cities:
        try:
            data = extractor.extract('weather', params={'q': city, 'units': 'metric'})
            all_data.extend(data)
        except Exception as e:
            print(f"Failed to extract data for {city}: {e}")
    
    if all_data:
        df = pd.DataFrame(all_data)
        output_path = f"/opt/airflow/data/raw/api_data_{datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(output_path, index=False)
        
        context['task_instance'].xcom_push(key='api_file', value=output_path)
        print(f"Extracted {len(df)} API records")
        return output_path
    else:
        print("No API data extracted")
        return None


def transform_data(**context):
    """Transform extracted data."""
    from src.transform.transformer import DataTransformer
    import pandas as pd
    
    print("Transforming data...")
    
    # Get extracted file from XCom
    ti = context['task_instance']
    extracted_file = ti.xcom_pull(task_ids='extract_csv', key='extracted_file')
    
    if not extracted_file or not os.path.exists(extracted_file):
        raise ValueError("No extracted data found")
    
    # Load data
    df = pd.read_csv(extracted_file)
    print(f"Loaded {len(df)} rows for transformation")
    
    # Transform
    transformer = DataTransformer()
    
    # Clean
    df = transformer.clean(df, remove_duplicates=True, handle_nulls='fill')
    
    # Standardize
    df = transformer.standardize_columns(df)
    
    # Add metadata
    df['etl_loaded_at'] = datetime.now()
    df['etl_batch_id'] = context['run_id']
    
    # Save transformed data
    output_path = f"/opt/airflow/data/processed/transformed_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(output_path, index=False)
    
    ti.xcom_push(key='transformed_file', value=output_path)
    print(f"Transformed {len(df)} rows")
    return output_path


def load_to_warehouse(**context):
    """Load data to warehouse."""
    from src.load.db_loader import DatabaseLoader
    import pandas as pd
    import os
    
    print("Loading data to warehouse...")
    
    # Get transformed file from XCom
    ti = context['task_instance']
    transformed_file = ti.xcom_pull(task_ids='transform', key='transformed_file')
    
    if not transformed_file or not os.path.exists(transformed_file):
        raise ValueError("No transformed data found")
    
    # Load data
    df = pd.read_csv(transformed_file)
    print(f"Loaded {len(df)} rows for loading")
    
    # Database connection
    conn_string = os.getenv(
        'DB_CONNECTION_STRING',
        'postgresql://etl_user:etl_password@postgres:5432/etl_warehouse'
    )
    
    loader = DatabaseLoader(conn_string, schema='public')
    
    # Load to warehouse
    rows_loaded = loader.load(
        df,
        table_name='fact_sales',
        strategy='append',
        batch_size=1000
    )
    
    print(f"Loaded {rows_loaded} rows to warehouse")
    return rows_loaded


def send_success_notification(**context):
    """Send success notification."""
    print("Pipeline completed successfully!")
    
    # Get statistics
    ti = context['task_instance']
    rows_loaded = ti.xcom_pull(task_ids='load_to_warehouse')
    
    print(f"Total rows loaded: {rows_loaded}")
    
    # Here you can add email/Slack notification
    # send_slack_message(f"ETL Pipeline completed: {rows_loaded} rows loaded")
    
    return True


# Define tasks
check_freshness = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_source_data_freshness,
    dag=dag,
)

extract_csv = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_csv_data,
    dag=dag,
)

extract_api = PythonOperator(
    task_id='extract_api',
    python_callable=extract_api_data,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

load_to_warehouse_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    dag=dag,
)

notify_success = PythonOperator(
    task_id='send_notification',
    python_callable=send_success_notification,
    dag=dag,
)

# Define dependencies
check_freshness >> [extract_csv, extract_api]
[extract_csv, extract_api] >> transform
transform >> load_to_warehouse_task >> notify_success
