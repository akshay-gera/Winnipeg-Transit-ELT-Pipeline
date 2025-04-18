
import logging
import os
from google.cloud import bigquery
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
import os
from utils.ETL_functions import extract_routes, fetch_destinations_for_variants, fetch_data_for_variants, fetch_stop_features_for_busstops
from utils.function_df_to_csv import save_df_to_csv
from utils.function_todays_extracted_data import get_today_extracted_data
from utils.function_stop_schedules import fetch_stop_schedules_for_busstops

# Creating a log process in our current directory where extraction and load success or failures will be documented
log_file_path = os.path.join(os.getcwd(), 'extraction.log')
logging.basicConfig(
    filename=log_file_path,
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Api End Point and parameters that we want

base_url = "https://api.winnipegtransit.com/v3"
# Fetching API key from the mentioned in airflow_settings.yaml file
API_KEY = BaseHook.get_connection('winnipeg_transit_api').extra_dejson.get('api_key')
headers = {
    "Authorization": f"Bearer {API_KEY}"
}
# Google Cloud BigQuery Project Name and Data Set
project_id = 'winnipeg-transit-data-pipeline'
dataset_id = f"{project_id}.Raw"
table_id = f"{dataset_id}.Raw_Data"

def extraction_routes(url=base_url, headers=headers, key=API_KEY, **kwargs):
    extract_routes(url, headers, key, **kwargs)

def process_routes(dataset_name='destinations',**kwargs):
    # Pull the 'variant_column' from XCom
    variant_column = kwargs['ti'].xcom_pull(task_ids='api_data_extraction_task', key='variant_column')
    if variant_column is None:
        logging.error("No variants found in XCom. Exiting...")
        return None
    logging.info(f"Fetched {len(variant_column)} variants from XCom.")

    destinations_df = fetch_destinations_for_variants(variant_list=variant_column, url=base_url, headers=headers, key=API_KEY, dataset_name='destinations')

    logging.info(f"Fetched {len(destinations_df)} destinations.")
    return save_df_to_csv(destinations_df, dataset_name, base_dir='/usr/local/airflow/extracted_data')

def process_stops(dataset_name='stops', **kwargs):
    # Pull the 'variant_column' from XCom
    variant_column = kwargs['ti'].xcom_pull(task_ids='api_data_extraction_task', key='variant_column')
    if variant_column is None:
        logging.error("No variants found in XCom. Exiting...")
        return None
    logging.info(f"Fetched {len(variant_column)} variants from XCom.")

    stops_df = fetch_data_for_variants(variant_list=variant_column, url=base_url, headers=headers, key=API_KEY, dataset_name='stops')

    logging.info(f"Fetched {len(stops_df)} stops for all the variants.")
    # Extracting just the 'variant' column as a list and pushing it to XCom
    stops_list = stops_df['stop_number'].tolist()
    # Get unique values
    stops_list = list(set(stops_list))
    kwargs['ti'].xcom_push(key='stop_numbers', value=stops_list)
    logging.info(f"Xcom pushed successfully with {len(stops_list)} records")
    return save_df_to_csv(stops_df, dataset_name, base_dir='/usr/local/airflow/extracted_data')

def process_stop_features(dataset_name='stop_features', **kwargs):
    # Pull the 'stop_numbers' from XCom
    stop_numbers = kwargs['ti'].xcom_pull(task_ids='process_stops_task', key='stop_numbers')
    if stop_numbers is None:
        logging.error("No stop numbers found in XCom. Exiting...")
        return None
    logging.info(f"Fetched {len(stop_numbers)} stop numbers from XCom.")
    stop_features_df = fetch_stop_features_for_busstops(stop_numbers, url=base_url, headers=headers, key=API_KEY, dataset_name='stop_features')
    logging.info(f"Fetched {len(stop_features_df)} stop features.")
    return save_df_to_csv(stop_features_df, dataset_name, base_dir='/usr/local/airflow/extracted_data')

def process_stop_schedules(dataset_name='stop_schedules', time_range=1, **kwargs):
    # Pull the 'stop_numbers' from XCom
    stop_numbers = kwargs['ti'].xcom_pull(task_ids='process_stops_task', key='stop_numbers')
    if stop_numbers is None:
        logging.error("No stop numbers found in XCom. Exiting...")
        return None
    logging.info(f"Fetched {len(stop_numbers)} stop numbers from XCom.")
    # Fetch the schedules for each stop number
    schedules_data = fetch_stop_schedules_for_busstops(stop_numbers, url=base_url, headers=headers, key=API_KEY, dataset_name='stops', time_range=time_range)
    logging.info(f"Fetched {len(schedules_data)} schedules.")
    return save_df_to_csv(schedules_data, dataset_name, base_dir='/usr/local/airflow/extracted_data')

def push_to_big_query(destination_table, project_id, dataset_name, new_data_strategy, **kwargs):
    df = get_today_extracted_data(dataset_name, base_dir='/usr/local/airflow/extracted_data')
    # Convert "timestamp_fetched" column to datetime if it's not already
    
    # Push the DataFrame to BigQuery using BigQueryHook
    hook = BigQueryHook(gcp_conn_id='gcp_conn')  # Use your Airflow BigQuery connection ID
    client = hook.get_client()
    destination_table = f"{dataset_id}.{destination_table}"
    logging.info(f"Pushing data to BigQuery table {destination_table}")
    # Load data to BigQuery
    df.to_gbq(destination_table, project_id, if_exists=new_data_strategy)
    logging.info(f"Data from successfully loaded to BigQuery table {destination_table}")

# DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 2, 15),
    'catchup': False,
}

dag = DAG(
    'Extract_and_Load_Pipeline_Winnipeg_Transit',
    default_args=default_args,
    description='Extracts data from Winnipeg Transit API and loads it to BigQuery',
    schedule_interval="00 12 * * *",  # Cron expression for 12:00 PM every day to get winnipeg 6AM schedules
    catchup=False,  # Ensures past runs are not triggered if not needed
)

# Task Definitions
api_routes_extraction_operator = PythonOperator(
    task_id='api_data_extraction_task',
    python_callable=extraction_routes,
    op_args=[base_url, headers, API_KEY],
    dag=dag,
    provide_context=True,
)

# Task Definitions for Processing Destinations
api_destinations_extraction_operator = PythonOperator(
    task_id='process_destinations_task',
    python_callable=process_routes,
    dag=dag,
    provide_context=True,  # Ensures 'ti' and other context are passed
)

# Task Definitions for Pushing Csv to BigQuery 
push_routes_to_big_query_operator = PythonOperator(
    task_id='push_routes_to_big_query_task',
    python_callable=push_to_big_query,
    op_args=['routes', project_id, 'routes', 'replace'],
    dag=dag,
)

# Task Definitions for Pushing Csv to BigQuery 
push_destinations_to_big_query_operator = PythonOperator(
    task_id='push_destinations_to_big_query_task',
    python_callable=push_to_big_query,
    op_args=['destinations', project_id, 'destinations', 'replace'],
    dag=dag,
)
# Task Definitions for Processing Stops
api_stops_extraction_operator = PythonOperator(
    task_id='process_stops_task',
    python_callable=process_stops,
    dag=dag,
    provide_context=True,  # Ensures 'ti' and other context are passed
)
# Task Definitions for Pushing Csv to BigQuery 
push_stops_to_big_query_operator = PythonOperator(
    task_id='push_stops_to_big_query_task',
    python_callable=push_to_big_query,
    op_args=['stops', project_id, 'stops', 'replace'],
    dag=dag,
)

# Task Definitions for Processing Stop Features
api_stop_features_extraction_operator = PythonOperator(
    task_id='process_stop_features_task',
    python_callable=process_stop_features,
    dag=dag,
    provide_context=True,  # Ensures 'ti' and other context are passed
)
# Task Definitions for Pushing Csv to BigQuery
push_stop_features_to_big_query_operator = PythonOperator(
    task_id='push_stop_features_to_big_query_task',
    python_callable=push_to_big_query,
    op_args=['stop_features', project_id, 'stop_features', 'replace'],
    dag=dag,
)

# Task Definitions for Processing Stop Schedules
api_stop_schedules_extraction_operator = PythonOperator(
    task_id='process_stop_schedules_task',
    python_callable=process_stop_schedules,
    dag=dag, 
    provide_context=True,  # Ensures 'ti' and other context are passed
)
# Task Definitions for Pushing Csv to BigQuery
push_stop_schedules_to_big_query_operator = PythonOperator(
    task_id='push_stop_schedules_to_big_query_task',
    python_callable=push_to_big_query,
    op_args=['stop_schedules', project_id, 'stop_schedules', 'replace'],
    dag=dag,
)
# Task dependencies: This sets the order of task execution
api_routes_extraction_operator >>  push_routes_to_big_query_operator >> api_destinations_extraction_operator >> push_destinations_to_big_query_operator >> api_stops_extraction_operator >> push_stops_to_big_query_operator  >> api_stop_schedules_extraction_operator >> push_stop_schedules_to_big_query_operator >> api_stop_features_extraction_operator >> push_stop_features_to_big_query_operator