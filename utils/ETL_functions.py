import requests
import logging
import os
import json
import pandas as pd
import time
from utils.function_df_to_csv import save_df_to_csv

# Defining the path where the log file will be stored
log_file_path = os.path.join(os.getcwd(), 'extraction.log')
print(f"Current working directory: {os.getcwd()}")
print(f"Log file will be created at: {log_file_path}")

# Set up logging to output to a file
logging.basicConfig(
    filename=log_file_path,  # Full path to the log file
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


def extract_routes(url, headers, key, dataset_name='routes', **kwargs):
    """
    Fetch all routes from the Winnipeg Transit API.
    Logs and returns data in the form of a pandas DataFrame.
    
    Parameters:
        - url: The base URL of the API
        - headers: The headers to pass with the request (including API key)
        - key: The API key to authenticate the request
        - kwargs: Additional keyword arguments to pass to the function relevant for Airflow Xcom Ti
    
    Returns:
        - df_routes_today: A pandas DataFrame containing the extracted routes, or None in case of failure.
    """
    try:
        logging.info("Starting data extraction from the API...")
        
        api_url = f"{url}/routes.json?api-key={key}"
        response = requests.get(api_url, headers)

        # To make sure we log messages from API call response which doesn't result into any data and also doesn't go into except code block
        if response.status_code == 200:
            logging.info("API Call Successful. No error messages returned")
            
            # Convertiung the response to JSON
            response_json = response.json()
            # Converting the JSON data to bytes for direct upload to GCS
            json_data = json.dumps(response_json).encode('utf-8') 

            # Checking if the expected 'routes' key exists
            if 'routes' not in response_json:
                logging.warning(f"API response doesn't contain 'routes' key: {response_json}")
                return None

            # If the 'routes' key exists, we can extract the data
            df = pd.json_normalize(response.json()['routes'], sep='_')
            # Tackling nested variants column to get complete dataframe for each bus variant
            df_exploded = df.explode('variants', ignore_index=True)
            # Normalize the 'variants' column to flatten it into individual columns
            df_normalized = pd.json_normalize(df_exploded['variants'])
            df_normalized.rename(columns={'key': 'variant'}, inplace=True)
            # Drop the original 'variants' column and concatenate the normalized columns
            df_exploded = df_exploded.drop(columns=['variants'])
            df_routes_today = pd.concat([df_exploded, df_normalized], axis=1)
            # Adding a timestamp column to the dataframe to track when the new data was fetched
            df_routes_today['timestamp_fetched'] = pd.Timestamp.now()
            logging.info(f"Today's Data Extracted with {len(df_routes_today)} records")
            
            save_df_to_csv(df_routes_today, dataset_name, base_dir='/usr/local/airflow/extracted_data')
            

            logging.info(f"CSV Generated with today's data")
            
            # Extracting just the 'variant' column as a list and pushing it to XCom
            variants_list = df_routes_today['variant'].tolist()
            kwargs['ti'].xcom_push(key='variant_column', value=variants_list)

            logging.info(f"Xcom pushed successfully with {len(variants_list)} records")
        else:
            logging.error(f"API Call failed. Response: {response.text}")
            return None
    # This logs error relating to HTTP request made via requests library        
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request made via requests library Error while fetching data from API: {e}")
        return None
    
    # Records any other error  relating to the function
    except Exception as e:
        logging.error(f"API Call Returned Error with message {e}")
        return None


def get_variant_destinations(url, headers, key, variant_key, dataset_name='destinations'):
    """
    Fetches the destination data for a given variant from an API.

    Args:
        url (str): The base URL of the API.
        headers (dict): The headers to be included in the request.
        key (str): The API key used for authentication.
        variant_key (str): The unique key for the variant for which the destinations are being fetched.
        dataset_name (str, optional): The name of the dataset to fetch. Default is 'destinations'.

    Returns:
        dict or None: A json dictionary containing the destination data if successful, otherwise None.
    
    Logs errors if the request fails or the status code is not 200.
    """
    api_url = f"{url}/variants/{variant_key}/{dataset_name}.json?api-key={key}"
    try:
        response = requests.get(api_url, headers)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Failed to fetch destinations for variant {variant_key}. Status Code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching destinations for variant {variant_key}: {e}")
        return None


def fetch_destinations_for_variants(variant_list, url, headers, key, dataset_name='destinations'):
    """
    Processes a list of variants (which comes through routes dataset) and  runs for loop to fetch destinations for each variant.

    Args:
        variant_list (list): A list of variant keys to fetch destination data for.
        url (str): The base URL of the API.
        headers (dict): The headers to be included in the request.
        key (str): The API key used for authentication.
        dataset_name (str, optional): The name of the dataset to fetch. Default is 'destinations'.

    Returns:
        pandas.DataFrame: A DataFrame containing the variant keys, destination names, and destination IDs.
    
    The function will call `get_variant_destinations` for each variant in the list and collect the destinations.
    It will also respect rate limits by waiting for 0.6 seconds between each request. Since winnipet transit API allows 100 requests per minute.
    """    
    all_destinations = []
    
    for variant_key in variant_list:
        logging.info(f"Fetching destinations for variant {variant_key}")
        
        destinations_data = get_variant_destinations(url, headers, key, variant_key, dataset_name='destinations')
        
        if destinations_data:
            destinations = destinations_data.get('destinations', [])
            for destination in destinations:
                # Assuming the response has a 'destination' field in each entry
                all_destinations.append({
                    'variant_key': variant_key,
                    'destination_name': destination.get('name', None),
                    'destination_id': destination.get('key', None)
                })
        
        # Wait for 0.6 seconds to not exceed 100 requests per minute
        time.sleep(0.6)
    
    # Convert the collected destinations into a DataFrame
    df_destinations = pd.DataFrame(all_destinations)
    df_destinations['timestamp_fetched'] = pd.Timestamp.now()
    return df_destinations





def make_api_call_for_bus_variants(url, headers, key, variant_key, dataset_name='stops'):
    """
    Makes api call to fetch bus stops data for each bus variant.

    Args:
        url (str): The base URL of the API.
        headers (dict): The headers to be included in the request.
        key (str): The API key used for authentication.
        variant_key (str): The unique key for the variant for which the stops are being fetched.
        dataset_name (str, optional): The name of the dataset to fetch. For this function, it is 'stops'.

    Returns:
        dict or None: A json dictionary containing the destination data if successful, otherwise None.
    
    Logs errors if the request fails or the status code is not 200.
    """
    api_url = f"{url}/{dataset_name}.json?api-key={key}&variant={variant_key}"
    
    try:
        response = requests.get(api_url, headers)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Failed to fetch {dataset_name} for variant {variant_key}. Status Code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {dataset_name} for variant {variant_key}: {e}")
        return None
    
def fetch_data_for_variants(variant_list, url, headers, key, dataset_name='stops'):
    """
    Processes a list of variants (which comes through routes dataset) and  runs for loop to fetch destinations for each variant.

    Args:
        variant_list (list): A list of variant keys to fetch destination data for.
        url (str): The base URL of the API.
        headers (dict): The headers to be included in the request.
        key (str): The API key used for authentication.
        dataset_name (str, optional): The name of the dataset to fetch. Default is 'destinations'.

    Returns:
        pandas.DataFrame: A DataFrame containing the variant keys, destination names, and destination IDs.
    
    The function will call `get_variant_destinations` for each variant in the list and collect the destinations.
    It will also respect rate limits by waiting for 0.6 seconds between each request. Since winnipet transit API allows 100 requests per minute.
    """    
    all_data_list = []
    
    for variant_key in variant_list:
        logging.info(f"Fetching {dataset_name} for variant {variant_key}")
        
        api_data_output = make_api_call_for_bus_variants(url, headers, key, variant_key, dataset_name='stops')
        
        if api_data_output:
            stops = api_data_output.get(dataset_name, [])
            for stop in stops:
                # Collecting the required details from each stop
                stop_data = {
                    'variant_key': variant_key,  # The variant key from the input list
                    'stop_id': stop.get('key', None),  # The unique key for the stop
                    'stop_name': stop.get('name', None),  # The name of the stop
                    'stop_number': stop.get('number', None),  # The stop number
                    'direction': stop.get('direction', None),  # The direction of the stop
                    'side': stop.get('side', None),  # The side of the street (e.g., farside, nearside)
                    'street_key': stop.get('street', {}).get('key', None),  # The key of the street where the stop is located
                    'street_name': stop.get('street', {}).get('name', None),  # The name of the street
                    'cross_street_key': stop.get('cross-street', {}).get('key', None),  # The key for the cross street
                    'cross_street_name': stop.get('cross-street', {}).get('name', None),  # The name of the cross street
                    'cross_street_leg': stop.get('cross-street', {}).get('leg', None),  # The leg of the cross street
                    'centre_utm_zone': stop.get('centre', {}).get('utm', {}).get('zone', None),  # The UTM zone
                    'centre_utm_x': stop.get('centre', {}).get('utm', {}).get('x', None),  # UTM X coordinate
                    'centre_utm_y': stop.get('centre', {}).get('utm', {}).get('y', None),  # UTM Y coordinate
                    'latitude': stop.get('centre', {}).get('geographic', {}).get('latitude', None),  # Latitude in geographic coordinates
                    'longitude': stop.get('centre', {}).get('geographic', {}).get('longitude', None),  # Longitude in geographic coordinates
                }

                # Add the stop data to the list for later DataFrame creation
                all_data_list.append(stop_data)
        
        # Wait for 0.6 seconds to not exceed 100 requests per minute
        time.sleep(0.6)
    
    # Convert the collected destinations into a DataFrame
    df = pd.DataFrame(all_data_list)
    df['timestamp_fetched'] = pd.Timestamp.now()
    return df