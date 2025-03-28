import pandas as pd
import requests
import logging
import time
from datetime import datetime, timedelta

def flatten_schedule_data(schedules):
    """
    Flattens the nested schedule data structure into a pandas DataFrame.

    The JSON structure is nested as follows:
    - 'stop-schedule' (parent) > 'stop' (details about the stop, including 'key', 'name', 'direction', etc.)
    - 'route-schedules' (list of routes) > 'route' (route details, including 'key', 'name', 'number', etc.)
    - 'scheduled-stops' (list of scheduled stops with 'key', 'times' for arrival and departure, 'bus' info, etc.)

    Parameters:
    schedules (list): List of schedule records, each containing a nested structure with 'stop-schedule' and 'route-schedules'.

    Returns:
    pd.DataFrame: A DataFrame containing flattened schedule information.
    """
    flattened_data = []

    # Loop through each record in the schedules list
    for record in schedules:
        # Extract stop details from 'stop-schedule'
        stop_schedule = record.get('stop-schedule', {})
        stop_number = stop_schedule.get('stop', {}).get('number', None)
        stop_name = stop_schedule.get('stop', {}).get('name', None)
        
        # Extract route schedules. Each route-schedule contains a list of scheduled stops
        route_schedules = stop_schedule.get('route-schedules', [])
        
        # Loop through route-schedules inside one object
        for route_schedule in route_schedules:
            # Extract scheduled stops for the current route
            scheduled_stops = route_schedule.get('scheduled-stops', [])
            
            # Loop through each scheduled stop for the route
            for stop in scheduled_stops:
                # Create a dictionary with the relevant information for each scheduled stop
                stop_info = {
                    'stop_number': stop_number,  # Include stop number
                    'stop_name': stop_name,  # Include stop name
                    'route_key': route_schedule.get('route', {}).get('key', None),
                    'route_name': route_schedule.get('route', {}).get('name', None),
                    'route_number': route_schedule.get('route', {}).get('number', None),
                    'scheduled_stop_key': stop.get('key', None),
                    'cancelled': stop.get('cancelled', None),
                    'arrival_scheduled': stop.get('times', {}).get('arrival', {}).get('scheduled', None),
                    'arrival_estimated': stop.get('times', {}).get('arrival', {}).get('estimated', None),
                    'departure_scheduled': stop.get('times', {}).get('departure', {}).get('scheduled', None),
                    'departure_estimated': stop.get('times', {}).get('departure', {}).get('estimated', None),
                    'variant_key': stop.get('variant', {}).get('key', None),
                    'variant_name': stop.get('variant', {}).get('name', None),
                    'bus_key': stop.get('bus', {}).get('key', None),
                    'bus_bike_rack': stop.get('bus', {}).get('bike-rack', None),
                    'bus_wifi': stop.get('bus', {}).get('wifi', None),
                }
                flattened_data.append(stop_info)

    # Convert the flattened data into a pandas DataFrame
    df = pd.DataFrame(flattened_data)
    
    # Add a timestamp of when the data was fetched
    df['timestamp_fetched'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    return df   
def make_api_call_for_bus_stop_schedules(url, headers, key, stop_number, time_range, dataset_name='stops'):
    """
    Makes api call to get bus scheules for a given stop number.

    Args:
        url (str): The base URL of the API.
        headers (dict): The headers to be included in the request.
        key (str): The API key used for authentication.
        stop_number (str): The unique key for the stop number for which the schedules are being fetched.
        time_range (int): The time range in hours for which the schedules are being fetched.
        dataset_name (str, optional): The name of the main dataset to fetch. For this function, it is 'stops'.

    Returns:
        dict or None: A json dictionary containing the stop schedule for the mentioned time range (by default for the next hour from current fetch time) if successful, otherwise None.
    
    Logs errors if the request fails or the status code is not 200.
    """
    # Set the time range for the schedules. By default API picks start time as fetch time so we mention end time
    start_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    end_time = datetime.now() +  timedelta(hours=time_range)
    end_time = end_time.strftime('%Y-%m-%dT%H:%M:%S')
    api_url = f"{url}/{dataset_name}/{stop_number}/schedule.json?api-key={key}&end={end_time}"
    
    
    try:
        response = requests.get(api_url, headers)
        if response.status_code == 200:
            logging.info(f"Fetched {dataset_name} schedules for stop number {stop_number} from start time start_time {start_time} and end time {end_time}")
            return response.json()
        else:
            logging.error(f"Failed to fetch {dataset_name} schedules for stop number {stop_number}. Status Code: {response.status_code} with message {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {dataset_name} schedules for stop number {stop_number}: {e}")
        return None

def fetch_stop_schedules_for_busstops(stops_list, url, headers, key, time_range, dataset_name='stops'):
    """
    Processes a list of stops (which comes through stops dataset) and  runs for loop to fetch stop scheules for each stop.

    Args:
        varistops_listant_list (list): A list of stop number keys to fetch schedules for.
        url (str): The base URL of the API.
        headers (dict): The headers to be included in the request.
        key (str): The API key used for authentication.
        time_range (int): The time range in hours for which the schedules are being fetched.
        dataset_name (str, optional): The name of the dataset to fetch. Default is 'stops'.

    Returns:
        List of json: A list of json dictionaries containing the schedule data for each stop for the give time range (by default for the next hour).
    
    The function will call `make_api_call_for_bus_stop_schedules` for each stop in the list and append the json output to schedules list.
    It will also respect rate limits by waiting for 0.6 seconds between each request. Since winnipet transit API allows 100 requests per minute.
    """    
    all_schedule_data = []
    
    for stop_number in stops_list:
        
        api_data_output = make_api_call_for_bus_stop_schedules(url, headers, key, stop_number, time_range, dataset_name='stops')
        
        if api_data_output:
            all_schedule_data.append(api_data_output)
        
        # Wait for 0.6 seconds to not exceed 100 requests per minute
        time.sleep(0.6)
    logging.info(f"Fetch of schedules completed {len(stops_list)} and extracted {len(all_schedule_data)} schedules")
    # Convert the collected destinations into a DataFrame
    df_schedules = flatten_schedule_data(schedules=all_schedule_data)
    logging.info(f"Dataframe created with {df_schedules.shape[0]} rows and {df_schedules.shape[1]} columns")
    return df_schedules
 