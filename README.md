# Winnipeg Transit ETL Pipeline

## Overview

The **Winnipeg Transit ETL Pipeline** is an automated data pipeline designed to extract, transform, and load (ETL) bus routes and destinations data from the Winnipeg Transit API into Google Cloud BigQuery. This project is aimed at providing structured and accessible transit data for further analysis and insights.

The pipeline is orchestrated using **Apache Airflow**, and the infrastructure is containerized using **Docker**. The project supports efficient data extraction, logging, error handling, and deployment. By using **Google Cloud's BigQuery**, the project ensures that large-scale transit data can be queried and processed quickly.

## Ideology

The core goal of this project is to automate the extraction of transit data from the **Winnipeg Transit API**, process the data, and store it in **Google BigQuery** for further usage. The approach is designed to be:

1. **Modular**: Each part of the pipeline (extraction, processing, loading) is encapsulated in independent tasks within Airflow, ensuring flexibility and ease of debugging.
2. **Scalable**: With the use of Docker containers, the application can be easily deployed in any environment. By integrating Google Cloud BigQuery, the data is stored in a highly scalable data warehouse.
3. **Reliable**: Detailed logging and error-handling mechanisms ensure that any failure in the pipeline is tracked and retried automatically.
4. **Efficient**: By using Airflow’s scheduling and task dependencies, we ensure that the ETL process runs on time every day, extracting and loading fresh data.

## Architecture

### High-Level Architecture

The ETL pipeline consists of the following key components:

1. **Data Extraction (API)**: Data is fetched from the **Winnipeg Transit API** for the bus routes and their respective variants. The API provides bus route details, including variant data, which can be used to fetch destination information.

2. **Data Processing**: The extracted data is then processed, including transforming the routes data into a structured format and retrieving destination details for each variant.

3. **Data Loading (BigQuery)**: After processing, the data is stored in **Google Cloud BigQuery**. This enables easy querying and access to the data for further analysis.

4. **Airflow Orchestration**: The entire process is managed using **Apache Airflow**. Airflow schedules tasks, handles dependencies, retries on failure, and logs execution details for transparency and debugging.

5. **Dockerized Deployment**: The entire solution runs inside Docker containers for consistency and portability. Airflow, along with the necessary Python packages, is set up within Docker containers.

### Components

- **Winnipeg Transit API**: The external API from which we extract the data about bus routes and variants.
- **Airflow**: The tool used to orchestrate the ETL pipeline. It manages the flow of tasks, ensuring that each step in the ETL process is executed at the right time.
- **Google Cloud BigQuery**: The data warehouse used to store the processed data. It provides a scalable platform for storing and querying large datasets.
- **Docker**: The containerization platform used to create a portable and consistent environment for running the ETL pipeline.

## Project Structure
This project follows a typical structure for an ETL pipeline. Below is an explanation of each component within the directory structure:

```
Winnipeg Transit ELT Pipeline
├── dags/                                    # Contains Directed Acyclic Graphs (DAGs) for workflow automation.
│   └── .airflowignore                        
│   └── ExtractLoad.py                        # Script for extracting and loading data as part of the ETL pipeline.
├── utils/                                   # Contains utility scripts that provide helper functions.
│   └── ETL_functions.py                      # Functions related to ETL processes like data extraction, transformation, and loading.
│   └── function_df_to_csv.py                 # Converts DataFrame into CSV format for saving or further processing.
│   └── function_todays_extracted_data.py     # Extracts today's data for processing in the pipeline.
├── .dockerignore                             
├── .gitignore                                
├── docker-compose.override.yml               # Overrides Docker Compose settings, typically used in development environments.
├── Dockerfile                                # Contains instructions for building the Docker image of the project.
├── packages.txt                             
├── README.md
├── requirements.txt                          
```

## Approach

### 1. **Data Extraction**

The **Winnipeg Transit API** provides endpoints to extract information about bus routes and their respective variants. The data extraction is done through Airflow’s **PythonOperator**, which runs the `extract_routes` function.

- The function fetches bus route data from the API endpoint.
- The variants for each bus route are extracted and stored as a list of variant keys.

**Note**: During this phase, I used the **Astronomer CLI** to jump straight into Airflow DAG development, avoiding setup issues related to installation and environment configuration. This allowed for faster iterations and testing of the pipeline.

### 2. **Data Processing**

After the routes and variants data is fetched, we extract the destination data for each variant using the `fetch_destinations_for_variants` function. This process involves:

- Fetching destination details for each route variant by calling the Winnipeg Transit API.
- Processing the data into a structured format (Pandas DataFrame).
- Saving the processed data into a CSV file using the `save_df_to_csv` function.

**Note**: At this stage, I mounted a volume directory using the **Docker Compose Override file**, allowing the extracted data to reside locally before the subsequent cloud steps (e.g., BigQuery loading). This helped in organizing the flow of the data and testing locally before pushing it to the cloud.

### 3. **Data Loading to BigQuery**

Once the data is processed, the pipeline pushes the resulting CSVs (for routes and destinations) into **Google Cloud BigQuery** using the **BigQueryHook** from Airflow. The `push_to_big_query` function is responsible for:

- Reading the CSV files containing the processed data.
- Loading the data into the designated BigQuery tables using the `to_gbq` method.

**Note**: To ensure that Airflow can communicate effectively with **BigQuery**, I had to enable the **Google Cloud Resource Manager API** and provide the service account email with appropriate IAM permissions.

### 4. **Airflow DAG**

The Airflow DAG defines the pipeline’s schedule, dependencies, and task flow. Here's the high-level task flow:

- **Task 1**: `api_data_extraction_task`: Extracts bus routes and variants data from the API.
- **Task 2**: `process_destinations_task`: Processes the destination data for each route variant.
- **Task 3**: `push_routes_to_big_query_task`: Loads processed route data into BigQuery.
- **Task 4**: `push_destinations_to_big_query_task`: Loads processed destinations data into BigQuery.

The Airflow DAG is scheduled to run daily, ensuring that the data is always up-to-date in BigQuery.

**Note**: Using **XCom** in Airflow, I passed the list of route variants from the routes extraction task to the destinations extraction task, allowing for more efficient handling of destination data for each route variant.

## Technologies

- **Python 3.x**: The primary language used for the ETL pipeline.
- **Astro CLI**: Pre-packaged installation and environment configuration for Apache Airflow
- **Apache Airflow**: Orchestration tool to manage the ETL pipeline.
- **Docker**: Containerization of the application.
- **Google Cloud BigQuery**: Data warehouse for storing the extracted and processed transit data.
- **Winnipeg Transit API**: External API to fetch bus route and destination data.

## Setup Instructions

1. **Clone the Repository**:
    ```bash
    git clone https://github.com/akshay-gera/Winnipeg-Transit-ELT-Pipeline.git
    cd Winnipeg-Transit-ELT-Pipeline
    ```

2. **Install Dependencies**:
    Create a virtual environment (optional but recommended) and install the required Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. **Set Up Google Cloud Credentials**:
    Make sure you have configured Google Cloud credentials to access BigQuery.

4. **Run Airflow Locally with Astro CLI**:
    You can run the Airflow DAG locally using **Astronomer CLI**. First, ensure that **Astro CLI** is installed, then run the following command:
    ```bash
    astro dev start
    ```

5. **Schedule and Monitor the DAG**:
    The DAG is set to run every day, and you can monitor its progress from the Airflow UI.

## Conclusion

The **Winnipeg Transit ETL Pipeline** provides an automated, scalable solution for fetching, processing, and loading transit data into **Google BigQuery**. By leveraging **Apache Airflow** for orchestration, **Docker** for containerization, and **Google Cloud BigQuery** for data storage, this pipeline offers a reliable way to manage and analyze large transit datasets.
