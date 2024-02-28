'''
============================================================================================================================================================================================================
Coffee Bean Quality Analysis Automation File

This program is created to automate the transformation and loading of data from PostgreSQL to ElasticSearch using Apache Airflow. 
Created on May 2023, the dataset was collected by the International Coffee Organization (ICO), which conducts extensive research on coffee and its global economic impact. 
ICO has gathered information on Arabica coffee bean producers from countries worldwide, including details on sensory qualities (aroma, flavor, clean cup, etc.) and the appearance of coffee beans (color). 
============================================================================================================================================================================================================
'''

# Import Libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from elasticsearch import Elasticsearch
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    # change the datetime for proceeding the automation accordingly
    'start_date': datetime(2024, 2, 25,21,54) - timedelta(hours = 7),
    # 'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'max_active_runs': 1,
    'catchup': False,
    'execution_timeout': timedelta(minutes=30),
}

# Function to fetch data from PostgreSQL
def fetch_data_from_postgres():
    """
    this function will fetch data from postgreSQL.
    initially, it would establish connection with database called airflow.
    then, it would read the sql table with name table_m3 then convert it into json file.
    the json file will be saved in folder dags.

    """
    try:
        conn_string = "dbname='airflow' user='airflow' password='airflow' host='postgres' port=5432"
        connection = psycopg2.connect(conn_string)
        # Take data from the table
        df = pd.read_sql("SELECT * FROM table_m3", connection)
        # Close connection
        connection.close()
        # Convert DataFrame to JSON
        json_data = df.to_json('/opt/airflow/dags/json_data.json',orient='records')
        return json_data
    except Exception as e:
        # Log the error
        logging.error("Error occurred while fetching data from PostgreSQL: %s", str(e))
        # Raise the error to mark the task as failed
        raise e

"""
THE FUNCTIONS BELOW ARE FOR DATA CLEANING
"""
def column_names_converter(dataset):
    """
    this function will convert all column names in lower case and replace the whitespaces with underscore.
    the old_column_names keeps previous column names in a lost.
    new_column_names makes a new list keeping the new column names. Just for In-Country Partner, - needs to be replaced with _.

    """
    old_column_names = dataset.columns.tolist()
    # make a new list keeping the new column names
    # for In-Country Partner, - needs to be replaced with _
    new_column_names = [name.lower().replace('-', '_') .replace(' ', '_') for name in old_column_names]
    return new_column_names

# handle missing values
def missing_values_handler(dataset):
    """
    this function will handle the missing values from column farm_name, variety, processing_method, region, and altitude.
    it would replace some values equivalent to missing values into one common phrase such as no_record and others.
    """
    # handling missing values for farm_name
    dataset['farm_name'] = dataset['farm_name'].fillna('no_record')
    # replace value in farm_name
    dataset['farm_name'] = dataset['farm_name'].str.replace('Not Specificated','no_record')
    dataset['farm_name'] = dataset['farm_name'].str.replace('N / A','no_record')
    dataset['farm_name'] = dataset['farm_name'].str.replace('-','no_record')

    # handling missing values for variety
    dataset['variety'] = dataset['variety'].fillna('no_record')
    dataset['variety'] = dataset['variety'].str.replace('unknown','no_record')
    dataset['variety'] = dataset['variety'].str.replace('unknow','no_record')

    # handling missing values for processing_method
    dataset['processing_method'] = dataset['processing_method'].fillna('others')

    # handling missing values for region
    dataset['region'] = dataset['region'].fillna('no_record')

    # handling missing values for altitude
    dataset.dropna(subset=['altitude'], inplace=True)

    return dataset

def bag_weight_kg_removal(main_data):
    """
    The function will first remove 'kg' on data, then remove any whitespaces. After that, it will convert data type into float.
    Finally, it will change the column name from bag_weight to bag_weight_kg.
    """
    main_data['bag_weight'] = main_data['bag_weight'].str.replace('kg','')
    main_data['bag_weight'] = main_data['bag_weight'].apply(lambda x: x.strip())
    main_data['bag_weight'] = main_data['bag_weight'].astype(float)
    main_data.rename(columns={'bag_weight':'bag_weight_kg'},inplace = True)

    return main_data

def get_average_altitude(altitude):
    """
    The function will convert all data into string form first to ensure consistency of data type in the column. After that, it will calculate the averages.
    Finally, it will return the final calculation of averaged altitude values or altitude data conversion into float.
    """
    altitude = str(altitude)
    # conditions for calculating averages
    if '-' in altitude:
        start_alt, end_alt = map(int, altitude.split('-'))
        return (start_alt + end_alt) / 2
    elif '~' in altitude:
        start_alt, end_alt = map(int, altitude.split('~'))
        return (start_alt + end_alt) / 2
    elif 'A' in altitude:
        start_alt, end_alt = map(int, altitude.split('A'))
        return (start_alt + end_alt) / 2
    # mark nan value as 0
    elif 'nan' in altitude:
        return 0
    else:
        # if there is only a single number in the string, just convert into float
        return float(altitude)

def variety_changer(dataset):
    """
    The function will replace the unnecessary phrases/symbols : +, blend,Y, & and.
    The presence of more than 1 coffee type indicates the coffee product is a blend.
    """
    # Replace the values
    dataset['variety'] = [vary.replace(' + ', ',').replace('+', ',').replace('blend', '').replace(' Y ', ',').replace(' & ', ',').replace(' and ', ',').lower() for vary in dataset['variety']]
    return dataset

def country_name_changer(dataset):
    """
    The function will replace Tanzania, United Republic Of into Tanzania so that the country name is shorter.
    """
    dataset['country_of_origin'] = dataset['country_of_origin'].replace('Tanzania, United Republic Of','Tanzania')
    return dataset

def color_value_converter(dataset):
    """
    The function will replace some miss-spelled words such as 'yello-green' and browish-green.
    Then, the function will also changes other forms of yellow-green ('yellow green', and 'yellow- green') into yellow-green 
    for consistency.
    """
    # for typo color yellow-green
    # 'yello-green','yellow green', and 'yellow- green'
    dataset['color'] = dataset['color'].replace('yello-green','yellow-green')
    dataset['color'] = dataset['color'].replace('yellow green','yellow-green')
    dataset['color'] = dataset['color'].replace('yellow- green','yellow-green')

    # typo browish-green
    dataset['color'] = dataset['color'].replace('browish-green','brownish-green')
    return dataset

def method_name_changer(dataset):
    """
    The function change the method name 'Pulped natural / honey' into 'Semi Washed'
    because they are the same methods.
    """
    dataset['processing_method'] = dataset['processing_method'].replace('Pulped natural / honey', 'Semi Washed')
    dataset['processing_method'] = dataset['processing_method'].str.lower()
    return dataset

def apply_cleaning_functions(dataset):
    """
    The function starts from converting the column names into format requested by the assignment.
    Then, it drops the irrelevan columns for the study. It also ensures no duplicated data.
    Afterwards, it applies the functions to handle missing values as well as 
    convert bag_weight, altitude, variety, country_of_origin, and color.
    Finally, it recalculates the total_cup_points.

    """
    # Convert column names
    new_columns = column_names_converter(dataset)
    # Replace the column names on the dataset
    dataset.columns = new_columns

    # change dropping unnecessary column names
    dataset.drop(['lot_number','mill','ico_number','producer','in_country_partner','owner','company','harvest_year',
                  'clean_cup','sweetness','grading_date','status','expiration','certification_body','certification_address',
                  'certification_contact'], axis=1, inplace=True)
    
    # Drop duplicate data
    dataset.drop_duplicates(inplace=True)

    # apply function to fill missing values
    # implement the function
    dataset = missing_values_handler(dataset)
    
    # Apply the function to the 'altitude' column, strip the extra whitespaces first
    dataset['altitude'] = dataset['altitude'].str.strip()
    dataset['altitude'] = dataset['altitude'].apply(get_average_altitude)
    # drop the original altitude column
    dataset.rename(columns={'altitude':'average_altitude_m'},inplace = True)


    # apply bag_weight_kg_removal
    dataset = bag_weight_kg_removal(dataset)

    # apply variety_changer function
    dataset = variety_changer(dataset)

    # apply country_name_changer function
    dataset = country_name_changer(dataset)

    # apply variety_changer function
    dataset = color_value_converter(dataset)

    # multiply number of bags with bag_weight_kg
    dataset['total_bag_weight'] = dataset['number_of_bags'] * dataset['bag_weight_kg']

    # apply variety_changer function
    dataset = method_name_changer(dataset)

    # replace the total_cup_points value
    dataset['total_cup_points'] = dataset[['aroma', 'flavor', 'aftertaste', 'acidity', 'body', 'balance', 'uniformity', 'overall']].sum(axis=1)
    return dataset

def data_cleaning_task():
    """
    The function will perform data cleaning task on json_data from postgresql.
    All data cleaning functions are combined into a single function called apply_cleaning_functions.
    After that, the cleaned data are saved in json and csv files.
    """
    try:
        # Load the JSON file
        json_file_path = '/opt/airflow/dags/json_data.json'
        df = pd.read_json(json_file_path)
        
        # # Perform data cleaning operations
        cleaned_df = apply_cleaning_functions(df)
        # Save the cleaned data to CSV
        clean_data = cleaned_df.to_csv('/opt/airflow/dags/cleaned_data.csv', index=False)
        # Convert cleaned data to JSON
        cleaned_data_json = cleaned_df.to_json(orient='records')
        with open('/opt/airflow/dags/cleaned_data.json', 'w') as json_file:
            json_file.write(cleaned_data_json)
        
        # Log the successful completion
        logging.info("Cleaned data saved to /opt/airflow/dags/cleaned_data.csv and cleaned_data.json")
        return cleaned_data_json  # Return JSON data for Elasticsearch indexing
    except Exception as e:
        # Log the error
        logging.error("Error occurred during data cleaning: %s", str(e))
        # Raise the error to mark the task as failed
        raise e

def transport_to_elasticsearch(): #create the elasticsearch query
    """
    The function will establish connection to elasticsearch from airflow.
    Then, it would read the saved csv file called cleaned_data.csv.
    Afterwards, it will convert the data into JSON document form with index pattern name m3_coffee_testing.

    """
    es = Elasticsearch('http://elasticsearch:9200') #define the elasticsearch url
    df=pd.read_csv('/opt/airflow/dags/cleaned_data.csv') 
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="m3_coffee_testing", doc_type = "doc", body=doc)

# Define the DAG, the data is updated every 2 minutes
with DAG(
    "project_m3",
    description='Project Milestone 3',
    # update every 2 minutes, you can change to hourly, even monthly if you like
    schedule_interval='*/2 * * * *',
    default_args=default_args,
    catchup=False
) as dag:
    
    # Task 1: Fetch data from PostgreSQL
    fetch_data = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_from_postgres,
    )

    # Task for data cleaning
    data_cleaning = PythonOperator(
        task_id='data_cleaning_process',
        python_callable=data_cleaning_task,
    )

    # Task 3: Transport clean CSV into Elasticsearch
    post_to_elasticsearch = PythonOperator(
        task_id='send_to_elasticsearch_task',
        python_callable=transport_to_elasticsearch,
    )

# Define task dependencies
fetch_data >> data_cleaning >> post_to_elasticsearch
