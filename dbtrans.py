import pandas as pd
from sqlalchemy import create_engine, text

# Define connection strings for KingChuck (source) and MyChuck (destination) SQL Servers
source_conn_string =  'mssql+pyodbc://demo:demo123@Demo/DataAnalytics?driver=ODBC+Driver+17+for+SQL+Server'
target_conn_string =  'mssql+pyodbc://demo:Dem0123@Demo/DataAnalytics?driver=ODBC+Driver+17+for+SQL+Server'


# Create engine connections for KingChuck and MyChuck databases
source_engine = create_engine(source_conn_string)
target_engine = create_engine(target_conn_string)

# Extract: Read data from the 'Chuckie' table on Chuck-Framework SQL Server
def extract_data():
    query = "SELECT * FROM people10000 Where Job_Title = 'Database Administrator'"
    source_data = pd.read_sql(query, source_engine)
    return source_data

# Transform: Rename the column 'Chuck' to 'Chuckie'
def transform_data(data):
    data.rename(columns={'count2': 'Count'}, inplace=True)
    return data


# Load: Insert data into the 'sales_records' table on MyChuck SQL Server
def load_data(data):
    data.to_sql('MaleJobTitle', target_engine, if_exists='append', index=False)

# Run the ETL process
def run_etl():
    # Step 1: Extract
    extracted_data = extract_data()

    # Step 2: Transform (rename 'Chuck' to 'Chuckie')
    transformed_data = transform_data(extracted_data)

    # Step 3: Load
    load_data(transformed_data)

    print("ETL process from KingChuck (CarPrice_Assignment) to MyChuck (CarPrice_Assignment) completed successfully.")

# Run the ETL pipeline
run_etl()


