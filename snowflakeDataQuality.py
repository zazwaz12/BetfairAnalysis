import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
import pandas as pd

# Snowflake connection parameters
conn = snowflake.connector.connect(
    user=os.getenv('sf.user'),
    password=os.getenv('sf.password'),
    account=os.getenv('sf.account'),
    warehouse=os.getenv('sf.warehouse'),
    database=os.getenv('sf.database'),
    schema=os.getenv('sf.schema')
)

"""
The One-Big-Table is only reliable as long as the data is accurately persisting
Data Quality tests will be used to make sure the data being inserted is reasonable

To check for null values in non-nullable field which is all but odds
To check for duplicates through natural conjugate keys
    Like selection ID and timestamp
To check to make sure data follows expectation of type. 
    market id = 1. --- 
    timestamp is an epoch in the 1.7 billion or trillion range depending on granularity (s or ms)
To check for outliers
"""

# Define data quality tests
def run_data_quality_tests():
    # Example data quality tests:
    # 1. Check for null values in 
    # 2. Check for duplicates
    # 3. Check for outliers
    
    # Example test: Check for null values in a specific table column
    last_time = 0
    last_count = 0
    with open('snowflakeCheckpoints.txt', 'r') as file:
        # Read the entire contents of the file
        last_time = file.readline()
        # Print the contents of the file
        last_count = file.readline()
    query = "SELECT MAX(TIMESTAMP) FROM ONEBIGTABLE"
    this_time = conn.cursor().execute(query).fetchone()[0]
    query = "SELECT COUNT(*) FROM ONEBIGTABLE"
    this_count = conn.cursor().execute(query).fetchone()[0]
    if this_count <= last_count:
        print("Data quality test: decreased row count in the table")
    if this_time <= last_time:
        print("Data quality test: no new data was added")
    else:
        print("Data quality test: new data was added!")
        # Open the text file in read mode


    # Add more data quality tests as needed...

# Run data quality tests
run_data_quality_tests()

# Close Snowflake connection
conn.close()
