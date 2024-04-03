import os
import snowflake.connector
from dotenv import load_dotenv
from events import instantiate_session

# Load environment variables from .env file
load_dotenv()

# Establish connection to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv('sf.user'),
    password=os.getenv('sf.password'),
    account=os.getenv('sf.account'),
    warehouse=os.getenv('sf.warehouse'),
    database=os.getenv('sf.database'),
    schema=os.getenv('sf.schema')
)

# Create a cursor object to execute SQL queries
cur = conn.cursor()

# Execute a query to fetch data from your Snowflake column
query = "SELECT * FROM UniqueEvents"
cur.execute(query)

# Fetch all rows from the result set
rows = cur.fetchall()

# Extract the values from the rows and store them in a Python list
market_data = [(row[0]) for row in rows]

# Close cursor and connection
cur.close()
conn.close()

# Convert the list elements to strings wrapped in double quotes
market_data = [f"{element}" for element in market_data]

# Join the elements with commas to form a single string
print(market_data)
betfair = instantiate_session()
print(betfair.getMarketCatalogue(market_data))
# Now you have your data in the Python list `data_list`
#print(market_data)

