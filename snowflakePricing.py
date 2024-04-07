import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Snowflake connection parameters
conn = snowflake.connector.connect(
    user=os.getenv('sf.user'),
    password=os.getenv('sf.password'),
    account=os.getenv('sf.account'),
    warehouse=os.getenv('sf.warehouse'),
    database=os.getenv('sf.database'),
    schema=os.getenv('sf.schema')
)


# SQL script to execute
sql_script = """
DROP TABLE IF EXISTS stream_odds_data;
CREATE TABLE stream_odds_data (
    json VARIANT
);
COPY INTO stream_odds_data
FROM @my_stage
FILE_FORMAT = (TYPE = 'JSON')
PATTERN = 'Streams/.*\.json';

-- Insert image into the table
INSERT INTO IMAGE
SELECT *
FROM stream_odds_data
LIMIT 1; -- Extract only the top row

INSERT INTO changes (
    SELECT *
    FROM stream_odds_data
    EXCEPT  -- Remove the initial images
    SELECT *
    FROM image
);

CREATE OR REPLACE TEMPORARY TABLE image_1 AS
SELECT 
    a.json:initialClk::string AS image,
    a.json:pt::int AS timestamp,
    a.json:mc AS markets
FROM
    image a;

CREATE OR REPLACE TEMPORARY TABLE image_2 as
SELECT 
    mc.value:id::string AS Market_id,
    mc.value:rc AS rc,
    f.image AS Image,
    f.timestamp AS Timestamp
FROM 
    image_1 f,
    LATERAL FLATTEN(input => f.markets) mc;

CREATE OR REPLACE TEMPORARY TABLE image_3 AS
SELECT
    x.MARKET_ID,
    x.IMAGE,
    x.TIMESTAMP,
    ab.value
FROM
    image_2 x,
    LATERAL FLATTEN(input => parse_json(x.rc)) ab;

CREATE OR REPLACE TEMPORARY TABLE image_4 AS
WITH flattened_data AS (
    SELECT 
        x.MARKET_ID,
        x.IMAGE,
        x.TIMESTAMP,
        val.KEY AS bet_type,
        CASE 
            WHEN val.KEY = 'id' THEN val.VALUE::STRING 
            ELSE val.VALUE
        END AS value
    FROM 
        image_3 x,
        LATERAL FLATTEN (input => parse_json(x.value)) val
)
SELECT
    MARKET_ID,
    IMAGE,
    TIMESTAMP,
    MAX(CASE WHEN bet_type = 'id' THEN value END) AS id,
    MAX(CASE WHEN bet_type = 'batb' THEN value END) AS batb,
    MAX(CASE WHEN bet_type = 'batl' THEN value END) AS batl
FROM 
    flattened_data
GROUP BY
    MARKET_ID,
    IMAGE,
    TIMESTAMP;

CREATE OR REPLACE TEMPORARY TABLE image_5 AS
SELECT
    MARKET_ID,
    IMAGE,
    TIMESTAMP,
    ID,
    parse_json(batb)[0][1]::FLOAT AS batb_odds,
    parse_json(batb)[0][2]::FLOAT AS batb_amount,
    parse_json(batl)[0][1]::FLOAT AS batl_odds,
    parse_json(batl)[0][2]::FLOAT AS batl_amount
FROM 
    image_4;

CREATE OR REPLACE TEMPORARY TABLE MARKET_SNAPSHOT AS
SELECT * FROM image_5
ORDER BY Market_id;
--------------------------------------------------
-- Now for the market changes
--------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE change_1 AS
SELECT 
    a.json:clk::string AS image,
    a.json:pt::int AS timestamp,
    a.json:mc AS markets
FROM
    CHANGES a;

CREATE OR REPLACE TEMPORARY TABLE change_2 as
SELECT 
    mc.value:id::string AS Market_id,
    mc.value:rc AS rc,
    f.image AS Image,
    f.timestamp AS Timestamp
FROM 
    change_1 f,
    LATERAL FLATTEN(input => f.markets) mc;

CREATE OR REPLACE TEMPORARY TABLE change_3 AS
SELECT
    x.MARKET_ID,
    x.IMAGE,
    x.TIMESTAMP,
    ab.value
FROM
    change_2 x,
    LATERAL FLATTEN(input => parse_json(x.rc)) ab;

CREATE OR REPLACE TEMPORARY TABLE change_4 AS
WITH flattened_data AS (
    SELECT 
        x.MARKET_ID,
        x.IMAGE,
        x.TIMESTAMP,
        val.KEY AS bet_type,
        CASE 
            WHEN val.KEY = 'id' THEN val.VALUE::STRING 
            ELSE val.VALUE
        END AS value
    FROM 
        change_3 x,
        LATERAL FLATTEN (input => parse_json(x.value)) val
)
SELECT
    MARKET_ID,
    IMAGE,
    TIMESTAMP,
    MAX(CASE WHEN bet_type = 'id' THEN value END) AS id,
    MAX(CASE WHEN bet_type = 'batb' THEN value END) AS batb,
    MAX(CASE WHEN bet_type = 'batl' THEN value END) AS batl
FROM 
    flattened_data
GROUP BY
    MARKET_ID,
    IMAGE,
    TIMESTAMP;

CREATE OR REPLACE TEMPORARY TABLE change_5 AS
SELECT
    MARKET_ID,
    IMAGE,
    TIMESTAMP,
    ID,
    parse_json(batb)[0][1]::FLOAT AS batb_odds,
    parse_json(batb)[0][2]::FLOAT AS batb_amount,
    parse_json(batl)[0][1]::FLOAT AS batl_odds,
    parse_json(batl)[0][2]::FLOAT AS batl_amount
FROM 
    change_4;

CREATE OR REPLACE TEMPORARY TABLE MARKET_CHANGE AS
SELECT * FROM change_5
ORDER BY Market_id;

CREATE OR REPLACE VIEW all_market_data AS
SELECT * FROM MARKET_CHANGE
UNION ALL
SELECT * FROM MARKET_SNAPSHOT;

CREATE OR REPLACE TABLE UNIQUEEVENTS AS
SELECT DISTINCT(MARKET_ID) FROM all_market_data;
"""

# Split SQL script into individual statements
sql_statements = sql_script.split(';')

# Execute each statement one by one
for sql_statement in sql_statements:
    if sql_statement.strip():  # Skip empty statements
        conn.cursor().execute(sql_statement)

# Closing the connection
conn.close()
