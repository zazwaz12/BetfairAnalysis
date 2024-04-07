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
//NEW MODEL SECTION BELOW
//MODEL 2
// -----------------------------------------------------------------------
//Now joining the event information
//------------------------------------------------------------------------
// adds the json to the table as one cell
CREATE OR REPLACE TABLE unprocessed_market_info (
    json VARIANT
);
COPY INTO unprocessed_market_info
FROM @my_stage
FILE_FORMAT = (TYPE = 'JSON')
PATTERN = 'MatchInfos/.*\.json';
SELECT * FROM unprocessed_market_info;

CREATE OR REPLACE TEMPORARY TABLE step_1 AS
SELECT
    json_data.value:competition:name::string AS competition_name,
    CAST(EXTRACT(epoch_second FROM TO_TIMESTAMP_NTZ(json_data.value:event:openDate)) AS INT) AS openDate,
    DATEDIFF(second, '1970-01-01', CURRENT_TIMESTAMP)::NUMBER as AS_OF,
    json_data.value:eventType:id::string AS eventType_id,
    json_data.value:eventType:name::string AS eventType_name,
    json_data.value:marketId::string AS mkt_id,
    json_data.value:marketName::string AS market_name,
    LISTAGG(runner.value:runnerName, '-verse-') WITHIN GROUP (ORDER BY runner.index) AS runners_names,
    LISTAGG(runner.value:selectionId, '-') WITHIN GROUP (ORDER BY runner.index) AS runners_ids,
    json_data.value:totalMatched AS total_matched
FROM
    unprocessed_market_info,
    LATERAL FLATTEN(input => parse_json(json), path => 'result') AS json_data,
    LATERAL FLATTEN(input => json_data.value:runners) AS runner
GROUP BY
    competition_name,
    openDate,
    eventType_id,
    eventType_name,
    mkt_id,
    market_name,
    total_matched;


CREATE OR REPLACE TABLE marketinfo AS
WITH runners_cte AS (
  SELECT
    step_1.*,
    SPLIT(runners_names, '-verse-') AS runner_names_array,
    SPLIT(runners_ids, '-') AS runner_ids_array
  FROM
    step_1
)
SELECT
  competition_name,
  as_of,
  openDate,
  eventType_name,
  mkt_id,
  TRIM(L.VALUE) AS runner_name,
  TRIM(R.VALUE) AS selection_id,
  total_matched
FROM
  runners_cte
CROSS JOIN LATERAL FLATTEN(runner_names_array) L,
  LATERAL FLATTEN(runner_ids_array) R
WHERE L.INDEX = R.INDEX;

// Now I need to get my one big table
CREATE OR REPLACE VIEW OneBigTableView AS (
    SELECT *
    FROM marketinfo
    JOIN all_market_data ON marketinfo.selection_id = all_market_data.ID
    AND marketinfo.mkt_id = all_market_data.market_id
);

CREATE OR REPLACE VIEW ONEBIGTABLEVIEW2 AS
SELECT DATEADD(HOUR, 16, TO_TIMESTAMP_NTZ(AS_OF)) AS as_of_time, DATEADD(HOUR, 8, TO_TIMESTAMP_NTZ(opendate)) AS match_start_time, DATEADD(HOUR, 8, TO_TIMESTAMP_NTZ(TIMESTAMP/1000)) AS change_time, * FROM OneBigTableView;

INSERT INTO OneBigTable
(
    SELECT *
    FROM ONEBIGTABLEVIEW2 AS t2
    WHERE NOT EXISTS (
        SELECT 1
        FROM OneBigTable AS t1
        WHERE t1.MKT_ID = t2.MKT_ID
          AND t1.TOTAL_MATCHED = t2.TOTAL_MATCHED
          AND t1.CHANGE_TIME = t2.CHANGE_TIME
    )
);
"""

# Split SQL script into individual statements
sql_statements = sql_script.split(';')

# Execute each statement one by one
for sql_statement in sql_statements:
    if sql_statement.strip():  # Skip empty statements
        conn.cursor().execute(sql_statement)


# Closing the connection
conn.close()
