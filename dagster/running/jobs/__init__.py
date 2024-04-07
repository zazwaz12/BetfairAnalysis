from dagster import pipeline, solid, job
from running.ops.stream import start_stream
from running.ops.s3 import json_to_s3
from running.ops.snowflakeMapping import get_market_ids
from running.ops.dbt import dbt_model1, dbt_model2

# Define solid functions
@solid
def start_stream_solid():
    start_stream()

@solid
def json_to_s3_solid():
    json_to_s3()

@solid
def get_market_ids_solid():
    get_market_ids()

@solid
def dbt_model_1_solid():
    dbt_model1()

@solid
def dbt_model_2_solid():
    dbt_model2()

# Define Dagster pipelines
@pipeline
def market_prices_pipeline():
    start_stream_solid()
    json_to_s3_solid()
    #dbt_model_1_solid()

@pipeline
def market_info_pipeline():
    get_market_ids_solid()
    json_to_s3_solid()
    #dbt_model_2_solid()

# Define Dagster jobs
@job()
def run_market_prices():
    return market_prices_pipeline

@job()
def run_market_info():
    return market_info_pipeline
