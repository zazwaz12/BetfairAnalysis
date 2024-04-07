-- model_1.sql

-- Run to refresh the data
-- Define source model
{{ config(materialized='table') }}

-- Load data into stream_odds_data
{{ dbt_utils.dispatch('copy_stream_odds_data') }}

-- Insert image into the table
{{ dbt_utils.dispatch('insert_image') }}

-- Insert changes
{{ dbt_utils.dispatch('insert_changes') }}

-- Create temporary tables
{{ dbt_utils.dispatch('create_temp_tables_image') }}
{{ dbt_utils.dispatch('create_temp_tables_change') }}

-- Create MARKET_SNAPSHOT table
{{ dbt_utils.dispatch('create_market_snapshot') }}
