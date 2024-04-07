-- model_2.sql

-- Join event information
-- Define source model
{{ config(materialized='table') }}

-- Load data into unprocessed_market_info
{{ dbt_utils.dispatch('copy_unprocessed_market_info') }}

-- Create temporary tables
{{ dbt_utils.dispatch('create_temp_tables_step_1') }}

-- Create marketinfo table
{{ dbt_utils.dispatch('create_market_info') }}

-- Create OneBigTableView
{{ dbt_utils.dispatch('create_one_big_table_view') }}

-- Create ONEBIGTABLEVIEW2
{{ dbt_utils.dispatch('create_one_big_table_view_2') }}

-- Insert into OneBigTable
{{ dbt_utils.dispatch('insert_into_one_big_table') }}
