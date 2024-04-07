from dagster import Definitions, EnvVar

from running.jobs import run_market_info, run_market_prices

defs = Definitions(
    jobs=[run_market_prices, run_market_info]
)
