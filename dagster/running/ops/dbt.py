import subprocess
from dagster import solid, Field, String

@solid()
def dbt_model1():
    dbt_command = f"dbt run --model MarketPrices"
    subprocess.run(dbt_command, shell=True, check=True)


@solid()
def dbt_model2():
    dbt_command = f"dbt run --model MarketInfo"
    subprocess.run(dbt_command, shell=True, check=True)
