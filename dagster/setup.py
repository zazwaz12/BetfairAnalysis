from setuptools import find_packages, setup

setup(
    name="running",
    packages=find_packages(exclude=["running_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
