from setuptools import find_packages, setup

setup(
    name="data_pipeline_demo",
    packages=find_packages(exclude=["data_pipeline_demo_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
