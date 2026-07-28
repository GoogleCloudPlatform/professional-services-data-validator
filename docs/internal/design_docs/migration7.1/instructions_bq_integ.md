# Instructions to run BigQuery integration tests
##
Now that Postgres and SQL Server integration tests are passing here are instructions to run the BigQuery integration tests.
## How to run the integration tests
* You have to use the Python virtual environment by running `source env/bin/activate` from the shell.
* Prompt me to set the environment variable PROJECT_ID
* Run the integration tests as follows `pytest tests/system/data_sources/test_bigquery.py`

## Suggested Steps
* Run the tests as mentioned above
* Review list of failures to identify the cause. Sometimes multiple failures may be due to a single root cause. Fix the root cause and rerun the tests.
* Objective is to have all tests pass before moving to the next database type.
