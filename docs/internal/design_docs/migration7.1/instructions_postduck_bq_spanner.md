# Instructions to run Integration tests for Bigquery and Spanner
##
Now that DVT uses Duckdb backend, we need to run the BigQuery and Spanner integration tests to ensure that we have not introduced any regressions. Further source code should also be updated to avoid deprecated methods and improve the code quality.
## How to run the integration tests
* You have to use the Python virtual environment by running `source env/bin/activate` from the shell.
* Prompt me to set the environment variable PROJECT_ID
* Run the integration tests as follows `pytest tests/system/data_sources/test_bigquery.py` and `pytest tests/system/data_sources/test_spanner.py`

## Suggested Steps
* Run the tests as mentioned above
* Review list of failures and warnings about deprecated functions to identify the cause. Sometimes multiple failures may be due to a single root cause. Fix the root cause.
* If the test failures result in changes in DVT code, run the unit tests and then the integration tests to ensure no regressions were introduced.
* Objective is to have all tests pass before moving to the next database type.
