# Fix Postgres integration test and also address FutureWarning and DeprecationWarning
##
DVT now uses duckdb to combine pandas dataframes instead of using ibis pandas connector. The code has been tested using `pytest tests/system/data_sources/test_postgres.py` and it is passing. However, there are some FutureWarning and DeprecationWarning messages. Also independently running the `pytest tests/system/data_sources/test_postgres.py resulted in some errors, so rerunning postgres integration tests is necessary to ensure that we have not introduced any regressions.

## Fix FutureWarnings, DeprecationWarnings and test failures
* You have to use the Python virtual environment by running `source env/bin/activate` from the shell.
* Run the unit tests and if any failures/warnings are observed, fix the root cause and rerun the tests. Objective is to have no failures and warnings.
* Prompt me  to set the environment variable PROJECT_ID
* Run the integration tests `pytest tests/system/data_sources/test_postgres.py` and if any failures/warnings are observed, fix the root cause and rerun the tests. Objective is to have no failures and warnings.

## Suggested Steps
* Run the tests as mentioned above
* Review list of failures to identify the cause. Sometimes multiple failures may be due to a single root cause. Fix the root cause and rerun the tests.
* Objective is to have all tests pass before moving to the next database type.
