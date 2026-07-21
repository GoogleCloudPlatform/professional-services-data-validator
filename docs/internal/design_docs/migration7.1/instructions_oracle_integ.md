# Instructions to run Oracle integration tests
##
Now that Postgres, SQL Server and BigQuery integration tests are passing here are instructions to run the Oracle integration tests along with PostgreSQL server.
## How to run the integration tests
* You have to use the Python virtual environment by running `source env/bin/activate` from the shell.
* Prompt me to set the environment variables PROJECT_ID, ORACLE_PASSWORD and POSTGRES_PASSWORD
* Run the integration tests as follows `pytest tests/system/data_sources/test_oracle.py`

## Suggested Steps
* Run the tests as mentioned above
* Review list of failures to identify the cause. Sometimes multiple failures may be due to a single root cause. Fix the root cause and rerun the tests.
* Objective is to have all tests pass before moving to the next database type.
