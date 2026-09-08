# Instructions to run Teradata integration tests
##
Now that Postgres, SQL Server, BigQuery, Oracle and Teradata integration tests are passing here are instructions to run the DB2, Impala, MySQL and Spanner integration tests.
## How to run the integration tests
* You have to use the Python virtual environment by running `source env/bin/activate` from the shell.
* Prompt me to set the environment variables PROJECT_ID, DB2_PASSWORD, and MYSQL_PASSWORD
* Run the integration tests for each database type as follows `pytest tests/system/data_sources/test_db2.py`, `pytest tests/system/data_sources/test_impala.py`, `pytest tests/system/data_sources/test_mysql.py`, `pytest tests/system/data_sources/test_spanner.py`

## Suggested Steps
* Run the tests as mentioned above
* Review list of failures to identify the cause. Sometimes multiple failures may be due to a single root cause. Fix the root cause and rerun the tests.
* Objective is to have all tests pass for a database type before moving to the next database type.
* When creating the migration report do not include the values of the environment variables I provide. Instead, use placeholders like `<DB2_PASSWORD>`,`<MYSQL_PASSWORD>`. 
