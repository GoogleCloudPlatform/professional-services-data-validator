# Instructions to run Teradata integration tests
##
Now that Postgres, SQL Server, BigQuery and Oracle integration tests are passing here are instructions to run the Teradata integration tests.
## How to run the integration tests
* You have to use the Python virtual environment by running `source env/bin/activate` from the shell.
* Prompt me to set the environment variables PROJECT_ID, TERADATA_HOST, and TERADATA_PASSWORD
* Run the integration tests as follows `pytest tests/system/data_sources/test_teradata.py`

## Suggested Steps
* Run the tests as mentioned above
* Review list of failures to identify the cause. Sometimes multiple failures may be due to a single root cause. Fix the root cause and rerun the tests.
* Objective is to have all tests pass before moving to the next database type.
* When creating the migration report do not include the values of the environment variables I provide. Instead, use placeholders like `<TERADATA_HOST>`, `<TERADATA_PASSWORD>`. 
