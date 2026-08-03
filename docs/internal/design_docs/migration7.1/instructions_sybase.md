# Instructions to enable Sybase backend
##
A number of backends have been ported and the integration tests for those backends pass. We now need to port the Sybase backend.

## How to run the integration tests
* You have to use the Python virtual environment by running `source env/bin/activate` from the shell.
* Prompt me to set the environment variable PROJECT_ID, SYBASE_PASSWORD, and SYBASE_USER.
* Run the integration tests as follows `pytest tests/system/data_sources/test_sybase.py`

## Suggested Steps
* Run the tests as mentioned above
* Review list of failures to identify the cause. Sometimes multiple failures may be due to a single root cause. Fix the root cause and rerun the tests.
* Objective is to have all tests pass before moving to the next database type.
