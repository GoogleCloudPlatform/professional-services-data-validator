# Instructions to fix errors in Snowflake integration tests
##
We need to fix the errors reported by Snowflake integration tests. The errors are shown below
```
FAILED tests/system/data_sources/test_snowflake.py::test_schema_validation_specific_types - TypeError: Unable to convert type: VARBINARY(length=8388608)
FAILED tests/system/data_sources/test_snowflake.py::test_schema_validation_core_types_to_bigquery - AssertionError
FAILED tests/system/data_sources/test_snowflake.py::test_schema_validation_not_null_vs_nullable - AssertionError: Column: col_nn, status: fail != success
FAILED tests/system/data_sources/test_snowflake.py::test_row_validation_core_types_auto_pks - AttributeError: 'Backend' object has no attribute 'list_primary_key_columns'
FAILED tests/system/data_sources/test_snowflake.py::test_row_validation_binary_pk_to_bigquery - TypeError: Unable to convert type: VARBINARY(length=8388608)
FAILED tests/system/data_sources/test_snowflake.py::test_row_validation_comp_fields_binary_values_to_bigquery - TypeError: Unable to convert type: VARBINARY(length=8388608)
```

## How to run the Snowflake integration tests
* You have to use the Python virtual environment by running `source env71/bin/activate` from the shell.
* Prompt me to set the environment variables PROJECT_ID, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD and SNOWFLAKE_ACCOUNT
* Run the integration tests as follows `pytest tests/system/data_sources/test_snowflake.py`

## Suggested Steps
* Run the tests as mentioned above
* Review list of failures to identify the cause. Sometimes multiple failures may be due to a single root cause. Fix the root cause and rerun the tests.
* Objective is to have all tests pass.
