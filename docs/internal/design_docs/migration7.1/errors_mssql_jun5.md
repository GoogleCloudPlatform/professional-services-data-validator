# Feedback on changes made for Postgres suggested next steps
##
Now that Postgres tests are succeeding, I looked at the MSSQL integration tests and found that many tests are failing.
## Observations

* 38 out of the 46 tests failed. The specific failures were:
```text
FAILED tests/system/data_sources/test_sql_server.py::test_sql_server_row - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_generate_partitions - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_schema_validation_core_types_to_bigquery - AssertionError
FAILED tests/system/data_sources/test_sql_server.py::test_schema_validation_ss_types_to_bigquery - AssertionError
FAILED tests/system/data_sources/test_sql_server.py::test_column_validation_core_types - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_column_validation_core_types_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_column_validation_ss_types_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_column_validation_tricky_dates_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_column_validation_large_decimals_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_column_validation_large_decimals_to_bigquery_mismatch - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_column_validation_camel_case - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_column_validation_identifiers - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_column_validation_reserved_words - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_column_validation_uuid_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_core_types - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_core_types_auto_pks - AttributeError: 'Backend' object has no attribute 'list_primary_key_columns'
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_core_types_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_comp_fields_ss_types_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_ss_types_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_large_decimals_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_binary_pk_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_comp_fields_binary_values_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_datetime_pk_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_tricky_dates_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_comp_fields_tricky_dates_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_many_columns - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_identifiers - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_reserved_words - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_comp_fields_reserved_words - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_uuid_hash_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_camel_case_auto_pk - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_row_validation_camel_case_pk_option - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_custom_query_column_validation_core_types_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_custom_query_row_validation_core_types_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_custom_query_row_hash_validation_core_types_to_bigquery - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_custom_query_row_validation_many_columns - NameError: name 'get_sqla_table' is not defined
FAILED tests/system/data_sources/test_sql_server.py::test_find_tables - AssertionError
FAILED tests/system/data_sources/test_sql_server.py::test_connections_add - data_validation.exceptions.DataClientConnectionFailure: Connection Type "MSSQL" could not connect: Backend.do_connect(...
```

## Suggested next steps

* Note that you have to use the virtual environment by source
  ~/professional-services-data-validator/env/bin/activate to run data-validation
* You can run the test using `pytest tests/system/data_sources/test_sql_server.py` to run all tests or specify the test name to run a specific test.  Make sure you prompt me for the values of the PROJECT_ID and the SQL_SERVER_PASSWORD environment variables before running the tests.
Categorize the tests that failed and the errors that were generated.
* Identify a plan to fix the errors, with the simplest errors first.
* Identify code changes, perform the changes and verify that the error is fixed by running pytest again.
* Report the results of every error fixed or attempted, allowing me to interrupt the process if you are going down the wrong path.
