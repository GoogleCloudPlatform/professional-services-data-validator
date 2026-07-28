# Feedback on changes made on Jun 4 and suggested next steps
##
You made significant progress on the reducing the number of errors in the integration tests. However, you did not follow the plan I suggested in errors_jun_4.md.
## Observations

* Your changes resulted in fewer postgres integration tests failing, i.e. 17 now from 40 earlier and is good progress. The specific failures were:
```text
FAILED tests/system/data_sources/test_postgres.py::test_schema_validation_pg_types - sqlalchemy.exc.ProgrammingError: (psycopg2.errors.SyntaxError) syntax error at or near "pso_data...
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_pg_types - sqlalchemy.exc.ProgrammingError: (psycopg2.errors.SyntaxError) syntax error at or near "pso_data...
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_core_types_to_bigquery - google.api_core.exceptions.BadRequest: 400 No matching signature for function UNIX_SECONDS
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_pg_types - sqlalchemy.exc.ProgrammingError: (psycopg2.errors.SyntaxError) syntax error at or near "pso_data...
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_core_types_auto_pks - AttributeError: 'Backend' object has no attribute 'list_primary_key_columns'
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_comp_fields_pg_types - sqlalchemy.exc.ProgrammingError: (psycopg2.errors.SyntaxError) syntax error at or near "pso_data...
FAILED tests/system/data_sources/test_postgres.py::test_fixed_char_pk_row_validation_to_bigquery - AssertionError: Not all rows have status 'success'
FAILED tests/system/data_sources/test_postgres.py::test_fixed_char_pk_query_row_validation_to_bigquery - AssertionError: We expect 5 rows with status success from this validation
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_datetime_pk_to_bigquery - KeyError: <class 'ibis.expr.datatypes.core.Timestamp'>
FAILED tests/system/data_sources/test_postgres.py::test_find_tables - AssertionError
FAILED tests/system/data_sources/test_postgres.py::test_find_views_and_tables - AssertionError
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_tricky_dates_to_bigquery - AttributeError: Can only use .dt accessor with datetimelike values
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_tricky_dates_to_bigquery - KeyError: <class 'ibis.expr.datatypes.core.Date'>
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_comp_fields_tricky_dates_to_bigquery - AttributeError: Can only use .dt accessor with datetimelike values
FAILED tests/system/data_sources/test_postgres.py::test_schema_validation_intervals - AttributeError: 'NoneType' object has no attribute 'upper'
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_intervals - AttributeError: 'NoneType' object has no attribute 'upper'
FAILED tests/system/data_sources/test_postgres.py::test_raw_column_metadata - AttributeError: 'Backend' object has no attribute 'raw_column_metadata'
```

## Suggested next steps

* Critically evaluate if these steps are consistent with good software engineering practices.
* Note that you have to use the virtual environment by source
  ~/professional-services-data-validator/env/bin/activate to run data-validation
* You can run the test using `pytest tests/system/data_sources/test_postgres.py` to run all tests or specify the test name to run a specific test.  Make sure you prompt me for the values of the PROJECT_ID and the POSTGRES_PASSWORD environment variables before running the tests.
Categorize the tests that failed and the errors that were generated.
* Identify a plan to fix the errors, with the simplest errors first.
* Identify code changes, perform the changes and verify that the error is fixed by running pytest again.
* Report the results of every error fixed or attempted, allowing me to interrupt the process if you are going down the wrong path.
