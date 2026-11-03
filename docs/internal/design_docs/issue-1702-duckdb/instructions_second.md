# Instructions to fix errors in Oracle
##
I have tried to fix issue 1702 by making changes to data_validation/combiner.py. While mostly successful, I ran into some issues which I want you to review and fix.

* All code changes so far have been checked in.
* Oracle Integration test (python3 -m nox -s integration_oracle) fails with the following two tests failing (see error messages) - these need to be fixed
```
FAILED tests/system/data_sources/test_oracle.py::test_column_validation_all_null_oracle_to_postgres - ibis.common.exceptions.IbisTypeError: Arguments differences_targ...
FAILED tests/system/data_sources/test_oracle.py::test_row_validation_comp_fields_oracle_to_postgres - sqlalchemy.exc.DataError: (duckdb.duckdb.ConversionException) Co...
```
* SQL Server integration test (python3 -m nox -s integration_sql_server) fails with the error message - this needs to be fixed
```
FAILED tests/system/data_sources/test_sql_server.py::test_column_validation_decimals_to_bigquery - sqlalchemy.exc.OperationalError: (pyodbc.OperationalError) ('08S01', '[08S01] [Microsoft][ODBC Driver 17...
```
* Fix the above with the minimal code changes.