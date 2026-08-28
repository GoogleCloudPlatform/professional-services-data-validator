# Instructions to fix errors in Oracle
##
I have tried to fix issue 1702 by making changes to data_validation/combiner.py. While mostly successful, I ran into some issues which I want you to review and fix.

* All code changes so far have been checked in.
* Oracle Integration tests fail with the following two tests failing (see error messages) - these need to be fixed
```
FAILED tests/system/data_sources/test_oracle.py::test_column_validation_all_null_oracle_to_postgres - ibis.common.exceptions.IbisTypeError: Arguments differences_targ...
FAILED tests/system/data_sources/test_oracle.py::test_row_validation_comp_fields_oracle_to_postgres - sqlalchemy.exc.DataError: (duckdb.duckdb.ConversionException) Co...
```
* BigQuery tests fail - pytest tests/system/data_sources/test_bigquery.py - these need to be fixed