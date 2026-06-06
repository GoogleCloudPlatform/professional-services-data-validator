# Feedback and suggested next steps on migration of DVT to use ibis 7.1

##
You came up with the plan for migrating DVT to use ibis 7.1, which is detailed in docs/internal/migraton7.1/ibis_7.1_migration_plan.md. That plan was approved and you proceeded with the migration. The rest of this document is about integration test, their results and next steps.

## Observations

A review of the code found the following:
* Many of the database specific changes which were isolated in their own
  directories in third_party/ibis, have now been all moved to third_party/ibis/ibis_addon directory. It is unclear that this is a good choice. Let this remain as is for now.

After compiling the code and executing DVT the following was observed:
* Column Validation of a postgres table against itself succeeded.
* Row validation of a postgres table against itself failed with the error
  `AttributeError: module 'ibis.expr.types' has no attribute 'TemporalValue'`.
  Complete error details are shown below:
```text
data-validation validate row -tb
ls=pso_data_validator.dvt_core_types -hash='*' -pk=id -sc postgres -tc postgres
Traceback (most recent call last):
  File "/usr/local/google/home/mudupalli/professional-services-data-validator/env/bin/data-validation", line 8, in <module>
    sys.exit(main())
             ^^^^^^
  File "/usr/local/google/home/mudupalli/professional-services-data-validator/env/lib/python3.11/site-packages/data_validation/__main__.py", line 681, in main
    validate(args)
  File "/usr/local/google/home/mudupalli/professional-services-data-validator/env/lib/python3.11/site-packages/data_validation/__main__.py", line 657, in validate
    run(args)
  File "/usr/local/google/home/mudupalli/professional-services-data-validator/env/lib/python3.11/site-packages/data_validation/__main__.py", line 613, in run
    config_managers = build_config_managers_from_args(args)
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/local/google/home/mudupalli/professional-services-data-validator/env/lib/python3.11/site-packages/data_validation/__main__.py", line 359, in build_config_managers_from_args
    return util.timed_call("Build config", _build_configs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/local/google/home/mudupalli/professional-services-data-validator/env/lib/python3.11/site-packages/data_validation/util.py", line 32, in timed_call
    result = fn(*args, **kwargs)
             ^^^^^^^^^^^^^^^^^^^
  File "/usr/local/google/home/mudupalli/professional-services-data-validator/env/lib/python3.11/site-packages/data_validation/__main__.py", line 353, in _build_configs
    config_manager = build_config_from_args(args, config_manager)
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/local/google/home/mudupalli/professional-services-data-validator/env/lib/python3.11/site-packages/data_validation/__main__.py", line 325, in build_config_from_args
    config_manager.build_column_configs(primary_keys)
  File "/usr/local/google/home/mudupalli/professional-services-data-validator/env/lib/python3.11/site-packages/data_validation/config_manager.py", line 802, in build_column_configs
    source_table = self.get_source_ibis_calculated_table()
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/local/google/home/mudupalli/professional-services-data-validator/env/lib/python3.11/site-packages/data_validation/config_manager.py", line 443, in get_source_ibis_calculated_table
    vb = ValidationBuilder(self)
         ^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/local/google/home/mudupalli/professional-services-data-validator/env/lib/python3.11/site-packages/data_validation/validation_builder.py", line 67, in __init__
    self.add_config_calculated_fields()
  File "/usr/local/google/home/mudupalli/professional-services-data-validator/env/lib/python3.11/site-packages/data_validation/validation_builder.py", line 190, in add_config_calculated_fields
    self.add_calc(calc_field)
  File "/usr/local/google/home/mudupalli/professional-services-data-validator/env/lib/python3.11/site-packages/data_validation/validation_builder.py", line 453, in add_calc
    source_field = getattr(CalculatedField, source_calc_type)(
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/local/google/home/mudupalli/professional-services-data-validator/env/lib/python3.11/site-packages/data_validation/query_builder/query_builder.py", line 425, in custom
    return CalculatedField(eval(ibis_expr), config, fields, **params)
                           ^^^^^^^^^^^^^^^
  File "<string>", line 1, in <module>
AttributeError: module 'ibis.expr.types' has no attribute 'TemporalValue'
```
* Running the postgres integration test suite resulted in 40 tests that failed as shown below:
```text
FAILED tests/system/data_sources/test_postgres.py::test_postgres_count - AttributeError: module 'ibis.expr.api' has no attribute 'IntegerColumn'
FAILED tests/system/data_sources/test_postgres.py::test_postgres_row - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_generate_partitions - AttributeError: module 'ibis.expr.types' has no attribute 'TemporalValue'
FAILED tests/system/data_sources/test_postgres.py::test_schema_validation_pg_types - sqlalchemy.exc.ProgrammingError: (psycopg2.errors.SyntaxError) syntax error at or near "pso_d...
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_pg_types - sqlalchemy.exc.ProgrammingError: (psycopg2.errors.SyntaxError) syntax error at or near "pso_d...
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_core_types_to_bigquery - AttributeError: module 'ibis.expr.api' has no attribute 'IntegerColumn'
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_large_decimals_to_bigquery - AttributeError: module 'ibis.expr.api' has no attribute 'IntegerColumn'
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_large_decimals_to_bigquery_mismatch - AttributeError: module 'ibis.expr.api' has no attribute 'IntegerColumn'
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_view_core_types_vw - AttributeError: module 'ibis.expr.api' has no attribute 'IntegerColumn'
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_pg_types - sqlalchemy.exc.ProgrammingError: (psycopg2.errors.SyntaxError) syntax error at or near "pso_d...
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_core_types_auto_pks - AttributeError: 'Backend' object has no attribute 'list_primary_key_columns'
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_comp_fields_pg_types - sqlalchemy.exc.ProgrammingError: (psycopg2.errors.SyntaxError) syntax error at or near "pso_d...
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_large_decimals_to_bigquery - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_binary_pk_to_bigquery - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_comp_fields_binary_values_to_bigquery - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_fixed_char_pk_row_validation_to_bigquery - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_varchar_pk_row_validation_to_bigquery - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_fixed_char_pk_query_row_validation_to_bigquery - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_varchar_pk_query_row_validation_to_bigquery - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_datetime_pk_to_bigquery - AttributeError: module 'ibis.expr.types' has no attribute 'TemporalValue'
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_pangrams_to_bigquery - AttributeError: module 'ibis.expr.api' has no attribute 'IntegerColumn'
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_pangrams_to_bigquery - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_find_tables - AssertionError
FAILED tests/system/data_sources/test_postgres.py::test_find_views_and_tables - AssertionError
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_many_columns_standard - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_many_columns_exclude - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_custom_query_row_validation_many_columns - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_group_by_timestamp - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_high_epoch_seconds - AttributeError: module 'ibis.expr.api' has no attribute 'IntegerColumn'
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_tricky_dates_to_bigquery - AttributeError: module 'ibis.expr.api' has no attribute 'IntegerColumn'
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_identifiers - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_reserved_words - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_comp_fields_reserved_words - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_tricky_dates_to_bigquery - AttributeError: module 'ibis.expr.types' has no attribute 'TemporalValue'
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_comp_fields_tricky_dates_to_bigquery - AttributeError: Can only use .dt accessor with datetimelike values
FAILED tests/system/data_sources/test_postgres.py::test_row_validation_tricky_strings_to_bigquery - ValueError: too many values to unpack (expected 1)
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_decimals_no_precision - AttributeError: module 'ibis.expr.api' has no attribute 'IntegerColumn'
FAILED tests/system/data_sources/test_postgres.py::test_schema_validation_intervals - AttributeError: 'NoneType' object has no attribute 'upper'
FAILED tests/system/data_sources/test_postgres.py::test_column_validation_intervals - AttributeError: 'NoneType' object has no attribute 'upper'
FAILED tests/system/data_sources/test_postgres.py::test_raw_column_metadata - AttributeError: 'Backend' object has no attribute 'raw_column_metadata'
```

## Suggested next steps

* Critically evaluate if the suggested steps are consistent with good software engineering practices.
* Note that you have to use the virtual environment by source
  ~/professional-services-data-validator/env/bin/activate to run data-validation
* Categorize the tests that failed and the errors that were generated.
* Identify a plan to fix the errors, with the simplest errors first.
* Identify code changes, perform the changes and verify that the error is fixed by running pytest again.
* Report the results of every error fixed or attempted, allowing me to interrupt the process if you are going down the wrong path.
