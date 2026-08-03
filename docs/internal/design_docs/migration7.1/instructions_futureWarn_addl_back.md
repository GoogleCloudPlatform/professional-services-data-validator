# Instructions to run fix FutureWarning and enable additional backends
##
A number of backends have been ported and the integration tests for those backends pass. We now need to port the Filesystem, and Hive backends.
When tests are being run, the following FutureWarnings are reported. Please fix them as indicated.
```
data_validation/clients.py:262: FutureWarning: `Table.relabel` is deprecated as of v7.0; use `Table.rename` instead (if passing a mapping, note the meaning of keys and values are swapped in Table.rename).
data_validation/clients.py:299: FutureWarning: `Backend.list_databases` is deprecated as of v6.1.0, removed in v8.0.0; use `list_schemas()`
tests/unit/test_combiner.py:693: FutureWarning: Mismatched null-like values nan and None found. In a future version, pandas equality-testing functions (e.g. assert_frame_equal) will consider these not-matching and raise.
third_party/ibis/ibis_addon/operations.py:214: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_addon/operations.py:299: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_addon/operations.py:425: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_addon/operations.py:485: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_addon/operations.py:525: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_addon/operations.py:526: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_addon/operations.py:553: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_addon/operations.py:589: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_cloud_spanner/registry.py:31: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_db2/registry.py:129: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_db2/registry.py:355: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_db2/registry.py:476: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_teradata/registry.py:144: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_teradata/registry.py:172: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_teradata/registry.py:173: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_teradata/registry.py:332: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
third_party/ibis/ibis_teradata/registry.py:352: FutureWarning: `Value.output_dtype` is deprecated as of v7.0; use .dtype property instead
```

## How to run the integration tests
* You have to use the Python virtual environment by running `source env/bin/activate` from the shell.
* Prompt me to set the environment variable PROJECT_ID
* Run the integration tests as follows `pytest tests/system/data_sources/test_filesystem.py` and`pytest tests/system/data_sources/test_hive.py`

## Suggested Steps
* Address the FutureWarnings first. The warnings are self-explanatory and you should be able to fix them. If unsure, prompt me for guidance. 
* Run the tests as mentioned above
* Review list of failures to identify the cause. Sometimes multiple failures may be due to a single root cause. Fix the root cause and rerun the tests.
* Objective is to have all tests pass before moving to the next database type.
