# Data Validation Tool

The Data Validation Tool (Beta) is an open sourced Python CLI tool based on the
[Ibis framework](https://ibis-project.org/docs/tutorial/01-Introduction-to-Ibis.html)
that compares heterogeneous data source tables with multi-leveled validation
functions.

Data validation is a critical step in a Data Warehouse, Database or Data Lake
migration project, where structured or semi-structured data from both the source
and the destination tables are compared to ensure they are matched and correct
after each migration step (e.g. data and schema migration, SQL script
translation, ETL migration, etc.). The Data Validation Tool (DVT) provides an
automated and repeatable solution to perform this task.

DVT supports the following validation types: * Table level * Table row count *
Group by row count * Column aggregation * Filters and limits * Column level *
Full column data type * Row level hash comparison (BigQuery tables only) * Raw
SQL exploration * Run custom queries on different data sources

DVT supports the following connection types:

*   [BigQuery](docs/connections.md#google-bigquery)
*   [Spanner](docs/connections.md#google-spanner)
*   [Teradata](docs/connections.md#teradata)
*   [Oracle](docs/connections.md#oracle)
*   [MSSQL](docs/connections.md#mssql-server)
*   [Snowflake](docs/connections.md#snowflake)
*   [Postgres](docs/connections.md#postgres)
*   [MySQL](docs/connections.md#mysql)
*   [Redshift](docs/connections.md#redshift)
*   [FileSystem](docs/connections.md#filesystem)
*   [Impala](docs/connections.md#impala)
*   [Hive](docs/connections.md#hive)

The [Connections](docs/connections.md) page provides details about how to create
and list connections for the validation tool.

## Installation

The [Installation](docs/installation.md) page describes the prerequisites and
setup steps needed to install and use the data validation tool.

## Usage

Before using this tool, you will need to create connections to the source and
target tables. Once the connections are created, you can run validations on
those tables. Validation results can be printed to stdout (default) or outputted
to BigQuery. The validation tool also allows you to save or edit validation
configurations in a YAML file. This is useful for running common validations or
updating the configuration.

### Managing Connections

The Data Validation Tool expects to receive a source and target connection for
each validation which is run.

These connections can be supplied directly to the configuration, but more often
you want to manage connections separately and reference them by name.

Connections can be stored locally or in a GCS directory.

To create connections please review the [Connections](docs/connections.md) page.

### Running CLI Validations

The data validation CLI is a main interface to use this tool.

The CLI has several different commands which can be used to create and re-run
validations.

The validation tool first expects connections to be created before running
validations. To create connections please review the
[Connections](docs/connections.md) page.

Once you have your connections set up, you are ready to run the validations.

### Validation command syntax and options

Below are the command syntax and options for running validations from the CLI.
DVT supports column (including grouped column) and schema validations.

#### Column Validations

Below is the command syntax for column validations. To run a grouped column
validation, simply specify the `--grouped-columns` flag. You can also take
grouped column validations a step further by providing the `--primary-key` flag.
With this flag, if a mismatch was found, DVT will dive deeper into the slice
with the error and find the row (primary key value) with the inconsistency.

```
data-validation (--verbose or -v) validate column
  --source-conn or -sc SOURCE_CONN
                        Source connection details
                        See: *Data Source Configurations* section for each data source
  --target-conn or -tc TARGET_CONN
                        Target connection details
                        See: *Connections* section for each data source
  --tables-list or -tbls SOURCE_SCHEMA.SOURCE_TABLE=TARGET_SCHEMA.TARGET_TABLE
                        Comma separated list of tables in the form schema.table=target_schema.target_table
                        Target schema name and table name are optional.
                        i.e 'bigquery-public-data.new_york_citibike.citibike_trips'
  [--grouped-columns or -gc GROUPED_COLUMNS]
                        Comma separated list of columns for Group By i.e col_a,col_b
  [--primary-keys or -pk PRIMARY_KEYS]
                        Comma separated list of columns to use as primary keys
                        (Note) Only use with grouped column validation
  [--count COLUMNS]     Comma separated list of columns for count or * for all columns
  [--sum COLUMNS]       Comma separated list of columns for sum or * for all numeric
  [--min COLUMNS]       Comma separated list of columns for min or * for all numeric
  [--max COLUMNS]       Comma separated list of columns for max or * for all numeric
  [--avg COLUMNS]       Comma separated list of columns for avg or * for all numeric
  [--bq-result-handler or -bqrh PROJECT_ID.DATASET.TABLE]
                        BigQuery destination for validation results. Defaults to stdout.
                        See: *Validation Reports* section
  [--service-account or -sa PATH_TO_SA_KEY]
                        Service account to use for BigQuery result handler output.
  [--filters SOURCE_FILTER:TARGET_FILTER]
                        Colon separated string values of source and target filters.
                        If target filter is not provided, the source filter will run on source and target tables.
                        See: *Filters* section
  [--config-file or -c CONFIG_FILE]
                        YAML Config File Path to be used for storing validations.
  [--threshold or -th THRESHOLD]
                        Float value. Maximum pct_difference allowed for validation to be considered a success. Defaults to 0.0
  [--labels or -l KEY1=VALUE1,KEY2=VALUE2]
                        Comma-separated key value pair labels for the run.
  [--format or -fmt]    Format for stdout output. Supported formats are (text, csv, json, table).
                        Defaults to table.
```

The default aggregation type is a 'COUNT *'. If no aggregation flag (i.e count,
sum , min, etc.) is provided, the default aggregation will run.

The [Examples](docs/examples.md) page provides many examples of how a tool can
used to run powerful validations without writing any queries.

#### Row Validations

(Note: Row hash validation is currently only supported for BigQuery, Teradata, and Imapala/Hive)

Below is the command syntax for row validations. In order to run row level
validations you need to pass a `--primary-key` flag which defines what field(s)
the validation will be compared on, as well as either the `--comparison-fields` flag
or the `--hash` flag.

The `--comparison-fields` flag specifies the values (e.g. columns) whose raw values will be compared
based on the primary key join. The `--hash` flag will run a checksum across all columns in
the table. This will include casting to string, sanitizing the data, concatenating, and finally
hashing the row. To exclude columns from the checksum, use the YAML config to customize the validation.


Additionally you can use
[Calculated Fields](#calculated-fields) to compare derived values such as string
counts and hashes of multiple columns.

```
data-validation (--verbose or -v) validate row
  --source-conn or -sc SOURCE_CONN
                        Source connection details
                        See: *Data Source Configurations* section for each data source
  --target-conn or -tc TARGET_CONN
                        Target connection details
                        See: *Connections* section for each data source
  --tables-list or -tbls SOURCE_SCHEMA.SOURCE_TABLE=TARGET_SCHEMA.TARGET_TABLE
                        Comma separated list of tables in the form schema.table=target_schema.target_table
                        Target schema name and table name are optional.
                        i.e 'bigquery-public-data.new_york_citibike.citibike_trips'
  --primary-keys or -pk PRIMARY_KEYS
                        Comma separated list of columns to use as primary keys
  --comparison-fields or -comp-fields FIELDS
                        Comma separated list of columns to compare. Can either be a physical column or an alias
                        See: *Calculated Fields* section for details
  --hash '*'            '*' to hash all columns. To exclude columns, use the YAML config.
  [--bq-result-handler or -bqrh PROJECT_ID.DATASET.TABLE]
                        BigQuery destination for validation results. Defaults to stdout.
                        See: *Validation Reports* section
  [--service-account or -sa PATH_TO_SA_KEY]
                        Service account to use for BigQuery result handler output.
  [--filters SOURCE_FILTER:TARGET_FILTER]
                        Colon spearated string values of source and target filters.
                        If target filter is not provided, the source filter will run on source and target tables.
                        See: *Filters* section
  [--config-file or -c CONFIG_FILE]
                        YAML Config File Path to be used for storing validations.
  [--labels or -l KEY1=VALUE1,KEY2=VALUE2]
                        Comma-separated key value pair labels for the run.
  [--format or -fmt]    Format for stdout output. Supported formats are (text, csv, json, table).
                        Defaults to table.
```

#### Schema Validations

Below is the syntax for schema validations. These can be used to compare column
types between source and target.

```
data-validation (--verbose or -v) validate schema
  --source-conn or -sc SOURCE_CONN
                        Source connection details
                        See: *Data Source Configurations* section for each data source
  --target-conn or -tc TARGET_CONN
                        Target connection details
                        See: *Connections* section for each data source
  --tables-list or -tbls SOURCE_SCHEMA.SOURCE_TABLE=TARGET_SCHEMA.TARGET_TABLE
                        Comma separated list of tables in the form schema.table=target_schema.target_table
                        Target schema name and table name are optional.
                        i.e 'bigquery-public-data.new_york_citibike.citibike_trips'
  [--bq-result-handler or -bqrh PROJECT_ID.DATASET.TABLE]
                        BigQuery destination for validation results. Defaults to stdout.
                        See: *Validation Reports* section
  [--service-account or -sa PATH_TO_SA_KEY]
                        Service account to use for BigQuery result handler output.
  [--config-file or -c CONFIG_FILE]
                        YAML Config File Path to be used for storing validations.
  [--format or -fmt]    Format for stdout output. Supported formats are (text, csv, json, table).
                        Defaults  to table.
```

### Running Custom SQL Exploration

There are many occasions where you need to explore a data source while running
validations. To avoid the need to open and install a new client, the CLI allows
you to run custom queries.

```
data-validation query
  --conn or -c CONN
          The connection name to be queried
  --query or -q QUERY
          The raw query to run against the supplied connection
```

### Using Beta CLI Features

There may be occasions we want to release a new CLI feature under a Beta flag.
Any features under Beta may or may not make their way to production. However, if
there is a Beta feature you wish to use than it can be accessed using the
following.

```
data-validation beta --help
```

#### [Beta] Deploy Data Validation as a Local Service

If you wish to use Data Validation as a Flask service, the following command
will help. This same logic is also expected to be used for Cloud Run, Cloud
Functions, and other deployment services.

`data-validation beta deploy`

## Query Configurations

You can customize the configuration for any given validation by providing use
case specific CLI arguments or editing the saved YAML configuration file.

For example, the following command creates a YAML file for the validation of the
`new_york_citibike` table: `data-validation validate column -sc my_bq_conn -tc
my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_trips -c
citibike.yaml`. 

The vaildation config file is saved to the GCS path specified by the `PSO_DV_CONFIG_HOME` env variable if that has been set; otherwise, it is saved to wherever the tool is run. 

Here is the generated YAML file named `citibike.yaml`:

```
result_handler: {}
source: my_bq_conn
target: my_bq_conn
validations:
- aggregates:
  - field_alias: count
    source_column: null
    target_column: null
    type: count
  filters: []
  labels: []
  schema_name: bigquery-public-data.new_york_citibike
  table_name: citibike_trips
  target_schema_name: bigquery-public-data.new_york_citibike
  target_table_name: citibike_trips
  threshold: 0.0
  type: Column
```

You can now edit the YAML file if, for example, the `new_york_citibike` table is
stored in datasets that have different names in the source and target systems.
Once the file is updated and saved, the following command runs the new
validation:

```
data-validation configs run -c citibike.yaml
```

View the complete YAML file for a GroupedColumn validation on the
[examples](docs/examples.md#) page.

You can view a list of all saved validation YAML files using `data-validation configs list`, and print a YAML config using `data-validation configs get -c citibike.yaml`. 

### Aggregated Fields

Aggregate fields contain the SQL fields that you want to produce an aggregate
for. Currently the functions `COUNT()`, `AVG()`, `SUM()`, `MIN()` and `MAX()`
are supported.

#### Sample Aggregate Config

```
validations:
- aggregates:
    - field_alias: count
    source_column: null
    target_column: null
    type: count
    - field_alias: count__tripduration
    source_column: tripduration
    target_column: tripduration
    type: count
    - field_alias: sum__tripduration
    source_column: tripduration
    target_column: tripduration
    type: sum
  - field_alias: bit_xor__hashed_column
    source_column: hashed_column
    target_column: hashed_column
    type: bit_xor
```

### Filters

Filters let you apply a WHERE statement to your validation query (ie. `SELECT *
FROM table WHERE created_at > 30 days ago AND region_id = 71;`). The filter is
written in the syntax of the given source.

Note that you are writing the query to execute, which does not have to match
between source and target as long as the results can be expected to align. If
the target filter is omitted, the source filter will run on both the source and
target tables.

### Grouped Columns

Grouped Columns contain the fields you want your aggregations to be broken out
by, e.g. `SELECT last_updated::DATE, COUNT(*) FROM my.table` will produce a
resultset that breaks down the count of rows per calendar date.

### Comparison Fields

For row validations you need to specify the specific columns that you want to
compare. These values will be compared via a JOIN on their corresponding primary
key and will be evaluated for an exact match.

### Calculated Fields

Sometimes direct comparisons are not feasible between databases due to
differences in how particular data types may be handled. These differences can
be resolved by applying functions to columns in the source query itself.
Examples might include trimming whitespace from a string, converting strings to
a single case to compare case insensitivity, or rounding numeric types to a
significant figure.

Once a calculated field is defined, it can be referenced by other calculated
fields at any "depth" or higher. Depth controls how many subqueries are executed
in the resulting query. For example, with the following yaml config...

```
- calculated_fields:
    - field_alias: rtrim_col_a
      source_calculated_columns: ['col_a']
      target_calculated_columns: ['col_a']
      type: rtrim
      depth: 0 # generated off of a native column
    - field_alias: ltrim_col_a
      source_calculated_columns: ['col_b']
      target_calculated_columns: ['col_b']
      type: ltrim
      depth: 0 # generated off of a native column
    - field_alias: concat_col_a_col_b
      source_calculated_columns: ['rtrim_col_a', 'ltrim_col_b']
      target_calculated_columns: ['rtrim_col_a', 'ltrim_col_b']
      type: concat
      depth: 1 # calculated one query above
```

is equivalent to the following SQL query...

```
SELECT
  CONCAT(rtrim_col_a, rtrim_col_b) AS concat_col_a_col_b
FROM (
  SELECT
      RTRIM(col_a) AS rtrim_col_a
    , LTRIM(col_b) AS ltrim_col_b
  FROM my.table
  ) as table_0
```

Calculated fields can be used by aggregate fields to produce validations on
calculated or sanitized raw data, such as calculating the aggregate hash of a
table. For example the following yaml config...

```
validations:
- aggregates:
  - field_alias: xor__multi_statement_hash
    source_column: multi_statement_hash
    target_column: multi_statement_hash
    type: bit_xor
  calculated_fields:
  - field_alias: multi_statement_hash
    source_calculated_columns: [multi_statement_concat]
    target_calculated_columns: [multi_statement_concat]
    type: hash
    depth: 2
  - field_alias: multi_statement_concat
    source_calculated_columns: [calc_length_col_a,
                                calc_ifnull_col_b,
                                calc_rstrip_col_c,
                                calc_upper_col_d]
    target_calculated_columns: [calc_length_col_a,
                                calc_ifnull_col_b,
                                calc_rstrip_col_c,
                                calc_upper_col_d]
    type: concat
    depth: 1
  - field_alias: calc_length_col_a
    source_calculated_columns: [col_a]
    target_calculated_columns: [col_a]
    type: length
    depth: 0
  - field_alias: calc_ifnull_col_b
    source_calculated_columns: [col_b]
    target_calculated_columns: [col_b]
    type: ifnull
    depth: 0
  - field_alias: calc_rstrip_col_c
    source_calculated_columns: [col_c]
    target_calculated_columns: [col_c]
    type: rstrip
    depth: 0
  - field_alias: calc_upper_col_d
    source_calculated_columns: [col_d]
    target_calculated_columns: [col_d]
    type: upper
    depth: 0
```

is equivalent to the following SQL query...

```
SELECT
  BIT_XOR(multi_statement_hash) AS xor__multi_statement_hash
FROM (
  SELECT
    FARM_FINGERPRINT(mult_statement_concat) AS multi_statement_hash
  FROM (
    SELECT
      CONCAT(calc_length_col_a,
             calc_ifnull_col_b,
             calc_rstrip_col_c,
             calc_upper_col_d) AS multi_statement_concat
    FROM (
      SELECT
          CAST(LENGTH(col_a) AS STRING) AS calc_length_col_a
        , IFNULL(col_b,
                 'DEFAULT_REPLACEMENT_STRING') AS calc_ifnull_col_b
        , RTRIM(col_c) AS calc_rstrip_col_c
        , UPPER(col_d) AS calc_upper_col_d
      FROM my.table
      ) AS table_0
    ) AS table_1
  ) AS table_2
```

## Validation Reports

The output handlers tell the data validation tool where to store the results of
each validation. The tool can write the results of a validation run to Google
BigQuery or print to stdout (default).

View the schema of the results [here](terraform/results_schema.json).

### Configure tool to output to BigQuery

```
data-validation validate column
  -sc bq_conn
  -tc bq_conn
  -tbls bigquery-public-data.new_york_citibike.citibike_trips
  -bqrh project_id.dataset.table
  -sa service-acct@project.iam.gserviceaccount.com
```

## Building Matched Table Lists

Creating the list of matched tables can be a hassle. We have added a feature
which may help you to match all of the tables together between source and
target. The find-tables tool:

-   Pulls all tables in the source (applying a supplied allowed-schemas filter)
-   Pulls all tables from the target
-   Uses Levenshtein distance to match tables
-   Finally, it prints a JSON list of tables which can be a reference for the
    validation run config.

Note that our score cutoff default is a 0.8, which was manually tested to be an
accurate value. If no matches occur, reduce this value.

```
data-validation find-tables --source-conn source --target-conn target \
    --allowed-schemas pso_data_validator \
    --score-cutoff 0.8
```

## Add Support for an existing Ibis Data Source

If you want to add an Ibis Data Source which exists, but was not yet supported
in the Data Validation tool, it is a simple process.

1.  In data_validation/data_validation.py

    -   Import the extended Client for the given source (ie. from
        ibis.sql.mysql.client import MySQLClient).
    -   Add the "<RefName>": Client to the global CLIENT_LOOKUP dictionary.

2.  In third_party/ibis/ibis_addon/operations.py

    -   Add the RawSQL operator to the data source registry (for custom filter
        support).

3.  You are done, you can reference the data source via the config.

    -   Config: {"source_type": "<RefName>", ...KV Values required in Client...}

## Deploy to Composer

```
#!/bin/bash

export COMPOSER_ENV=""
export LOCATION=""

echo "Creating Composer Env: $COMPOSER_ENV"
gcloud services enable composer.googleapis.com
gcloud composer environments create $COMPOSER_ENV --location=$LOCATION --python-version=3

echo "Updating Composer Env Reqs: $COMPOSER_ENV"
# Composer builds Pandas and BigQuery for you, these should be stripped out
cat requirements.txt | grep -v pandas | grep -v google-cloud-bigquery > temp_reqs.txt
gcloud composer environments update $COMPOSER_ENV --location=$LOCATION --update-pypi-packages-from-file=temp_reqs.txt
rm temp_reqs.txt

# Deploy Package to Composer (the hacky way)
echo "Rebuilding Data Validation Package in GCS"
export GCS_BUCKET_PATH=$(gcloud composer environments describe $COMPOSER_ENV --location=$LOCATION | grep dagGcsPrefix | awk '{print $2;}')
gsutil rm -r $GCS_BUCKET_PATH/data_validation
gsutil cp -r data_validation $GCS_BUCKET_PATH/data_validation

# Deploy Test DAG to Composer
echo "Pushing Data Validation Test Operator to GCS"
gsutil cp tests/test_data_validation_operators.py $GCS_BUCKET_PATH/
```

## Contributing

Contributions are welcome. See the [contributing guide](CONTRIBUTING.md) for
details.
