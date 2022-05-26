# Data Validation Tool

The Data Validation Tool is an open sourced Python CLI tool based on the
[Ibis framework](https://ibis-project.org/docs/3.0.2/)
that compares heterogeneous data source tables with multi-leveled validation
functions.

Data validation is a critical step in a data warehouse, database, or data lake
migration project where data from both the source and the target tables are
compared to ensure they are matched and correct after each migration step
(e.g. data and schema migration, SQL script translation, ETL migration, etc.).
The Data Validation Tool (DVT) provides an automated and repeatable solution to
perform this task.

DVT supports the following validations:
* Column validation (count, sum, avg, min, max, group by)
* Row validation (BQ, Hive, and Teradata only)
* Schema validation 
* Custom Query validation
* Ad hoc SQL exploration

DVT supports the following connection types:

*   [BigQuery](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#google-bigquery)
*   [DB2](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#db2)
*   [FileSystem](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#filesystem)
*   [Hive](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#hive)
*   [Impala](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#impala)
*   [MSSQL](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#mssql-server)
*   [MySQL](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#mysql)
*   [Oracle](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#oracle)
*   [Postgres](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#postgres)
*   [Redshift](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#redshift)
*   [Snowflake](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#snowflake)
*   [Spanner](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#google-spanner)
*   [Teradata](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md#teradata)

The [Connections](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md) page provides details about how to create
and list connections for the validation tool.

### Disclaimer
This is not an officially supported Google product. Please be aware that bugs may lurk, and that we reserve the right to make small backwards-incompatible changes. Feel free to open bugs or feature requests, or contribute directly 
(see [CONTRIBUTING.md](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/CONTRIBUTING.md) for details).

## Installation

The [Installation](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/installation.md) page describes the prerequisites and
setup steps needed to install and use the Data Validation Tool.

## Usage

Before using this tool, you will need to create connections to the source and
target tables. Once the connections are created, you can run validations on
those tables. Validation results can be printed to stdout (default) or outputted
to BigQuery (recommended). DVT also allows you to save or edit validation
configurations in a YAML file. This is useful for running common validations or
updating the configuration.

### Managing Connections

Before running validations, DVT requires setting up a source and target connection.
These connections can be stored locally or in a GCS directory. To create connections,
please review the [Connections](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/connections.md) page.

### Running Validations

The CLI is the main interface to use this tool and it has several different
commands which can be used to create and run validations. Below are the command
syntax and options for running validations.

Alternatives to running DVT in the CLI include deploying DVT to Cloud Run, Cloud Functions, or Airflow
([Examples Here](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/develop/samples)). See the [Validation Logic](https://github.com/GoogleCloudPlatform/professional-services-data-validator#validation-logic) section
to learn more about how DVT uses the CLI to generate SQL queries.

#### Column Validations

Below is the command syntax for column validations. To run a grouped column
validation, simply specify the `--grouped-columns` flag.

You can specify a list of string columns for aggregations in order to calculate
an aggregation over the `length(string_col)`. Running an aggregation 
over all columns ('*') will only run over numeric columns, unless the
`--wildcard-include-string-len` flag is present.

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
  [--wildcard-include-string-len or -wis]
                        If flag is present, include string columns in aggregation as len(string_col)
  [--cast-to-bigint or -ctb]
                        If flag is present, cast all int32 columns to int64 before aggregation
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

The [Examples](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/examples.md) page provides many examples of how a tool can be used to run powerful validations without writing any queries.

#### Row Validations

(Note: Row hash validation is currently only supported for BigQuery, Teradata, and Imapala/Hive. Struct and array 
data types are not currently supported.)

Below is the command syntax for row validations. In order to run row level
validations you need to pass a `--primary-key` flag which defines what field(s)
the validation will be compared on, as well as either the `--comparison-fields` flag
or the `--hash` flag.

The `--comparison-fields` flag specifies the values (e.g. columns) whose raw values will be compared
based on the primary key join. The `--hash` flag will run a checksum across specified columns in
the table. This will include casting to string, sanitizing the data (ifnull, rtrim, upper), concatenating,
and finally hashing the row.


Under the hood, row validation uses
[Calculated Fields](https://github.com/GoogleCloudPlatform/professional-services-data-validator#calculated-fields) to
apply functions such as IFNULL() or RTRIM(). These can be edited in the YAML config to customize your row validation.

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
  --hash COLUMNS        Comma separated list of columns to hash or * for all columns 
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
  [--use-random-row or -rr]
                        Finds a set of random rows of the first primary key supplied.
  [--random-row-batch-size or -rbs]
                        Row batch size used for random row filters (default 10,000).
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

#### Custom Query Column Validations

Below is the command syntax for custom query column validations.

```
data-validation (--verbose or -v) validate custom-query
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
  --custom-query-type CUSTOM_QUERY_TYPE, -cqt CUSTOM_QUERY_TYPE
                        Type of custom query validation: ('row'|'column')
                        Enter 'column' for custom query column validation
  --source-query-file  SOURCE_QUERY_FILE, -sqf SOURCE_QUERY_FILE
                        File containing the source sql commands
  --target-query-file TARGET_QUERY_FILE, -tqf TARGET_QUERY_FILE
                        File containing the target sql commands
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
  [--labels or -l KEY1=VALUE1,KEY2=VALUE2]
                        Comma-separated key value pair labels for the run.
  [--format or -fmt]    Format for stdout output. Supported formats are (text, csv, json, table).
                        Defaults to table.
```

The default aggregation type is a 'COUNT *'. If no aggregation flag (i.e count,
sum , min, etc.) is provided, the default aggregation will run.

The [Examples](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/examples.md)
page provides few examples of how this tool can be used to run custom query validations.


#### Custom Query Row Validations 

(Note: Row hash validation is currently only supported for BigQuery, Teradata, and
Imapala/Hive. Struct and array data types are not currently supported.)

Below is the command syntax for row validations. In order to run row level
validations you need to pass `--hash` flag which will specify the fields
of the custom query result that will be concatenated and hashed.

Below is the command syntax for custom query row validations.

```
data-validation (--verbose or -v) validate custom-query
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
  --custom-query-type CUSTOM_QUERY_TYPE, -cqt CUSTOM_QUERY_TYPE
                        Type of custom query validation: ('row'|'column')
                        Enter 'row' for custom query column validation
  --source-query-file SOURCE_QUERY_FILE, -sqf SOURCE_QUERY_FILE
                        File containing the source sql commands
  --target-query-file TARGET_QUERY_FILE, -tqf TARGET_QUERY_FILE
                        File containing the target sql commands
  --hash '*'            '*' to hash all columns.
  [--bq-result-handler or -bqrh PROJECT_ID.DATASET.TABLE]
                        BigQuery destination for validation results. Defaults to stdout.
                        See: *Validation Reports* section
  [--service-account or -sa PATH_TO_SA_KEY]
                        Service account to use for BigQuery result handler output.
  [--labels or -l KEY1=VALUE1,KEY2=VALUE2]
                        Comma-separated key value pair labels for the run.
  [--format or -fmt]    Format for stdout output. Supported formats are (text, csv, json, table).
                        Defaults to table.
```

The [Examples](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/examples.md)
page provides few examples of how this tool can be used to run custom query row validations.

### YAML Configuration Files

You can customize the configuration for any given validation by providing use
case specific CLI arguments or editing the YAML configuration file.

For example, the following command creates a YAML file for the validation of the
`new_york_citibike` table: `data-validation validate column -sc my_bq_conn -tc
my_bq_conn -tbls bigquery-public-data.new_york_citibike.citibike_trips -c
citibike.yaml`. 

The vaildation config file is saved to the GCS path specified by the `PSO_DV_CONFIG_HOME`
env variable if that has been set; otherwise, it is saved to wherever the tool is run. 

You can now edit the YAML file if, for example, the `new_york_citibike` table is
stored in datasets that have different names in the source and target systems.
Once the file is updated and saved, the following command runs the
validation:

```
data-validation configs run -c citibike.yaml
```

View the complete YAML file for a Grouped Column validation on the
[Examples](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/examples.md#sample-yaml-config-grouped-column-validation) page.

You can view a list of all saved validation YAML files using `data-validation configs list`, and print a YAML config using `data-validation configs get -c citibike.yaml`. 

### Validation Reports

The result handlers tell DVT where to store the results of
each validation. The tool can write the results of a validation run to Google
BigQuery or print to stdout (default). View the schema of the results 
table [here](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/terraform/results_schema.json).

To output to BigQuery, simply include the `-bqrh` flag during a validation run
like so:
```
data-validation validate column
  -sc bq_conn
  -tc bq_conn
  -tbls bigquery-public-data.new_york_citibike.citibike_trips
  -bqrh project_id.dataset.table
  -sa service-acct@project.iam.gserviceaccount.com
```

### Ad Hoc SQL Exploration

There are many occasions where you need to explore a data source while running
validations. To avoid the need to open and install a new client, the CLI allows
you to run ad hoc queries.

```
data-validation query
  --conn or -c CONN
          The connection name to be queried
  --query or -q QUERY
          The raw query to run against the supplied connection
```

### Building Matched Table Lists

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

## Validation Logic
### Aggregated Fields

Aggregate fields contain the SQL fields that you want to produce an aggregate
for. Currently the functions `COUNT()`, `AVG()`, `SUM()`, `MIN()`, and `MAX()`
are supported.

Here is a sample aggregate config:
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
```

If you are aggregating columns with large values, you can CAST() before aggregation
with calculated fields as shown in [this example](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/examples.md#sample-yaml-with-calc-fields-cast-to-numeric-before-aggregation).

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

### Hash and Comparison Fields

Row level validations can involve either a hash/checksum or comparison fields.
A hash validation (`--hash '*'`) will first sanitize the data with the following
operations on all or selected columns: CAST to string, IFNULL replace with a default
replacement string, RSTRIP, and UPPER. Then, it will CONCAT() the results
and run a SHA256() hash and compare the source and target results. Since each row will
be returned in the result set, it is recommended to utilize the `--use-random-row` feature
to validate a subset of the table.

Comparison field validations (`--comp-fields column`) involve an value comparison of the
column values. These values will be compared via a JOIN on their corresponding primary
key and will be evaluated for an exact match.

See hash and comparison field validations in the [Examples](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/examples.md#run-a-row-hash-validation-for-all-rows) page.

### Calculated Fields

Sometimes direct comparisons are not feasible between databases due to
differences in how particular data types may be handled. These differences can
be resolved by applying functions to columns in the query itself.
Examples might include trimming whitespace from a string, converting strings to
a single case to compare case insensitivity, or rounding numeric types to a
significant figure.

Once a calculated field is defined, it can be referenced by other calculated
fields at any "depth" or higher. Depth controls how many subqueries are executed
in the resulting query. For example, with the following YAML config...

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

If you generate the config file for a row validation, you can see that it uses
calculated fields to generate the query. You can also use calculated fields
in column level validations to generate the length of a string, or cast
a INT field to BIGINT for aggregations. 

See the [Examples page](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/examples.md#sample-yaml-with-calc-fields-cast-to-numeric-before-aggregation) for a sample
cast to NUMERIC.

## Contributing

Contributions are welcome. See the [contributing guide](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/CONTRIBUTING.md) for details.
