# Data Validation Tool

The Data Validation Tool (DVT) is a open sourced Python CLI tool based on the [ibis framework](https://ibis-project.org/docs/tutorial/01-Introduction-to-Ibis.html) that compares heterogeneous data source tables with multiple levels validation functions. 

Data validation is a critical step in a EDW, a Database or a Datalake migration project, where structured or semi-structured data from both the source and the destination tables are compared to ensure they are matched and correct after each migration step (e.g., data and schema migration, SQL script translation, ETL migration, etc.). The Data Validation Tool provides an automated and repeatable solution to perform this task.

DVT supports the following validation types:
- Table level
  - Table row count
  - Group by row count
  - Column aggregation
  - Filters and limits

- Column level
  - Full column data type
  - Selected column data type

- Row level hash comparison  (* BQ tables only)

- Schema validation

- Raw SQL exploration
  - Retrieve result set with a custom query

## Installation

The [installation](docs/installation.md) page describes the prerequisites and
setup steps needed to install and use the data validation tool.

## Usage

Before using this tool, you will need to create connections to the source and
target tables. Once the connections are created, you can run validations on
those tables. Validation results can be printed to stdout (default) or outputted
to BigQuery. The validation tool also allows you to save or edit validation
configurations in a YAML file. This is useful for running common validations or
updating the configuration.

### Connections

The [Connections](docs/connections.md) page provides details about how to create
and list connections for the validation tool.

### Running CLI Validations

The data validation CLI is a main interface to use this tool.

The CLI has several different commands which can be used to create and re-run
validations.

The validation tool first expects connections to be created before running
validations. To create connections please review the
[Connections](docs/connections.md) page.

Once you have your connections set up, you are ready to run the validations.

### Validation command syntax and options

```
data-validation run
  --type TYPE, -t TYPE  Type of Data Validation (Column, GroupedColumn)
  --source-conn SOURCE_CONN, -sc SOURCE_CONN
                        Source connection details.
                        See: *Data Source Configurations* section for each data source
  --target-conn TARGET_CONN, -tc TARGET_CONN
                        Target connection details
                        See: *Data Source Configurations* section for each data source
  --tables-list TABLES, -tbls TABLES
                        JSON List of tables
                        '[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips","target_table_name":"citibike_trips"}]'
  --grouped-columns GROUPED_COLUMNS, -gc GROUPED_COLUMNS
                        JSON List of columns to use in group by '["col_a"]'
                        (Optional) Only used in GroupedColumn type validations
  --count COUNT         JSON List of columns count '["col_a"]' or * for all columns
  --sum SUM             JSON List of columns sum '["col_a"]' or * for all numeric
  --min MIN             JSON List of columns min '[\"col_a\"]' or * for all numeric
  --max MAX             JSON List of columns max '[\"col_a\"]' or * for all numeric
  --avg AVG             JSON List of columns avg '[\"col_a\"]' or * for all numeric
  --result-handler-config RESULT_HANDLER_CONFIG, -rc RESULT_HANDLER_CONFIG
                        (Optional) JSON Result handler config details. Defaults to stdout
                        See: *Validation Reports* section
  --filters FILTER      JSON List of filters '[{"type":"custom","source":"Col > 100","target":"Col > 100"}]'
  --config-file CONFIG_FILE, -c CONFIG_FILE
                        YAML Config File Path to be used for storing validations.
  --threshold THRESHOLD, -th THRESHOLD
                        (Optional) Float value. Maximum pct_difference allowed for validation to be considered a success. Defaults to 0.0
  --labels KEY=VALUE, -l KEY1=VALUE1,KEY2=VALUE2
                        (Optional) Comma-separated key value pair labels for the run.
  --verbose, -v         Verbose logging will print queries executed
```

The [Examples](docs/examples.md) page provides many examples of how a tool can
used to run powerful validations without writing any queries.

### Running Custom SQL Exploration

There are many occasions where you need to explore a data source while running
validations. To avoid the need to open and install a new client, the CLI allows
you to run custom queries. `data-validation query --conn connection-name The
named connection to be queried. --query, -q The Raw query to run against the
supplied connection`

## Query Configurations

You can customize the configuration for any given validation by providing use
case specific CLI arguments or editing the saved YAML configuration file.

For example, the following command creates a YAML file for the validation of the
`new_york_citibike` table. `data-validation run -t Column -sc bq -tc bq -tbls
'[{"schema_name":"bigquery-public-data.new_york_citibike","table_name":"citibike_trips"}]'
-c citibike.yaml`

Here is the generated YAML file named `citibike.yaml`:

```
result_handler: {}
source: bq target:
bq validations:
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
target_table_name: citibike_trips type: Column
```

You can now edit the YAML file if, for example, the `new_york_citibike` table is
stored in datasets that have different names in the source and target systems.
Once the file is updated and saved, the following command runs the new
validation:

```
data-validation run-config -c citibike.yaml
```

The Data Validation Tool exposes several components that can be stitched
together to generate a wide range of queries

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

Filters let you apply a WHERE statement to your validation query. Currently the
only form of filter supported is a custom filter written by you in the syntax of
the given source. In future we will also release pre-built filters to cover
certain usecases (ie. `SELECT * FROM table WHERE created_at > 30 days ago;`).

#### Custom Filters

```
{
    "type": "custom",
    "source": "created_at > '2020-01-01 00:00:00'::TIMESTAMP",
    "target": "created_at > '2020-01-01 00:00:00'",
}
```

Note that you are writing the query to execute, which does not have to match
between source and target as long as the results can be expected to align.

### Grouped Columns

Grouped Columns contain the fields you want your aggregations to be broken out
by, e.g. `SELECT last_updated::DATE, COUNT(*) FROM my.table` will produce a
resultset that breaks down the count of rows per calendar date.

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

The data validation tool can write the results of a validation run to Google
BigQuery or print to Std Out.

The output handlers tell the data validation where to store the results of each
validation. By default the handler will print to stdout.

### Configure tool to output to BigQuery

```
{
    # Configuration Required for All Data Soures
    "type": "BigQuery",

    # BigQuery Specific Connection Config
    "project_id": "my-project-name",

    # (Optional) BigQuery JSON Config File for On-Prem usecases
    "google_service_account_key_path": "/path/to/key.json",

    # Configuration Required Depending on Validator Type
    "table_id": "dataset_name.table_name"
}
```

## Building Matched Table Lists

Creating the JSON list of matched tables can be a hassle.  We have added a feature which may help you to match all of the tables together between source and target.
The find-tables tool:

- Pulls all tables in the source (applying a supplied allowed-schemas filter)
- Pulls all tables from the target
- Uses Levenshtein distance to match tables
- Finally, it prints a JSON list of tables which can be copy/pasted into the validation run config.

`data-validation find-tables --source-conn source --target-conn target --allowed-schemas '["pso_data_validator"]' `

## Add Support for an existing Ibis Data Source

If you want to add an Ibis Data Source which exists, but was not yet supported
in the Data Validation tool, it is a simple process.

1.  In data_validation/data_validation.py

    -   Import the extened Client for the given source (ie. from
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
