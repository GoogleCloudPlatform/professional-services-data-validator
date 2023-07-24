# Helper scripts for BigQuery validations

## Dataset-level

Currently, we do not natively support validations of an entire BQ dataset yet. This is a workaround to execute this task.

This script will run validations on all the BigQuery tables within a provided dataset **as long as the table names match between source and target datasets.** 

**IMPORTANT:** The script will only run column and schema validations for BigQuery source and target databases.

1. Enter the directory:

```shell script 
cd samples/bq_utils/
```

1. Grant execution permissions to file: 

```shell script 
chmod u+x bq-dataset-level-validation.sh
```

1. To run a validation, execute the script by passing the following parameters:

```shell script 
./bq-dataset-level-validation.sh [SOURCE_BQ_PROJECT] [SOURCE_BQ_DATASET] [TARGET_BQ_PROJECT] [TARGET_BQ_DATASET] [FULLNAME_BQ_RESULT_HANDLER] <OPTIONAL ARGUMENTS>
```

Like this example:

```shell script 
./bq-dataset-level-validation.sh your-project dataset1 your-project dataset2 your-project.pso_data_validator.results
```

(Optional) Add an optional filter. Assume all your tables have a partition timestamp and you want to perform a validation within a specific timeframe. You can add the filter as an optional argument:

```shell script 
./bq-dataset-level-validation.sh your-project dataset1 your-project dataset2 your-project.pso_data_validator.results "--filters 'partitionTs BETWEEN TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -3 DAY) AND CURRENT_TIMESTAMP()'"
```

