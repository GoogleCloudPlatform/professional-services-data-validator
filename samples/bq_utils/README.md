# Helper scripts for BigQuery validations

## Dataset-level

Currently, we do not natively support validations of an entire BQ dataset yet. This is a workaround to execute this task.

This script will take all existing tables at source and run validations on target, **if they have the same name.** 

**IMPORTANT:** It will run only 2 types of validations (column, schema). Other types are not available in this script, feel free to modify it.

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
./bq-dataset-level-validation.sh [PROJECT_BQ_SOURCE] [SOURCE_BQ_DATASET] [PROJECT_BQ_SOURCE] [SOURCE_BQ_DATASET] [FULLNAME_BQ_TABLE_VALIDATION_RESULTS] <OPTIONAL ARGUMENTS>
```

Like this example:

```shell script 
./bq-dataset-level-validation.sh your-project dataset1 your-project dataset2 your-project.pso_data_validator.results
```

1. (Optional) You can add a filter. Let's suppose all your tables have a partition timestamp and you want to perform a validation only in a specific timeframe. You can add the filter to that as an optional argument:

```shell script 
./bq-dataset-level-validation.sh your-project dataset1 your-project dataset2 your-project.pso_data_validator.results "--filters 'partitionTs BETWEEN TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -3 DAY) AND CURRENT_TIMESTAMP()'"
```

