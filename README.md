# Data Validation Tooling
The goal of this tool is to allow easy comparison and validation between different tables.  The repository supports several types of comparison
    - Count Validation: Total counts and other aggregates match between source and destination
    - Partitioned Count: Partitioned counts and other aggregations between source and destination
        - Grouped values ie SELECT updated_at::DATE, COUNT(1) matches for each date
    - Stretch: Custom queries of any kind match between source and destination
    - Stretch: Random sampling of data matches between source and destination


## Source Configuration
Every source type requires its own configuration for connectivity.  Below is an example of the expected configuration for each source type.

### BigQuery
```
{
    # Configuration Required for All Data Soures
    "source_type": "BigQuery",

    # BigQuery Specific Connection Config
    "project_id": "my-project-name",
    
    # Configuration Required Depending on Validator Type
    "schema_name": "dataset_name",
    "table_name": "table_name",
    consts.PARTITION_COLUMN: "updated_at",
    consts.PARTITION_COLUMN_TYPE: "DATE",
}
```

## Example of Comparing BigQuery to BigQuery
```
from data_validation import data_validation, consts
from data_validation.data_sources import data_client, example_client, bigquery
from data_validation.query_builder import query_builder

inp_config = {
    # Configuration Required for All Data Soures
    "source_type": "BigQuery",

    # BigQuery Specific Connection Config
    "project_id": os.environ["PROJECT_ID"],

    # Configuration Required Depending on Validator Type
    "schema_name": "bigquery-public-data.new_york_citibike",
    "table_name": "citibike_trips",
    consts.PARTITION_COLUMN: "starttime",
    consts.PARTITION_COLUMN_TYPE: "DATE",
}
out_config = {
    # Configuration Required for All Data Soures
    "source_type": "BigQuery",

    # BigQuery Specific Connection Config
    "project_id": os.environ["PROJECT_ID"],

    # Configuration Required Depending on Validator Type
    "schema_name": "bigquery-public-data.new_york_citibike",
    "table_name": "citibike_trips",
    consts.PARTITION_COLUMN: "starttime",
    consts.PARTITION_COLUMN_TYPE: "DATE",
}

builder = query_builder.QueryBuilder.build_partition_count_validator()
data_validation.process_data(builder, inp_config, out_config)
```