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
{
    "source_type": "BigQuery",
    "project_id": os.environ["PROJECT_ID"],
    "schema_name": "bigquery-public-data.new_york_citibike",
    "table_name": "citibike_trips"
}

## Example of Comparing BigQuery to BigQuery
```
from data_validation import data_validation
from data_validation.data_sources import data_client, example_client, bigquery
from data_validation.query_builder import query_builder

inp_config = {"source_type": "BigQuery",
              "project_id": os.environ["PROJECT_ID"],
              "schema_name": "bigquery-public-data.new_york_citibike",
              "table_name": "citibike_trips"}
out_config = {"source_type": "BigQuery",
              "project_id": os.environ["PROJECT_ID"],
              "schema_name": "bigquery-public-data.new_york_citibike",
              "table_name": "citibike_trips"}

builder = query_builder.QueryBuilder.build_count_validator()
data_validation.process_data(builder, inp_config, out_config)
```