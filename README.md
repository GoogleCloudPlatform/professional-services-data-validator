# Data Validation Tooling
The goal of this tool is to allow easy comparison and validation between different tables.  The repository supports several types of comparison
	- Count Validation: Total counts and other aggregates match between source and destination
	- Partitioned Count: Partitioned counts and other aggregations between source and destination
		- Grouped values ie SELECT updated_at::DATE, COUNT(1) matches for each date
	- Stretch: Custom queries of any kind match between source and destination
	- Stretch: Random sampling of data matches between source and destination


## Example

client = example_client.ExampleClient()
builder = query_builder.QueryBuilder.build_count_validator()

query = builder.render_query(client, "schema_name", "table_name", partition_column=None, partition_column_type=None)
print(query)
TODO more