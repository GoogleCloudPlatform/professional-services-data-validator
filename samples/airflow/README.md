# Data Validation on Airflow (Cloud Composer)

-- explain how to deploy this on Airflow/Composer.
-- Under the high level overview we can include this 'Warning' section. 

Requirements to run the DAG:
- Airflow environment created with Public IP (Private environment disabled)
- Create an Airflow variable called 'gcp_project' with the GCP Project ID


### Warning 

The Airflow DAG expects the raw config JSON which is not the same as a YAML config converted to JSON.

Parameters in a typical YAML config file are slightly different from the raw JSON config, 
which is generated after DVT parses the YAML. The [build_config_manager()](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/data_validation/config_manager.py#L429) 
method generates the JSON config and should be used as a reference.

Our Cloud Run sample also expects a raw JSON config in the `'data'` variable shown
[here](https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/develop/samples/run#test-cloud-run-endpoint).

For example, the following YAML config is equivalent to the JSON config below, where the source param is written as `source_conn`.

```yaml
result_handler: {}
source: my_bq_conn
target: my_bq_conn
validations:
- aggregates:
  - field_alias: count
    source_column: null
    target_column: null
    type: count
...
```

```json5
{
    "source_conn": BQ_CONN,
    "target_conn": BQ_CONN,
    "aggregates": [
        {
            "field_alias": "count",
            "source_column": None,
            "target_column": None,
            "type": "count"
        }
    ]
}
```

For more implementation details, [this](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/data_validation/config_manager.py#L444) 
is where the raw JSON config is generated in the DVT code.
