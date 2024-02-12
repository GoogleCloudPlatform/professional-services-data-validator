# Data Validation on Airflow (ideally Cloud Composer)

DVT on Airflow makes use of our [latest released version of DVT library on PyPi](https://pypi.org/project/google-pso-data-validator/#history).
There is no specific operator for it at the moment, so the DAG uses the
main structures of the library itself.

The input is a JSON data representation of the DVT run configuration. 

By default, the DAG will output the results to BigQuery as a result handler.

### Requirements to run the DAG:
- Pre-existing Airflow environment created with Public IP (Private environment disabled)
- Create an Airflow variable called `gcp_project` with the GCP Project ID

### Instructions

1. Download the DAG file in this directory
2. Get the JSON configuration for your use case, explained in the next section
3. Upload it to the DAGs folder in your Airflow environment

## JSON Configuration 

The Airflow DAG expects a JSON config content which is not the same as a YAML config converted to JSON format. The parameters in a typical YAML config file for DVT are slightly different from the JSON config file version, which is generated after DVT parses the YAML.

You can get the JSON content specific for your scenario by using our CLI and providing the argument to generate the JSON config file [`--config-file-json` or `-cj <filepath>.json`]. IMPORTANT: do not forget to make the necessary adjustments between JSON and Python objects, check [this link as a reference](https://python-course.eu/applications-python/json-and-python.php).

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
