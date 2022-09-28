# Data Validation Tool Installation Guide

The tool natively supports BigQuery connections. If you need to connect to other databases such as Teradata or Oracle, you will need to install the appropriate connection libraries. (See the [Connections](connections.md) page for details)

This tool can be natively installed on your machine or can be containerized and run with Docker.


## Prerequisites

- Any machine with Python 3.7+ installed.

## Setup

By default, the data validation tool writes the results of data validation to `stdout`. However, we recommend storing the results of validations to a BigQuery table in order to standardize the process and share results across a team. In order to allow the data validation tool to write to a BigQuery table, users need to have a BigQuery table created with a specific schema. If you choose to write results to a BigQuery table, there are a couple of requirements:

- A Google Cloud Platform project with the BigQuery API enabled.

- A Google user account with appropriate permissions. If you plan to run this tool in production, it's recommended that you create a service account specifically for running the tool. See our [guide](https://cloud.google.com/docs/authentication/production) on how to authenticate with your service account. If you are using a service account, you need to grant your service account appropriate roles on your project so that it has permissions to create and read resources.

Clone the repository onto your machine and navigate inside the directory:

```
git clone https://github.com/GoogleCloudPlatform/professional-services-data-validator.git
cd professional-services-data-validator
```

There are two methods of creating the BigQuery output table for the tool: via *Terraform* or the *Cloud SDK*.


### Cloud Resource Creation - Terraform

By default, Terraform is run inside a test environment and needs to be directed to your project. Perform the following steps to direct the creation of the BigQuery table to your project:

1. Delete the `testenv.tf` file inside the `terraform` folder
2. View `variables.tf` inside the `terraform` folder and replace `default = "pso-kokoro-resources"` with `default = "YOUR_PROJECT_ID"`


After installing the [terraform CLI tool](https://learn.hashicorp.com/tutorials/terraform/install-cli) and completing the steps above, run the following commands from inside the root of the repo:

```
cd terraform
terraform init
terraform apply
```

### Cloud Resource Creation - Cloud SDK (gcloud)

Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) if necessary. 

Create a dataset for validation results:

```
bq mk pso_data_validator
```

Create a table:

```
bq mk --table \
  --time_partitioning_field start_time \
  --clustering_fields validation_name,run_id \
  pso_data_validator.results \
  terraform/results_schema.json
```

### Cloud Resource Creation - After success

You should see a dataset named `pso_data_validator` and a table named
`results` created inside of your project.

After installing the CLI tool using the instructions below, you will be ready to run data validation commands and output the results to BigQuery. See an example [here](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/examples.md#store-results-in-a-bigquery-table).


## Deploy Data Validation CLI on your machine

The Data Validation tooling requires Python 3.7+.

```
sudo apt-get install python3
sudo apt-get install python3-dev
```

Create and activate a new virtual environment to sandbox the tool and its
dependencies from your system installation of Python. 

```
python3.7 -m venv venv
source venv/bin/activate
```

Update pip and make sure gcc is installed in your environment.
```
sudo apt-get update  && sudo apt-get install gcc -y && sudo apt-get clean
pip install --upgrade pip
```

You can install the tool via [pip](https://pypi.org/project/google-pso-data-validator/1.2.0/).
```
pip install google-pso-data-validator
```

If you require Teradata and have a license, install the `teradatasql` package.

```
python -m pip install teradatasql
```

If you plan to perform row level hashing on teradata, you will need to install a UDF that implements sha256 on your Teradata instance. An example can be found [here](https://downloads.teradata.com/forum/extensibility/sha-2-udfs-for-teradata).

After installing the Data Validation package you will
have access to the `data-validation -h` or `python -m data_validation -h`
tool on your CLI.


## Test locally
If you want to test local changes to the tool, run the following command from the root directory of this repository:
```
pip install .
```
The unit test suite can be executed using either `pytest tests/unit` or `python -m nox -s unit` from the root directory. If you are using nox, you will need to run `pip install nox` first.


## Build a Docker container 
If native installation is not an option for you, you can create a Docker image for this tool.  

Here's an [example](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/samples/docker/README.md) on how you can create a sample docker image for this tool.


## Automate using Apache Airflow
You can orchestrate DVT by running a validation as a task within an Airflow DAG.

Here's a simple [example](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/samples/airflow/dvt_airflow_dag.py) on how you can execute this tool using the [PythonVirtualenvOperator](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html#pythonvirtualenvoperator).
