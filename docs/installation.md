# Data Validation Tool Installation Guide
The data validation tool can be installed on any machine that has Python 3.6+ installed. 

The tool natively supports BigQuery connections. If you need to connect to other databases such as Teradata or Oracle, you will need to install the appropriate connection libraries. (See the [Connections](connections.md) page for details)

This tool can be natively installed on your machine or can be containerized and run with Docker.


## Prerequisites
The data validation tool can be configured to store the results of validation runs into BigQuery tables. To allow tool to 
do that, we need to do following:

## Setup

To write results to BigQuery, you'll need to setup the required cloud
resources, local authentication, and configure the tool.

A Google Cloud Platform project with the BigQuery API enabled is required.

Confirm which Google user account will be used to execute the tool. If you plan to run this tool in
production, it's recommended that you create a service account specifically
for running the tool.
There are two methods of creating the Cloud resources necessary for the tool: via Terraform or the Cloud SDK.
### Create cloud resources - Terraform

You can use Terraform to create the necessary BigQuery resources. (See next
section for manually creating resources with `gcloud`.)

```
cd terraform
terraform init
terraform apply
```

You should see a dataset named `pso_data_validator` and a table named
`results`.

### Create cloud resources - Cloud SDK (gcloud)

Create a dataset for validation results.

```
bq mk pso_data_validator
```

Create a table.

```
bq mk --table \
  --time_partitioning_field start_time \
  --clustering_fields validation_name,run_id \
  pso_data_validator.results \
  terraform/results_schema.json
```




## Deploy Data Validation CLI on your machine

The Data Validation tooling requires Python 3.6+.

Create and activate a new virtual environment to sandbox the tool and its
dependencies from your system installation of Python.

```
python3.6 -m venv venv
source venv/bin/activate
```

Update pip and make sure gcc is installed in your environment.
```
apt-get update  && apt-get install gcc -y && apt-get clean
pip install --upgrade pip
```


You can [pip install this package directly from git](https://pip.pypa.io/en/stable/reference/pip_install/#git)
for any tag (or branch or commit). We suggest installing from latest [GitHub Release]([GitHub](https://github.com/GoogleCloudPlatform/professional-services-data-validator/releases).

install from tag 1.1.0
```bash
python -m pip install -e git+https://github.com/GoogleCloudPlatform/professional-services-data-validator.git@1.1.0#egg=google_pso_data_validator
```

install from HEAD of default branch
```bash
python -m pip install -e git+https://github.com/GoogleCloudPlatform/professional-services-data-validator.git#egg=google_pso_data_validator
```


Alternatively, you can install from [Python Wheel](https://pythonwheels.com/) on [GCS](
https://storage.googleapis.com/professional-services-data-validator/releases/1.1.0/google_pso_data_validator-1.1.0-py3-none-any.whl).

```
python -m pip install https://storage.googleapis.com/professional-services-data-validator/releases/1.1.0/google_pso_data_validator-1.1.0-py3-none-any.whl
```


If you require Teradata and have a license, install the `teradatasql` package.

```
python -m pip install teradatasql
```

After installing the Data Validation package you will
have access to the `data-validation -h` or `python -m data_validation -h`
tool on your CLI.


## Build a Docker container 
If native installation is not an option for you, you can create a Docker image for this tool.  

Here's an [example](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/samples/docker/README.md) on how you can create a sample docker image for this tool.
