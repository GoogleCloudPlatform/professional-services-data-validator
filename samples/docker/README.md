# :whale: Data Validation Tool - Dockerized Version

## Introduction

Welcome to the Dockerized version of the Data Validation Tool. Thisversion of DVT solution encapsulates the robust functionality of the tool within a Docker container, allowing users to benefit from portability, ease of setup, and consistent environments across deployments.

## Prerequisites

* Docker and Docker Compose installed on your machine.
* Google Cloud SDK credentials if accessing data on GCP.
* A data directory on your local system to mount into the container.
* The default directory is `/etc/data` but can be changed in the `docker-compose.yml` file.

* Please note that you will want to look over the example `docker-compose.yml` file and make any necessary changes to the environment variables and volumes to match your local environment.

## Setup and Installation

1. Clone the repository to your local machine.

2. Navigate to the `docker` directory.

3. Make any necessary changes to the `docker-compose.yml` file.

4. Build the Docker image

```bash
docker-compose build
```

5. Run the Docker container

```bash
docker-compose run validator
```

## Troubleshooting

* Issue: Command history not saved.
* Solution: Ensure that the .bash_history symlink is correctly pointing to /etc/data/bash_history.  

* Issue: Unable to access GCP services.
* Solution: Verify that the Google Cloud SDK credentials are correctly mounted into the container and the GOOGLE_APPLICATION_CREDENTIALS environment variable is set.

* Issue: Unable to access data on local machine.
* Solution: Verify that the data directory is correctly mounted into the container.

## Goals

The Google Data Validation Tool allows users to validate data in a variety of ways.

This Dockerized DVT solution allows you to run DVT in a Docker container with all of the tools and configurations needed to make things a bit easier.

The goals of this project are as follows:

* Provide a Dockerized version of the Data Validation Tool that can be run on any machine with Docker installed.

* Make the experience feel like a local installation of DVT.

* Simplify Configuration.

## Running Validations

A shell will be provided upon running the container. From here, you can run any of the validations available in the DVT4.

Please note that you'll want to ensure your data validation connection properties json documents are persisted to the `/etc/data` directory on your local machine. This will allow you to access them from within the container. If not already done, please issue this command in the terminal:

```bash
export PSO_DV_CONFIG_HOME=/etc/data
```

If everything is working correctly, after running the container, you should be presented with a shell that looks like this:

```bash
root@033341eb89dc:/$
```

From here, we can see what's available to us by running a few commands.

```bash
root@033341eb89dc:/$ ./cloud_sql_proxy --version
cloud-sql-proxy version 2.6.0+linux.amd64

root@033341eb89dc:/$ gsutil ls gs://gcp-public-data-landsat/ | wc -l

12

root@033341eb89dc:/$ ./cloud_sql_proxy --port 15432 &
[1] 159

root@033341eb89dc:/$ 2023/08/09 01:04:28 Authorizing with Application Default Credentials
2023/08/09 01:04:29 Listening on 127.0.0.1:15432

root@033341eb89dc:/$ data-validation -h
usage: The Data Validation CLI tool is intended to help to build and execute
data validation runs with ease.

root@033341eb89dc:/$ data-validation connections add --connection-name postgres Postgres --host 127.0.0.1 --user postgres --password foo --database tfmv --port 15432

root@033341eb89dc:/$ data-validation connections list
08/09/2023 01:05:45 AM-INFO: Connection Name: postgres : Postgres

root@033341eb89dc:/$ exit

(base) ➜  DVT4-DOCKER git:(feature/DVT4DOCKER) ✗ docker-compose run validator

The programs included with the Debian GNU/Linux system are free software;
the exact distribution terms for each program are described in the
individual files in /usr/share/doc/*/copyright.

Debian GNU/Linux comes with ABSOLUTELY NO WARRANTY, to the extent
permitted by applicable law.
Welcome to the Data Valdation Tool
=====================================
Data Validation Tool Help:
=====================================
To view help the Data Validation Tool, run the following command:
data-validation -h
=====================================
CLOUD_SQL_INSTANCE environment variable is not set. Please export it to start Cloud SQL Proxy.
Example: export CLOUD_SQL_INSTANCE=project:region:instance
Then start the proxy with: ./cloud_sql_proxy --port 15432 $CLOUD_SQL_INSTANCE &

root@90384ece68fe:/$ data-validation connections list
08/09/2023 01:07:08 AM-INFO: Connection Name: postgres : Postgres

root@90384ece68fe:/$ data-validation connections list
08/09/2023 01:07:08 AM-INFO: Connection Name: postgres : Postgres

root@33014e0b4363:/$ data-validation validate column -sc postgres -tc postgres -tbls public.flights=public.flights -bqrh project_id.pso_data_validator.results
08/09/2023 01:13:16 AM-INFO:  
╒═══════════════════╤═══════════════════╤═════════════════════╤══════════════════════╤════════════════════╤════════════════════╤══════════════════╤═════════════════════╤══════════════════════════════════════╕
│ validation_name   │ validation_type   │ source_table_name   │ source_column_name   │   source_agg_value │   target_agg_value │   pct_difference │ validation_status   │ run_id                               │
╞═══════════════════╪═══════════════════╪═════════════════════╪══════════════════════╪════════════════════╪════════════════════╪══════════════════╪═════════════════════╪══════════════════════════════════════╡
│ count             │ Column            │ public.flights      │                      │             214867 │             214867 │                0 │ success             │ db02f4cb-affa-4227-a340-5430c12dc7fa │
╘═══════════════════╧═══════════════════╧═════════════════════╧══════════════════════╧════════════════════╧════════════════════╧══════════════════╧═════════════════════╧══════════════════════════════════════╛
```

