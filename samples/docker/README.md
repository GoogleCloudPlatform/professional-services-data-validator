This application can be containerized using following command:

```
./build_docker.sh
```
The Docker container is built by installing the latest version of google-pso-data-validator from PyPi. The Dockerfile contains other Python driver packages that may need to be installed to connect to different databases. The Docker container also uses environment variables to encode source and target connection files (details below). With this approach credentials to access databases are not installed in the Docker image, improving security. If above command finishes successfully, following command would show a new image with the latest version of data validation application.

```buildoutcfg
docker images
```
Two environment variables DVT_SRC_CONN and DVT_TGT_CONN contain base64 encoded contents of connections to source and target databases. Assume the connections names in the local VM are `postgres` and `bigquery`, and the connection files are in (default) `~/.config/google-pso-data-validator/`. These connection files can be encoded with connection names in the docker image as `src_postgres` and `tgt_bigquery` as follows:
```
export DVT_SRC_CONN=src_postgres:`base64 -w 0 ~/.config/google-pso-data-validator/postgres.connection.json`
export DVT_TGT_CONN=tgt_bigquery:`base64 -w 0 ~/.config/google-pso-data-validator/bigquery.connection.json`
```
You can run the following command to run data validation tool as a Docker container, securely passing your database login credentials using environment variables as follows:

```buildoutcfg
 docker run -e DVT_SRC_CONN -e DVT_TGT_CONN data_validation:7.7.0 validate row -sc src_postgres -tc tgt_bigquery -tbls=pso_data_validator.dvt_core_types -pk=id -hash='*'
```
The service account associated with the docker container will need access to GCP APIs (e.g BigQuery as src or target), GCS and BigQuery (for reporting) depending on the options used. 