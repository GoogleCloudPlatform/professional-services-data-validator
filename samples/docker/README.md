This application can be containerized using following command:

```
./build_docker.sh
```
The docker container is built by installing the latest version of google-pso-data-validator from PyPi. The Dockerfile contains other python driver packages that may need to be installed to connect to different databases. The docker container also uses environment variables to encode source and target connection files (details below). With this approach credentials to access databases are not installed in the docker image, improving security. If above command finishes successfully, following command would show a new image with the latest version of data validation application.

```buildoutcfg
docker images
```
Two environment variables DVT_SRC_CONN and DVT_TGT_CONN contain base64 encoded contents of connections to source and target databases. If your connections names are postgres and bigquery, then the connection files are usually in `~/.config/google-pso-data-validator/`. These connection files can be encoded with connection names as postgres and bigquery as follows:
```
export DVT_SRC_CONN=postgres:`base64 -w 0 ~/.config/google-pso-data-validator/postgres.connection.json`
export DVT_TGT_CONN=bigquery:`base64 -w 0 ~/.config/google-pso-data-validator/bigquery.connection.json`
```
You can run following command to run data validation tool as a docker container, securely passing your database login credentials using environment variables as follows:

```buildoutcfg
 docker run -e DVT_SRC_CONN -e DVT_TGT_CONN data_validation:7.7.0 validate row -sc postgres -tc bigquery -tbls=pso_data_validator.dvt_core_types -pk=id -hash='*'
```