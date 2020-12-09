This application can be containerized using following command:

```
./build_docker.sh
```

If above command finishes successfully, following command would show a new image with the latest version of data
validation application.

```buildoutcfg
docker images
```

Once your confirm that image of the data validation is available. You can run following command to run data validation
tool as a docker container.

```buildoutcfg
 docker run -v ~/.config/:/root/.config --rm data_validation:1.1.2 -h
```
**Please note:** ~/.config/:/root/.config in above command mounts your VM's local .config directory with container's 
.config directory. This allows users to store GCP service account as well as connection information needed by data 
validation tool on a VM.