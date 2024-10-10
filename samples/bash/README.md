# BASH DVT Utility Scripts

## auto_partition.sh

This script will generate-partitions and then execute the partitions based on the resources of the local VM and the size of the table being validated.

It makes the following assumptions:

- A row --hash validation is the desired validation type.
- The parallel validation will run on the current host (where this script is executing).
- All local CPU resources are for this process to use in full.
- All available RAM on the host is for this process to use in full.

In essence the script will validate the table in small enough partitions to not exceed available memory and in enough parallel streams to max out avalable vCPUs.

### Examples

Validatate a 10m row table on a host with 4 vCPUs and
```
$ ./auto_partition.sh -t dvt_test.tab_vol_1m -c 1000000 -p id
```

Output:
```
Splitting dvt_test.tab_vol_1m
Partitions: 4
Parallelism: 4
Parallel passes: 1
10/09/2024 10:57:53 AM-INFO: Writing table partition configs to directory: /tmp/auto_partition
10/09/2024 10:57:53 AM-INFO: Success! Table partition configs written to directory: /tmp/auto_partition
total 16
-rw-r--r-- 1 neiljohnson_google_com neiljohnson_google_com 2164 Oct  9 10:57 0000.yaml
-rw-r--r-- 1 neiljohnson_google_com neiljohnson_google_com 2198 Oct  9 10:57 0001.yaml
-rw-r--r-- 1 neiljohnson_google_com neiljohnson_google_com 2198 Oct  9 10:57 0002.yaml
-rw-r--r-- 1 neiljohnson_google_com neiljohnson_google_com 2166 Oct  9 10:57 0003.yaml
Pass: 1
============
Submitting partition: 0
Submitting partition: 1
Submitting partition: 2
Submitting partition: 3
10/09/2024 10:57:55 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_1m/0000.yaml
10/09/2024 10:57:55 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_1m/0003.yaml
10/09/2024 10:57:55 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_1m/0002.yaml
10/09/2024 10:57:55 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_1m/0001.yaml
10/09/2024 10:59:05 AM-INFO: Results written to BigQuery, run id: ae42cc47-9577-4ab9-9be4-dea3d30ccd0b
10/09/2024 10:59:08 AM-INFO: Results written to BigQuery, run id: 9862c44e-f5a3-4914-8ea0-322ff453f9e3
10/09/2024 10:59:08 AM-INFO: Results written to BigQuery, run id: c3f33655-c533-475f-ba0c-ebd879b13ef1
10/09/2024 10:59:08 AM-INFO: Results written to BigQuery, run id: 49ad17c2-b453-4ae0-976d-72aacc83fe32
```

Notes:
- The table was small enough to validate in a single pass but because there are 4 vCPUs available it was split into 4 partitions
- The validation took just over 1 minute.

