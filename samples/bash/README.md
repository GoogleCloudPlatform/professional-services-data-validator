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

Validatate a 1m row table on a host with 4 vCPUs and > 30GB available RAM.
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
-rw-r--r-- 1 nj 2164 Oct  9 10:57 0000.yaml
-rw-r--r-- 1 nj 2198 Oct  9 10:57 0001.yaml
-rw-r--r-- 1 nj 2198 Oct  9 10:57 0002.yaml
-rw-r--r-- 1 nj 2166 Oct  9 10:57 0003.yaml
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
- The validation took just over 1 minute


Validatate a 20m row table on a host with 4 vCPUs and only 4GB available RAM. Request two DVT threads per vCPU.
```
$ ./auto_partition.sh -t dvt_test.tab_vol_20m -c 20000000 -p id -d 2
Splitting dvt_test.tab_vol_20m
Partitions: 64
Parallelism: 8
Parallel passes: 8
10/10/2024 09:45:04 AM-INFO: Writing table partition configs to directory: /tmp/auto_partition
10/10/2024 09:45:04 AM-INFO: Success! Table partition configs written to directory: /tmp/auto_partition
total 256
-rw-r--r-- 1 nj 2166 Oct 10 09:45 0000.yaml
-rw-r--r-- 1 nj 2200 Oct 10 09:45 0001.yaml
... snip ...
-rw-r--r-- 1 nj 2208 Oct 10 09:45 0062.yaml
-rw-r--r-- 1 nj 2172 Oct 10 09:45 0063.yaml
Pass: 1
============
Submitting partition: 0
Submitting partition: 1
Submitting partition: 2
Submitting partition: 3
Submitting partition: 4
Submitting partition: 5
Submitting partition: 6
Submitting partition: 7
10/10/2024 09:45:08 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0001.yaml
10/10/2024 09:45:08 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0000.yaml
10/10/2024 09:45:08 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0005.yaml
10/10/2024 09:45:08 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0003.yaml
10/10/2024 09:45:08 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0002.yaml
10/10/2024 09:45:08 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0007.yaml
10/10/2024 09:45:08 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0006.yaml
10/10/2024 09:45:08 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0004.yaml
10/10/2024 09:47:01 AM-INFO: Results written to BigQuery, run id: fdb047f4-b2d0-49ff-8e8d-a641f54130e5
10/10/2024 09:47:03 AM-INFO: Results written to BigQuery, run id: 12a8f5ac-93a1-49da-871f-ddc47a7b3086
10/10/2024 09:47:04 AM-INFO: Results written to BigQuery, run id: 776e77c5-4ec7-4a81-b116-883871969fac
10/10/2024 09:47:10 AM-INFO: Results written to BigQuery, run id: 25325778-1e99-48ec-8e53-638ec79f6ba1
10/10/2024 09:47:11 AM-INFO: Results written to BigQuery, run id: f323980b-4b5e-4727-9b6a-bdb9a6d48e9d
10/10/2024 09:47:11 AM-INFO: Results written to BigQuery, run id: aed43d11-7fc1-4eb4-9230-bc5155438c78
10/10/2024 09:47:12 AM-INFO: Results written to BigQuery, run id: 38b5d194-8b80-4393-ac88-21442793acd9
10/10/2024 09:47:13 AM-INFO: Results written to BigQuery, run id: 109d6aff-205d-428a-aef3-13617394a401
Pass: 2
============
... snip ...
Pass: 8
============
TBC
```

Notes:
- We requested 2 DVT proceses per vCPU and therefore run 8 DVT partitions concurrently
- Because of low memory availability, 4GB, the table was split into 64 partitions
- The 64 partitions will take 8 passes to be processed
- The validation took TBC
