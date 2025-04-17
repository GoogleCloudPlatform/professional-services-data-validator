# BASH DVT Utility Scripts

## auto_partition.sh

This script will execute DVT's [generate-table-partitions](https://github.com/GoogleCloudPlatform/professional-services-data-validator?tab=readme-ov-file#generate-partitions-for-large-row-validations) feature and then execute the partitions based on the resources of the local VM and the size of the table being validated. This approach is an alternative to the [main options to scale DVT usage](https://github.com/GoogleCloudPlatform/professional-services-data-validator?tab=readme-ov-file#scaling-dvt).

It makes the following assumptions:

- A row --hash validation is the desired validation type.
- The parallel validation will run on the current host (where this script is executing).
- All local CPU resources are for this process to use in full.
- All available RAM on the host is for this process to use in full.

In essence the script will validate the table in small enough partitions to not exceed available memory and in enough parallel streams to max out available vCPUs.

To use the script please edit the `SRC`, `TRG` and `RH` variables.

The script does not include `--filter-status=fail`, for a significant boost in performance you could add that option to your own script.

### Examples

#### 1 million rows

Validate a 1m row table on a host with 4 vCPUs and > 30GB available RAM.
```shell
$ ./auto_partition.sh -t dvt_test.tab_vol_1m -c 1000000 -i id
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

#### 20 million rows

Validate a 20m row table on a host with 4 vCPUs and only 4GB available RAM. Requesting parallelism of 8, two DVT processes per vCPU.
```
$ ./auto_partition.sh -t dvt_test.tab_vol_20m -c 20000000 -i id -p 8
Splitting dvt_test.tab_vol_20m
Partitions: 64
Parallelism: 8
Parallel passes: 8
10/10/2024 11:13:17 AM-INFO: Writing table partition configs to directory: /tmp/auto_partition
10/10/2024 11:13:17 AM-INFO: Success! Table partition configs written to directory: /tmp/auto_partition
total 256
-rw-r--r-- 1 nj 2166 Oct 10 11:13 0000.yaml
-rw-r--r-- 1 nj 2200 Oct 10 11:13 0001.yaml
... snip ...
-rw-r--r-- 1 nj 2208 Oct 10 11:13 0062.yaml
-rw-r--r-- 1 nj 2172 Oct 10 11:13 0063.yaml
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
10/10/2024 11:13:21 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0007.yaml
10/10/2024 11:13:21 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0002.yaml
10/10/2024 11:13:21 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0000.yaml
10/10/2024 11:13:21 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0003.yaml
10/10/2024 11:13:21 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0006.yaml
10/10/2024 11:13:21 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0001.yaml
10/10/2024 11:13:21 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0005.yaml
10/10/2024 11:13:21 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0004.yaml
10/10/2024 11:15:02 AM-INFO: Results written to BigQuery, run id: fd675089-cc4b-469f-8110-f7ceac235b48
10/10/2024 11:15:03 AM-INFO: Results written to BigQuery, run id: 8c447688-cf87-42a3-93be-79a83af2a237
10/10/2024 11:15:04 AM-INFO: Results written to BigQuery, run id: 2c2a0fb3-d122-4fb5-acf9-f5d0862064cd
10/10/2024 11:15:04 AM-INFO: Results written to BigQuery, run id: a51a74be-64b8-4518-8bef-cb8025fc4b38
10/10/2024 11:15:05 AM-INFO: Results written to BigQuery, run id: 367efd32-8731-46bd-ab28-12d26ed56f45
10/10/2024 11:15:05 AM-INFO: Results written to BigQuery, run id: b4dc675a-bf50-43ef-94ea-a1d5e70ae3ce
10/10/2024 11:15:06 AM-INFO: Results written to BigQuery, run id: 8c598927-4614-4370-8457-9fcce4ea4122
10/10/2024 11:15:06 AM-INFO: Results written to BigQuery, run id: a236687e-04cb-4f87-b911-f2c93a9231cf
Pass: 2
============
... snip ...
Pass: 8
============
Submitting partition: 56
Submitting partition: 57
Submitting partition: 58
Submitting partition: 59
Submitting partition: 60
Submitting partition: 61
Submitting partition: 62
Submitting partition: 63
10/10/2024 11:26:13 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0057.yaml
10/10/2024 11:26:13 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0058.yaml
10/10/2024 11:26:14 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0063.yaml
10/10/2024 11:26:14 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0060.yaml
10/10/2024 11:26:14 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0059.yaml
10/10/2024 11:26:14 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0056.yaml
10/10/2024 11:26:14 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0062.yaml
10/10/2024 11:26:14 AM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_20m/0061.yaml
10/10/2024 11:27:57 AM-INFO: Results written to BigQuery, run id: 7a583883-8a8f-4751-90d4-74c1917bc570
10/10/2024 11:27:57 AM-INFO: Results written to BigQuery, run id: 00a70900-a5f7-4adc-8609-f02c0d423402
10/10/2024 11:27:57 AM-INFO: Results written to BigQuery, run id: 5bc3e8b4-18f7-453a-b23f-eee5b263b0c2
10/10/2024 11:27:58 AM-INFO: Results written to BigQuery, run id: 298b7c69-54c5-4571-a96e-b765666a727f
10/10/2024 11:27:59 AM-INFO: Results written to BigQuery, run id: 2e2f8a08-fb7a-4687-b9f5-77e02917867e
10/10/2024 11:27:59 AM-INFO: Results written to BigQuery, run id: 8f646fe7-7d73-4ce7-b807-58fd7b2e287a
10/10/2024 11:27:59 AM-INFO: Results written to BigQuery, run id: ded7b50e-3b58-4b14-b24c-a8569a85f838
10/10/2024 11:27:59 AM-INFO: Results written to BigQuery, run id: 064dffc5-daf5-4047-ac12-6f431aeceddd
```

Notes:
- We requested parallelism of 8, two DVT processes per vCPU
- Because of low memory availability, 4GB, the table was split into 64 partitions
- The 64 partitions will take 8 passes to be processed
- The validation took 14 minutes

#### 100 million rows

Validatate a 100m row table on a host with 4 vCPUs and only 30GB available RAM, requesting parallelism of 8, two DVT processes per vCPU.
```
$ ./auto_partition.sh -t dvt_test.tab_vol_100m -c 100000000 -i id -p 8
Splitting dvt_test.tab_vol_100m
Partitions: 40
Parallelism: 8
Parallel passes: 5
10/28/2024 01:49:16 PM-INFO: Writing table partition configs to directory: /tmp/auto_partition
10/28/2024 01:49:16 PM-INFO: Success! Table partition configs written to directory: /tmp/auto_partition
total 160
-rw-r--r-- 1 nj 2170 Oct 28 13:49 0000.yaml
-rw-r--r-- 1 nj 2206 Oct 28 13:49 0001.yaml
...
-rw-r--r-- 1 nj 2210 Oct 28 13:49 0038.yaml
-rw-r--r-- 1 nj 2174 Oct 28 13:49 0039.yaml
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
10/28/2024 01:49:22 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0001.yaml
10/28/2024 01:49:22 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0004.yaml
10/28/2024 01:49:22 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0005.yaml
10/28/2024 01:49:22 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0003.yaml
10/28/2024 01:49:22 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0000.yaml
10/28/2024 01:49:22 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0007.yaml
10/28/2024 01:49:22 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0006.yaml
10/28/2024 01:49:22 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0002.yaml
10/28/2024 02:12:34 PM-INFO: Results written to BigQuery, run id: aabfd5e7-a2c0-4efd-9365-3b5cc163ff88
10/28/2024 02:12:36 PM-INFO: Results written to BigQuery, run id: d5f3aada-33ec-467b-bc3a-4f686b37e3a7
10/28/2024 02:12:53 PM-INFO: Results written to BigQuery, run id: 0e8a68a9-db93-4161-8166-e185113f9e85
10/28/2024 02:18:58 PM-INFO: Results written to BigQuery, run id: d4803ab8-6ed9-4a73-b97e-04801a9b6bc3
10/28/2024 02:19:01 PM-INFO: Results written to BigQuery, run id: 2d47a5b1-b7f0-4d0e-ad23-b33e9ad8fcda
10/28/2024 02:19:19 PM-INFO: Results written to BigQuery, run id: eba15952-a798-49e3-b02d-7183c9785d0d
10/28/2024 02:19:21 PM-INFO: Results written to BigQuery, run id: 6b13b620-a01d-4fca-9acb-d0d33e87eb0c
10/28/2024 02:25:25 PM-INFO: Results written to BigQuery, run id: af2074e8-1e78-4f1e-af2b-080446369ba7
Pass: 2
============
...
Pass: 5
============
Submitting partition: 32
Submitting partition: 33
Submitting partition: 34
Submitting partition: 35
Submitting partition: 36
Submitting partition: 37
Submitting partition: 38
Submitting partition: 39
10/28/2024 03:19:06 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0036.yaml
10/28/2024 03:19:06 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0034.yaml
10/28/2024 03:19:06 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0039.yaml
10/28/2024 03:19:06 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0033.yaml
10/28/2024 03:19:06 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0038.yaml
10/28/2024 03:19:06 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0037.yaml
10/28/2024 03:19:06 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0032.yaml
10/28/2024 03:19:06 PM-INFO: Currently running the validation for YAML file: /tmp/auto_partition/dvt_test.tab_vol_100m/0035.yaml
10/28/2024 03:35:41 PM-INFO: Results written to BigQuery, run id: eac014c6-028a-4372-a1e1-2c2331a8ce62
10/28/2024 03:36:04 PM-INFO: Results written to BigQuery, run id: 0810b1ca-b618-4a20-bc9a-394aa0060014
10/28/2024 03:36:15 PM-INFO: Results written to BigQuery, run id: e75c35d4-ba9e-4986-af17-8b9da26c25aa
10/28/2024 03:36:16 PM-INFO: Results written to BigQuery, run id: a2f8a549-c51d-45d0-9571-96a882464926
10/28/2024 03:36:18 PM-INFO: Results written to BigQuery, run id: bbf89093-4d07-47c0-91a7-4f87823e5179
10/28/2024 03:36:18 PM-INFO: Results written to BigQuery, run id: c44ab3f2-6413-4629-bbc4-a60ab67a78b3
10/28/2024 03:36:19 PM-INFO: Results written to BigQuery, run id: f9487d83-3f3c-4bd2-aed1-1803035903f6
10/28/2024 03:36:22 PM-INFO: Results written to BigQuery, run id: b1db8834-9669-4112-8e87-67668d8ebf2d
```

Notes:
- We requested parallelism of 8, two DVT processes per vCPU
- Because there was plenty of memory available, 30GB, the table was split into 40 partitions
- The 40 partitions will take 5 passes to be processed
- Validation of the 100m row table took 1 hour 45 minutes (your mileage may vary)
- The auto_partition.sh script stored all results in BigQuery (100m entries) which takes some time. We could use --filter-status=fail for a faster validation