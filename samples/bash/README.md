# BASH DVT Utility Scripts

## auto_partition.sh

This script will generate-partitions and then execute the partitions based on the resources of the local VM and the size of the table being validated.

It makes the following assumptions:

- A row --hash validation is the desired validation type.
- The parallel validation will run on the current host (where this script is executing).
- All local CPU resources are for this process to use in full.
- All available RAM on the host is for this process to use in full.

