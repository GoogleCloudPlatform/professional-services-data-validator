#!/bin/bash
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# TODO Add README.md

# Assumptions:
# - Require to auto partition a row --hash validation.
# - The parallel validation will run on the current host (where this script is executing).

# Constant based on internal testing.
# Should be reliable because the size of a hash is constant.
MB_PER_MILLION_ROWS=1500

# TODO These will become command line parameters.
# TABLE_NAME="dvt_test.tab_vol_20m"
# TABLE_ROW_COUNT=20000000
TABLE_NAME="dvt_test.tab_vol_1m"
TABLE_ROW_COUNT=1000000
# Eventually this will be automated in DVT.
PRIMARY_KEYS="id"

# Available RAM on the VM
AVAILABLE_MB=$(free -m|grep "Mem:"|awk '{print $NF}')
VCPU=$(nproc)

# TODO convert logic below to Python snippet.
MB_PER_VCPU=$(echo "${AVAILABLE_MB}|${VCPU}"|awk -F"|" '{print $1/$2}')
ROWS_PER_PARTITION=$(echo "${MB_PER_VCPU}|${MB_PER_MILLION_ROWS}"|awk -F"|" '{printf("%d\n",($1/$2)*1000000)}')

# TODO round DVT_PARTITIONS up to multiple of VCPU for a balanced parallelism.
DVT_PARTITIONS=$(python3 -c "import math; print(${VCPU} * round(math.ceil(${TABLE_ROW_COUNT}/${ROWS_PER_PARTITION}/${VCPU})))")
# TODO status checking.

# This is the number of loop iterations required to process all partitions.
# TODO Once we round DVT_PARTITIONS up correctly this will be a whole number, for now +1.
PARALLEL_PASSES=$(python3 -c "import math; print(math.ceil(${DVT_PARTITIONS}/${VCPU}))")

echo "Splitting ${TABLE_NAME}"
echo "Partitions: ${DVT_PARTITIONS}"
echo "Parallelism: ${VCPU}"
echo "Parallel passes: ${PARALLEL_PASSES}"

YAML_DIR=/tmp/auto_partition
mkdir -p ${YAML_DIR}

data-validation generate-table-partitions -sc=ora23 -tc=pg \
  -tbls=${TABLE_NAME} \
  --primary-keys=${PRIMARY_KEYS} --hash='*' \
  -cdir=${YAML_DIR} \
  --bq-result-handler=db-black-belts-playground-01.neiljohnson_dvt_test.results \
  --partition-num=${DVT_PARTITIONS}
# TODO status checking.

YAML_TABLE_DIR="${YAML_DIR}/${TABLE_NAME}"
ls -l ${YAML_TABLE_DIR}
# TODO stat the directory and check there are files.

JOB_COUNT=0
for i in $(seq 1 1 ${PARALLEL_PASSES});do
    echo "Pass: ${i}"
    echo "============"
    for j in $(seq 0 1 $(expr ${VCPU} - 1));do
        echo "Submitting partition: ${JOB_COUNT}"
        JOB_COMPLETION_INDEX=${JOB_COUNT} data-validation configs run -kc -cdir ${YAML_TABLE_DIR} &
        JOB_COUNT=$(expr ${JOB_COUNT} + 1)
    done
    # Wait for all jobs in this pass to complete.
    wait
done
