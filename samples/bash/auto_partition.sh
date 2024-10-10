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
# - All local CPU resources are for this process to use in full.
# - All available RAM on the host is for this process to use in full.

function show_usage {
  echo 'Usage: $0 -t <table-name> -c <row-count-in-table> -p <primary-key-columns-csv>'
}

# Constant based on internal testing.
# Should be reliable because the size of a hash is constant.
MB_PER_MILLION_ROWS=1500

# Connection names
SRC="ora23"
TRG="pg"
BQRH="--bq-result-handler=db-black-belts-playground-01.neiljohnson_dvt_test.results"

OPTIND=1

# Initialize our own variables:
TABLE_NAME=""
TABLE_ROW_COUNT=0
PRIMARY_KEYS=""
DVTS_PER_VCPU=1

while getopts "ht:c:p:d:" OPT; do
  case "$OPT" in
    h)
      show_usage
      exit 0
      ;;
    t)
      TABLE_NAME=$OPTARG
      ;;
    c)
      TABLE_ROW_COUNT=$OPTARG
      ;;
    p)
      PRIMARY_KEYS=$OPTARG
      ;;
    d)
      DVTS_PER_VCPU=$OPTARG
      ;;
  esac
done

shift $((OPTIND-1))

if [[ -z "${TABLE_NAME}" || -z "${TABLE_ROW_COUNT}" || -z "${PRIMARY_KEYS}" ]];then
  show_usage
  exit 0
fi

# Available RAM on the VM.
AVAILABLE_MB=$(free -m|grep "Mem:"|awk '{print $NF}')
VCPU=$(nproc)

# How many DVT processes can be executed concurrently.
PERMITTED_THREADS=$(python3 -c "print(${VCPU}*${DVTS_PER_VCPU})")

ROWS_PER_PARTITION=$(python3 -c "mb_per_vcpu=${AVAILABLE_MB}/${PERMITTED_THREADS};print(round(mb_per_vcpu/${MB_PER_MILLION_ROWS}*1000000))")

DVT_PARTITIONS=$(python3 -c "import math; print(${PERMITTED_THREADS} * round(math.ceil(${TABLE_ROW_COUNT}/${ROWS_PER_PARTITION}/${PERMITTED_THREADS})))")
if [[ $? != 0 ]];then
  echo "Error calculating DVT_PARTITIONS"
  exit 1
fi

# This is the number of loop iterations required to process all partitions.
PARALLEL_PASSES=$(python3 -c "import math; print(math.ceil(${DVT_PARTITIONS}/${PERMITTED_THREADS}))")

echo "Splitting ${TABLE_NAME}"
echo "Partitions: ${DVT_PARTITIONS}"
echo "Parallelism: ${PERMITTED_THREADS}"
echo "Parallel passes: ${PARALLEL_PASSES}"

YAML_DIR=/tmp/auto_partition
mkdir -p ${YAML_DIR}

CMD="data-validation generate-table-partitions -sc=${SRC} -tc=${TRG} \
  -tbls=${TABLE_NAME} \
  --primary-keys=${PRIMARY_KEYS} --hash=* \
  -cdir=${YAML_DIR} \
  --partition-num=${DVT_PARTITIONS}" ${BQRH}
${CMD}
if [[ $? != 0 ]];then
  echo "Error generating partitions with command:"
  echo "${CMD}"
  exit 1
fi

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
