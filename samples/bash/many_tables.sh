#!/bin/bash
# Copyright 2026 Google LLC
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

# Assumptions:
# - Requirement to run a column validation for every table in one or more schemas.
# - Schema and table names are the same in source and target.
# - The validations will run on the current host (where this script is executing) in a
#   rolling pool of concurrent DVT processes.
# - Column validations push aggregation down to the databases, therefore the main constraint
#   on parallelism is load on the source/target databases rather than local CPU/RAM.

function show_usage {
  echo "Usage: $0 -s <schemas-csv> [-p <parallelism>] [-w <work-dir>]"
  echo "       parallelism defaults to local vCPU count"
  echo "       work-dir defaults to /tmp/many_tables/<timestamp> and must not already contain YAML files"
}

# Connection names
SRC="ora"
TRG="pg"
RH="--result-handler=conn-name.dvt_dataset.results"

# Column validation aggregates. Remove all of them for a COUNT(*) only validation.
AGGREGATES=(--count='*' --sum='*' --min='*' --max='*')

OPTIND=1

# Initialize our own variables:
SCHEMAS=""
PARALLELISM=""
WORK_DIR=""

while getopts "hs:p:w:" OPT; do
  case "$OPT" in
    h)
      show_usage
      exit 0
      ;;
    s)
      SCHEMAS=$OPTARG
      ;;
    p)
      PARALLELISM=$OPTARG
      ;;
    w)
      WORK_DIR=$OPTARG
      ;;
    *)
      show_usage
      exit 1
      ;;
  esac
done

shift $((OPTIND-1))

if [[ -z "${SCHEMAS}" ]];then
  show_usage
  exit 1
fi

# How many DVT processes can be executed concurrently.
if [[ -z "${PARALLELISM}" ]];then
  PARALLELISM=$(nproc)
fi
if ! [[ "${PARALLELISM}" =~ ^[1-9][0-9]*$ ]];then
  echo "Parallelism must be a positive integer: ${PARALLELISM}"
  exit 1
fi

if [[ -z "${WORK_DIR}" ]];then
  WORK_DIR="/tmp/many_tables/$(date +'%Y%m%d%H%M%S')"
fi
YAML_DIR="${WORK_DIR}/yaml"
LOG_DIR="${WORK_DIR}/logs"
mkdir -p "${WORK_DIR}" "${LOG_DIR}"

# Convert "schema1,schema2" into "schema1.*,schema2.*".
IFS=',' read -ra SCHEMA_ARRAY <<< "${SCHEMAS}"
TABLES_LIST=$(printf '%s.*,' "${SCHEMA_ARRAY[@]}")
TABLES_LIST=${TABLES_LIST%,}

echo "Tables: ${TABLES_LIST}"
echo "Parallelism: ${PARALLELISM}"
echo "Work directory: ${WORK_DIR}"

# 1. Generate one validation YAML file per table.
#    "schema.*" expands to every table in the schema that exists in both source and target.
#    The find-tables command offers more control, for example schema name mapping
#    (--allowed-schemas=src_schema=trg_schema), including views and fuzzy match scoring. Its JSON
#    output can be passed to --tables-list instead, although very long JSON strings may exceed
#    the operating system's maximum argument length.
#    --config-dir names each file <schema>.<table>.yaml and refuses to write to a non-empty directory.
data-validation validate column -sc="${SRC}" -tc="${TRG}" \
  --tables-list="${TABLES_LIST}" \
  "${AGGREGATES[@]}" \
  --config-dir="${YAML_DIR}" \
  ${RH}
if [[ $? != 0 ]];then
  echo "Error generating validation configs"
  exit 1
fi

# DVT assigns files to task indexes in sorted file name order (Python sorted()).
# LC_ALL=C sort gives the same code point order, letting us label each task with its file name.
mapfile -t YAML_FILES < <(cd "${YAML_DIR}" && ls -1 -- *.yaml 2>/dev/null | LC_ALL=C sort)
FILE_COUNT=${#YAML_FILES[@]}
if [[ "${FILE_COUNT}" == 0 ]];then
  echo "No YAML configs generated"
  exit 1
fi
echo "YAML configs generated: ${FILE_COUNT}"

# 2. Run the validations in a rolling pool of ${PARALLELISM} DVT processes.
#    Unlike fixed passes, a new validation starts as soon as any running validation finishes,
#    so one large table does not hold up the rest.
function run_task {
  local idx=$1
  local name=$2
  local log="${LOG_DIR}/${name%.yaml}.log"
  echo "Starting task ${idx}: ${name}"
  # CLOUD_RUN_TASK_INDEX and CLOUD_RUN_TASK_COUNT are set by Cloud Run for each task in a job
  # and catered for in DVT by the -kc option. We mimic them below so that each DVT process
  # picks a single YAML file from the config directory.
  if CLOUD_RUN_TASK_INDEX=${idx} CLOUD_RUN_TASK_COUNT=${FILE_COUNT} \
    data-validation configs run -kc -cdir="${YAML_DIR}" > "${log}" 2>&1; then
    echo "[OK]    task ${idx}: ${name}"
  else
    echo "[ERROR] task ${idx}: ${name}, see ${log}"
    return 1
  fi
}
export -f run_task
export YAML_DIR LOG_DIR FILE_COUNT

SECONDS=0
for i in "${!YAML_FILES[@]}";do
  printf '%s\0%s\0' "${i}" "${YAML_FILES[$i]}"
done | xargs -0 -n 2 -P "${PARALLELISM}" bash -c 'run_task "$1" "$2"' _ \
  | tee "${WORK_DIR}/summary.txt"

ERROR_COUNT=$(grep -c '^\[ERROR\]' "${WORK_DIR}/summary.txt")
echo "============"
echo "Validations: ${FILE_COUNT}, errors: ${ERROR_COUNT}, elapsed: ${SECONDS}s"
echo "Logs: ${LOG_DIR}"
if [[ "${ERROR_COUNT}" != 0 ]];then
  exit 1
fi
