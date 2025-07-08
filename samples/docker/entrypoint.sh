#!/bin/sh
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

# Exit on first error
set -e

# Decode environment variables to connection files if they exist.
# These files can then be referenced in the validation YAML config.
if [ -n "$DVT_SRC_CONN" ]; then
  echo "Found DVT_SRC_CONN, decoding to /app/source_connection.json"
  echo "$DVT_SRC_CONN" | base64 -d > /app/source_connection.json
fi

if [ -n "$DVT_TGT_CONN" ]; then
  echo "Found DVT_TGT_CONN, decoding to /app/target_connection.json"
  echo "$DVT_TGT_CONN" | base64 -d > /app/target_connection.json
fi

# Execute the main data validation command, passing all arguments (`$@`)
exec python -m data_validation "$@"
