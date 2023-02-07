# Copyright 2020 Google LLC
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

import os
from data_validation import data_validation, consts


HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "default")


CONN = {
    "source_type": "Hive",
    "host": HIVE_HOST,
    "user": "System",
    "port": 10000,
    "database": HIVE_DATABASE,
}


HIVE_CONFIG = {
    # Specific Connection Config
    consts.CONFIG_SOURCE_CONN: CONN,
    consts.CONFIG_TARGET_CONN: CONN,
    # Validation Type
    consts.CONFIG_TYPE: "Column",
    # Configuration Required Depending on Validator Type
    consts.CONFIG_SCHEMA_NAME: "SYSTEM",
    consts.CONFIG_TABLE_NAME: "hive_data",
    consts.CONFIG_AGGREGATES: [
        {
            consts.CONFIG_TYPE: "count",
            consts.CONFIG_SOURCE_COLUMN: "title",
            consts.CONFIG_TARGET_COLUMN: "title",
            consts.CONFIG_FIELD_ALIAS: "count",
        },
    ],
    consts.CONFIG_FORMAT: "table",
    consts.CONFIG_FILTER_STATUS: None,
}


def test_count_validator():
    validator = data_validation.DataValidation(HIVE_CONFIG, verbose=True)
    df = validator.execute()
    assert int(df["source_agg_value"][0]) > 0
    assert df["source_agg_value"][0] == df["target_agg_value"][0]
