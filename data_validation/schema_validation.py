# Copyright 2021 Google LLC
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

import datetime
import pandas

from data_validation import metadata, consts, clients


class SchemaValidation(object):
    def __init__(self, config_manager, run_metadata=None, verbose=False):
        """Initialize a SchemaValidation client

        Args:
            config_manager (ConfigManager): The ConfigManager for the validation.
            run_metadata (RunMetadata): The RunMetadata for the validation.
            verbose (bool): If verbose, the Data Validation client will print the queries run
        """
        self.verbose = verbose
        self.config_manager = config_manager
        self.run_metadata = run_metadata or metadata.RunMetadata()

    def execute(self):
        """Performs a validation between source and a target schema"""
        ibis_source_schema = clients.get_ibis_table_schema(
            self.config_manager.source_client,
            self.config_manager.source_schema,
            self.config_manager.source_table,
        )
        ibis_target_schema = clients.get_ibis_table_schema(
            self.config_manager.target_client,
            self.config_manager.target_schema,
            self.config_manager.target_table,
        )

        source_fields = {}
        for field_name, data_type in ibis_source_schema.items():
            source_fields[field_name] = data_type
        target_fields = {}
        for field_name, data_type in ibis_target_schema.items():
            target_fields[field_name] = data_type
        results = schema_validation_matching(source_fields, target_fields)
        df = pandas.DataFrame(
            results,
            columns=[
                "source_column_name",
                "target_column_name",
                "source_agg_value",
                "target_agg_value",
                "validation_status",
                "error_result.details",
            ],
        )

        # Update and Assign Metadata Values
        self.run_metadata.end_time = datetime.datetime.now(datetime.timezone.utc)

        df.insert(loc=0, column="run_id", value=self.run_metadata.run_id)
        df.insert(loc=1, column="validation_name", value="Schema")
        df.insert(loc=2, column="validation_type", value="Schema")

        df.insert(
            loc=3,
            column="labels",
            value=[self.run_metadata.labels for _ in range(len(df.index))],
        )
        df.insert(loc=4, column="start_time", value=self.run_metadata.start_time)
        df.insert(loc=5, column="end_time", value=self.run_metadata.end_time)

        df.insert(
            loc=6,
            column="source_table_name",
            value=self.config_manager.full_source_table,
        )
        df.insert(
            loc=7,
            column="target_table_name",
            value=self.config_manager.full_target_table,
        )
        df.insert(loc=10, column="aggregation_type", value="Schema")

        del df["error_result.details"]
        return df


def schema_validation_matching(source_fields, target_fields):
    """Compare schemas between two dictionary objects"""
    results = []
    # Go through each source and check if target exists and matches
    for source_field_name, source_field_type in source_fields.items():
        # target field exists
        if source_field_name in target_fields:
            # target data type matches
            if source_field_type == target_fields[source_field_name]:
                results.append(
                    [
                        source_field_name,
                        source_field_name,
                        "1",
                        "1",
                        consts.VALIDATION_STATUS_SUCCESS,
                        "Source_type:{} Target_type:{}".format(
                            source_field_type, target_fields[source_field_name]
                        ),
                    ]
                )
            # target data type mismatch
            else:
                results.append(
                    [
                        source_field_name,
                        source_field_name,
                        "1",
                        "1",
                        consts.VALIDATION_STATUS_FAIL,
                        "Data type mismatch between source and target. Source_type:{} Target_type:{}".format(
                            source_field_type, target_fields[source_field_name]
                        ),
                    ]
                )
        # target field doesn't exist
        else:
            results.append(
                [
                    source_field_name,
                    "N/A",
                    "1",
                    "0",
                    consts.VALIDATION_STATUS_FAIL,
                    "Target doesn't have a matching field name",
                ]
            )

    # source field doesn't exist
    for target_field_name, target_field_type in target_fields.items():
        if target_field_name not in source_fields:
            results.append(
                [
                    "N/A",
                    target_field_name,
                    "0",
                    "1",
                    consts.VALIDATION_STATUS_FAIL,
                    "Source doesn't have a matching field name",
                ]
            )
    return results
