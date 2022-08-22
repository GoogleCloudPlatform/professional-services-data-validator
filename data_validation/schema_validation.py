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

        results = schema_validation_matching(
            source_fields, target_fields, self.config_manager.exclusion_columns
        )
        df = pandas.DataFrame(
            results,
            columns=[
                "source_column_name",
                "target_column_name",
                "source_agg_value",
                "target_agg_value",
                "validation_status",
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

        # empty columns added due to changes on the results schema
        df.insert(loc=14, column="primary_keys", value=None)
        df.insert(loc=15, column="num_random_rows", value=None)
        df.insert(loc=16, column="group_by_columns", value=None)
        df.insert(loc=17, column="difference", value=None)
        df.insert(loc=18, column="pct_threshold", value=None)

        return df


def schema_validation_matching(source_fields, target_fields, exclusion_fields):
    """Compare schemas between two dictionary objects"""
    results = []
    # Apply the casefold() function to lowercase the keys of source and target
    source_fields_casefold = {
        source_field_name.casefold(): source_field_type
        for source_field_name, source_field_type in source_fields.items()
    }
    target_fields_casefold = {
        target_field_name.casefold(): target_field_type
        for target_field_name, target_field_type in target_fields.items()
    }

    if exclusion_fields is not None:
        for field in exclusion_fields:
            source_fields_casefold.pop(field, None)
            target_fields_casefold.pop(field, None)

    # Go through each source and check if target exists and matches
    for source_field_name, source_field_type in source_fields_casefold.items():
        # target field exists
        if source_field_name in target_fields_casefold:
            # target data type matches
            target_field_type = target_fields_casefold[source_field_name]
            if source_field_type == target_field_type:
                results.append(
                    [
                        source_field_name,
                        source_field_name,
                        str(source_field_type),
                        str(target_field_type),
                        consts.VALIDATION_STATUS_SUCCESS,
                    ]
                )
            # target data type mismatch
            else:
                results.append(
                    [
                        source_field_name,
                        source_field_name,
                        str(source_field_type),
                        str(target_field_type),
                        consts.VALIDATION_STATUS_FAIL,
                    ]
                )
        # target field doesn't exist
        else:
            results.append(
                [
                    source_field_name,
                    "N/A",
                    str(source_field_type),
                    "N/A",
                    consts.VALIDATION_STATUS_FAIL,
                ]
            )

    # source field doesn't exist
    for target_field_name, target_field_type in target_fields_casefold.items():
        if target_field_name not in source_fields_casefold:
            results.append(
                [
                    "N/A",
                    target_field_name,
                    "N/A",
                    str(target_field_type),
                    consts.VALIDATION_STATUS_FAIL,
                ]
            )
    return results
