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
            source_fields, target_fields, self.config_manager.exclusion_columns, allow_list
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


def schema_validation_matching(source_fields, target_fields, exclusion_fields, allow_list):
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

    #allow list map in case of incompatible  data types in source and target
    allow_list_map = parse_allow_list(allow_list)
    
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
            elif source_field_type in allow_list_map:
                target_field_type = allow_list_map[source_field_type]
                if target_field_type == source_field_type:
                    results.append(
                        [
                            source_field_name,
                            source_field_name,
                            str(source_field_type),
                            str(target_field_type),
                            consts.VALIDATION_STATUS_SUCCESS,
                        ]
                    ) 
                else:                   
                    bool_flag = parse_allowed_precision(source, target)
                    if bool_flag:
                        results.append(
                            [
                                source_field_name,
                                source_field_name,
                                str(source_field_type),
                                str(target_field_type),
                                consts.VALIDATION_STATUS_SUCCESS,
                            ]
                        ) 
                    else:
                        results.append(
                            [
                                source_field_name,
                                source_field_name,
                                str(source_field_type),
                                str(target_field_type),
                                consts.VALIDATION_STATUS_WARNING,
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

def is_number(val):
    try:
        num = int(val)
    except ValueError as e:
        return False
    return True

def parse_allow_list(st):
    output = {}
    stack = []
    key = None
    for i in range(len(st)):
        if st[i] == ":":
            key = "".join(stack)
            output[key] = None
            stack = []
            continue
        if st[i] == "," and not is_number(st[i+1]):
            value = "".join(stack)
            output[key] = value
            stack = []
            i+=1
            continue
        stack.append(st[i])
    value = "".join(stack)
    output[key] = value
    stack = []
    return output

def get_typea_numeric_sustr(st):
    nums = []
    for i in range(len(st)):
        if is_number(st[i]):
            nums.append(st[i])
    num = "".join(nums)
    if num == '':
        return -1
    return int(num)

def get_typeb_numeric_sustr(st):
    nums = []
    first_half = st.split(",")[0]
    second_half = st.split(",")[1]
    first_half_num = get_typea_numeric_sustr(first_half)
    second_half_num = get_typea_numeric_sustr(second_half)
    return first_half_num, second_half_num

def validate_typeb_vals(source, target):
    if source[0] > target[0]:
        return False
    else:
        if source[1] > target[1]:
            return False
    return True

def parse_allowed_precision(source, target):
    #Check for type of precisions supplied e.g: int8,Decimal(10,2),int
    if "(" in source:
        typeb_source = get_typeb_numeric_sustr(source)
        typeb_target = get_typeb_numeric_sustr(target)
        if validate_typeb_vals(typeb_source, typeb_target):
            return True
        return False
    source_num = get_numeric_sustr(source)
    target_num = get_numeric_sustr(target)
    if source_num < target_num:
        return False
    return True




