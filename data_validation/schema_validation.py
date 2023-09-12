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
import itertools
import logging
import pandas
import re

from data_validation import metadata, consts, clients, exceptions


# Check for decimal data type with precision and/or scale. Permits hyphen in p/s for value ranges.
DECIMAL_PRECISION_SCALE_PATTERN = re.compile(
    r"([!]?decimal)\(([0-9\-]+)(?:,[ ]*([0-9\-]+))?\)", re.I
)
# Extract lower/upper from a range of the format "0-2" or "12-18".
DECIMAL_PRECISION_SCALE_RANGE_PATTERN = re.compile(
    r"([0-9]{2}|[0-9])(?:\-)([0-9]{2}|[0-9])"
)


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
            source_fields,
            target_fields,
            self.config_manager.exclusion_columns,
            self.config_manager.allow_list,
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


def schema_validation_matching(
    source_fields, target_fields, exclusion_fields, allow_list
):
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

    # Allow list map in case of incompatible  data types in source and target
    allow_list_map = parse_allow_list(allow_list)
    # Go through each source and check if target exists and matches
    for source_field_name, source_field_type in source_fields_casefold.items():
        if source_field_name not in target_fields_casefold:
            # Target field doesn't exist
            results.append(
                [
                    source_field_name,
                    "N/A",
                    str(source_field_type),
                    "N/A",
                    consts.VALIDATION_STATUS_FAIL,
                ]
            )
            continue

        target_field_type = target_fields_casefold[source_field_name]
        if source_field_type == target_field_type:
            # Target data type matches
            results.append(
                [
                    source_field_name,
                    source_field_name,
                    str(source_field_type),
                    str(target_field_type),
                    consts.VALIDATION_STATUS_SUCCESS,
                ]
            )
        elif (
            string_val(source_field_type) in allow_list_map
            and string_val(target_field_type)
            in allow_list_map[string_val(source_field_type)]
        ):
            # Data type pair match an allow-list pair.
            results.append(
                [
                    source_field_name,
                    source_field_name,
                    string_val(source_field_type),
                    str(target_field_type),
                    consts.VALIDATION_STATUS_SUCCESS,
                ]
            )
        else:
            # Target data type mismatch
            (higher_precision, lower_precision,) = parse_n_validate_datatypes(
                string_val(source_field_type), string_val(target_field_type)
            )
            if higher_precision:
                # If the target precision is higher then the validation is acceptable but worth a warning.
                logging.warning(
                    "Source and target data type has precision mismatch: %s - %s",
                    string_val(source_field_type),
                    str(target_field_type),
                )
                results.append(
                    [
                        source_field_name,
                        source_field_name,
                        string_val(source_field_type),
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
                        consts.VALIDATION_STATUS_FAIL,
                    ]
                )

    # Source field doesn't exist
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


def split_allow_list_str(allow_list_str: str) -> list:
    """Split the allow list string into a list of datatype:datatype tuples."""
    # I've not moved this patter to a compiled constant because it should only
    # happen once per command and I felt splitting the pattern into variables
    # aided readability.
    nullable_pattern = r"!?"
    precision_scale_pattern = r"(?:\((?:[0-9 ,\-]+|'UTC')\))?"
    data_type_pattern = nullable_pattern + r"[a-z0-9 ]+" + precision_scale_pattern
    csv_split_pattern = data_type_pattern + r":" + data_type_pattern
    data_type_pairs = [
        _.replace(" ", "").split(":")
        for _ in re.findall(csv_split_pattern, allow_list_str, re.I)
    ]
    invalid_pairs = [_ for _ in data_type_pairs if len(_) != 2]
    if invalid_pairs:
        raise exceptions.SchemaValidationException(
            f"Invalid data type pairs: {invalid_pairs}"
        )
    return data_type_pairs


def expand_precision_range(s: str) -> list:
    """Expand an integer range (e.g. "0-3") to a list (e.g. ["0", "1", "2", "3"])."""
    m_range = DECIMAL_PRECISION_SCALE_RANGE_PATTERN.match(s)
    if not m_range:
        return [s]
    try:
        p_lower = int(m_range.group(1))
        p_upper = int(m_range.group(2))
        if p_lower >= p_upper:
            raise exceptions.SchemaValidationException(
                f"Invalid allow list data type precision/scale: Lower value {p_lower} >= upper value {p_upper}"
            )
        return [str(_) for _ in range(p_lower, p_upper + 1)]
    except ValueError as e:
        raise exceptions.SchemaValidationException(
            f"Invalid allow list data type precision/scale: {s}"
        ) from e


def expand_precision_or_scale_range(data_type: str) -> list:
    """Take a data type and example any precision/scale range.

    For example "decimal(1-3,0)" becomes:
      ["decimal(1,0)", "decimal(2,0)", "decimal(3,0)"]"""

    m = DECIMAL_PRECISION_SCALE_PATTERN.match(data_type.replace(" ", ""))
    if not m:
        return [data_type]

    if len(m.groups()) != 3:
        raise exceptions.SchemaValidationException(
            f"Badly formatted data type: {data_type}"
        )

    type_name, p, s = m.groups()
    p_list = expand_precision_range(p)
    if s:
        s_list = expand_precision_range(s)
        return_list = [
            f"{type_name}({p},{s})" for p, s in itertools.product(p_list, s_list)
        ]
    else:
        return_list = [f"{type_name}({_})" for _ in p_list]
    return return_list


def parse_allow_list(st: str) -> dict:
    """Convert allow-list data type pairs into a dictionary like {key[value1, value2, etc], }"""

    def expand_allow_list_ranges(data_type_pairs: list) -> list:
        expanded_pairs = []
        for dt1, dt2 in data_type_pairs:
            dt1_list = expand_precision_or_scale_range(dt1)
            dt2_list = expand_precision_or_scale_range(dt2)
            expanded_pairs.extend(
                [(_[0], _[1]) for _ in itertools.product(dt1_list, dt2_list)]
            )
        return expanded_pairs

    def convert_pairs_to_dict(expanded_pairs: list) -> dict:
        """Take the list data type tuples and convert them into a dictionary keyed on source data type.
        For example:
            [('decimal(2,0)', 'int64'), ('decimal(2,0)', 'int32')]
        becomes:
            {'decimal(2,0)': ['int64', 'int32']}
        """
        return_pairs = {}
        for dt1, dt2 in expanded_pairs:
            if dt1 in return_pairs:
                return_pairs[dt1].append(dt2)
            else:
                return_pairs[dt1] = [dt2]
        return return_pairs

    data_type_pairs = split_allow_list_str(st)
    expanded_pairs = expand_allow_list_ranges(data_type_pairs)
    return_pairs = convert_pairs_to_dict(expanded_pairs)
    return return_pairs


# typea data types: int8,int16
def get_typea_numeric_sustr(st):
    nums = []
    if "(" in st:
        return -1
    for i in range(len(st)):
        if st[i].isdigit():
            nums.append(st[i])
    num = "".join(nums)
    if num == "":
        return -1
    return int(num)


# typeb data types: Decimal(10,2)
def get_typeb_numeric_sustr(st: str) -> tuple:
    m = DECIMAL_PRECISION_SCALE_PATTERN.match(st.replace(" ", ""))
    if not m:
        return -1, -1
    _, p, s = m.groups()
    if s is None:
        s = 0
    return int(p), int(s)


def string_val(st):
    return str(st).replace(" ", "")


def validate_typeb_vals(source, target):
    if source[0] > target[0] or source[1] > target[1]:
        return False, True
    elif source[0] == target[0] and source[1] == target[1]:
        return False, False
    return True, False


def strip_null(st):
    return st.replace("!", "")


def parse_n_validate_datatypes(source, target) -> tuple:
    """
    Args:
        source: Source table datatype string
        target: Target table datatype string
    Returns:
        bool:target has higher precision value
        bool:target has lower precision value
    """
    if strip_null(source) == target:
        return False, False
    if "(" in source and "(" in target:
        typeb_source = get_typeb_numeric_sustr(source)
        typeb_target = get_typeb_numeric_sustr(target)
        higher_precision, lower_precision = validate_typeb_vals(
            typeb_source, typeb_target
        )
        return higher_precision, lower_precision
    source_num = get_typea_numeric_sustr(source)
    target_num = get_typea_numeric_sustr(target)
    # In case of no bits specified, we will not match for precisions
    if source_num == -1 or target_num == -1:
        return False, False
    if source_num == target_num:
        return False, False
    elif source_num > target_num:
        return False, True
    return False, False
