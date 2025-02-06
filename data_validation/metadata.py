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

"""Metadata classes with data about the validation run."""

import dataclasses
import datetime
import typing

from data_validation import consts


@dataclasses.dataclass
class ValidationMetadata(object):
    validation_type: str
    aggregation_type: str
    source_table_schema: str
    source_table_name: str
    target_table_schema: str
    target_table_name: str
    source_column_name: str
    target_column_name: str
    primary_keys: list
    num_random_rows: int
    threshold: float

    def get_table_name(self, result_type: str) -> str:
        if result_type == consts.RESULT_TYPE_SOURCE:
            return (
                self.source_table_schema + "." + self.source_table_name
                if self.source_table_schema
                else self.source_table_name
            )
        elif result_type == consts.RESULT_TYPE_TARGET:
            return (
                self.target_table_schema + "." + self.target_table_name
                if self.target_table_schema
                else self.target_table_name
            )
        else:
            raise ValueError(f"Unexpected result_type: {result_type}")

    def get_column_name(self, result_type: str) -> str:
        if result_type == consts.RESULT_TYPE_SOURCE:
            return self.source_column_name
        elif result_type == consts.RESULT_TYPE_TARGET:
            return self.target_column_name
        else:
            raise ValueError(f"Unexpected result_type: {result_type}")


@dataclasses.dataclass
class RunMetadata(object):
    run_id: str = dataclasses.field(default_factory=str)
    validations: dict = dataclasses.field(default_factory=dict)
    labels: list = dataclasses.field(default_factory=list)
    start_time: typing.Optional[datetime.datetime] = dataclasses.field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    end_time: typing.Optional[datetime.datetime] = None
