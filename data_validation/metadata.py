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
import uuid


@dataclasses.dataclass
class ValidationMetadata(object):
    validation_type: str
    aggregation_type: str
    source_table_name: str
    target_table_name: str
    source_column_name: str
    target_column_name: str


@dataclasses.dataclass
class RunMetadata(object):
    run_id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    validations: dict = dataclasses.field(default_factory=dict)
    start_time: typing.Optional[datetime.datetime] = dataclasses.field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    end_time: typing.Optional[datetime.datetime] = None
