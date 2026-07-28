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

from ibis.backends.bigquery.datatypes import BigQueryType
import ibis.expr.datatypes as dt

orig_bq_to_ibis = BigQueryType.to_ibis


@classmethod
def dvt_bq_to_ibis(cls, typ: str, nullable: bool = True) -> dt.DataType:
    if typ == "INTERVAL":
        return dt.Interval(unit="s", nullable=nullable)
    return orig_bq_to_ibis(typ, nullable=nullable)


BigQueryType.to_ibis = dvt_bq_to_ibis
