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


from google.cloud import bigquery
from ibis_bigquery.client import _DTYPE_TO_IBIS_TYPE
import ibis.expr.datatypes as dt
from ibis.backends.pandas.client import _inferable_pandas_dtypes
import pyarrow


# BigQuery BIGNUMERIC support needs to be pushed to Ibis
bigquery._pandas_helpers.BQ_TO_ARROW_SCALARS["BIGNUMERIC"] = pyarrow.decimal256
_DTYPE_TO_IBIS_TYPE["BIGNUMERIC"] = dt.float64
_DTYPE_TO_IBIS_TYPE["NUMERIC"] = dt.float64


# Ibis Pandas Clent Inference
# Still Open: floating, integer, mixed-integer,
# mixed-integer-float, complex, categorical, timedelta64, timedelta, period
_inferable_pandas_dtypes['date'] = dt.date
_inferable_pandas_dtypes['datetime64'] = dt.timestamp
_inferable_pandas_dtypes['datetime'] = dt.timestamp
_inferable_pandas_dtypes['time'] = dt.time
