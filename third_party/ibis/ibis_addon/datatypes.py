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

import ibis.expr.datatypes as dt
import numpy as np
import pyarrow

from google.cloud import bigquery
from ibis.backends.bigquery.client import _DTYPE_TO_IBIS_TYPE

# Ibis 5.0.0 has a bug (https://github.com/ibis-project/ibis/issues/5765) 
# for BIGNUMERIC. Once the bug fix is merged, we can remove these two lines.
bigquery._pandas_helpers.BQ_TO_ARROW_SCALARS["BIGNUMERIC"] = pyarrow.decimal256
_DTYPE_TO_IBIS_TYPE["BIGNUMERIC"] = dt.Decimal(76, 38)


def _infer_object_array_dtype(x):
    # Added support for decimal in pandas
    import pandas as pd
    from pandas.api.types import infer_dtype

    classifier = infer_dtype(x, skipna=True)
    if classifier == "mixed":
        value = x.iloc[0] if isinstance(x, pd.Series) else x[0]
        if isinstance(value, (np.ndarray, pd.Series, Sequence, Mapping)):
            return infer(value)
        else:
            return dt.binary
    else:
        return {
            'string': dt.string,
            'bytes': dt.string,
            'floating': dt.float64,
            'integer': dt.int64,
            'mixed-integer': dt.binary,
            'mixed-integer-float': dt.float64,
            'decimal': dt.float64,
            'complex': dt.binary,
            'boolean': dt.boolean,
            'datetime64': dt.timestamp,
            'datetime': dt.timestamp,
            'date': dt.date,
            'decimal': dt.Decimal(),
            'timedelta64': dt.interval,
            'timedelta': dt.interval,
            'time': dt.time,
            'period': dt.binary,
            'empty': dt.null,
            'unicode': dt.string,
        }[classifier]

dt.value._infer_object_array_dtype = _infer_object_array_dtype

