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

import decimal
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
import numpy as np
import pandas as pd
import pyarrow

from google.cloud import bigquery
from ibis_bigquery.client import _DTYPE_TO_IBIS_TYPE
from ibis_bigquery.datatypes import ibis_type_to_bigquery_type, TypeTranslationContext, trans_numeric_udf
from ibis.backends.pandas.client import _inferable_pandas_dtypes, infer_pandas_schema, infer_pandas_dtype
from ibis.backends.pandas.execution.constants import IBIS_TYPE_TO_PANDAS_TYPE


# BigQuery BIGNUMERIC support needs to be pushed to Ibis
bigquery._pandas_helpers.BQ_TO_ARROW_SCALARS["BIGNUMERIC"] = pyarrow.decimal256
_DTYPE_TO_IBIS_TYPE["BIGNUMERIC"] = dt.Decimal(38,9)
_DTYPE_TO_IBIS_TYPE["NUMERIC"] = dt.Decimal(38,9)


# Ibis Pandas Client Inference
# Still Open: floating, integer, mixed-integer,
# mixed-integer-float, complex, categorical, timedelta64, timedelta, period
IBIS_TYPE_TO_PANDAS_TYPE[dt.Decimal(10,0)] = np.dtype(decimal.Decimal)
_inferable_pandas_dtypes['decimal'] = dt.Decimal(10,0)

_inferable_pandas_dtypes['date'] = dt.date
_inferable_pandas_dtypes['datetime64'] = dt.timestamp
_inferable_pandas_dtypes['datetime'] = dt.timestamp
_inferable_pandas_dtypes['time'] = dt.time

# Patch Bug in Ibis BQ that was fixed in version 2.1.1
@ibis_type_to_bigquery_type.register(dt.Decimal, TypeTranslationContext)
def trans_numeric(t, context):
    if (t.precision, t.scale) != (38, 9):
        raise TypeError(
            'BigQuery only supports decimal types with precision of 38 and '
            'scale of 9'
        )
    return 'NUMERIC'

trans_numeric_udf = trans_numeric

@sch.infer.register(pd.DataFrame)
def infer_pandas_schema_incl_decimals(df, schema=None):
    schema = schema if schema is not None else {}

    pairs = []
    for column_name, pandas_dtype in df.dtypes.iteritems():
        if not isinstance(column_name, str):
            raise TypeError(
                'Column names must be strings to use the pandas backend'
            )

        if column_name in schema:
            ibis_dtype = dt.dtype(schema[column_name])
        elif pandas_dtype == np.object_:
            inferred_dtype = infer_pandas_dtype(df[column_name], skipna=True)
            if inferred_dtype == 'mixed':
                raise TypeError(
                    'Unable to infer type of column {0!r}. Try instantiating '
                    'your table from the client with client.table('
                    "'my_table', schema={{{0!r}: <explicit type>}})".format(
                        column_name
                    )
                )
            ibis_dtype = _inferable_pandas_dtypes[inferred_dtype]
        else:
            ibis_dtype = dt.dtype(pandas_dtype)

        pairs.append((column_name, ibis_dtype))

    return sch.schema(pairs)

infer_pandas_schema = infer_pandas_schema_incl_decimals