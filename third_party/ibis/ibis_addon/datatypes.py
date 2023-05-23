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
from sqlalchemy.dialects import mssql
from sqlalchemy.dialects.mssql.base import MSDialect
from ibis.backends.mssql.datatypes import _MSSQL_TYPE_MAP


# from ibis.backends.pandas.execution.constants import IBIS_TYPE_TO_PANDAS_TYPE
# import numpy as np
# import pyarrow as pa

# Register additional MSSQL data types
@dt.dtype.register(MSDialect, (mssql.VARBINARY, mssql.IMAGE))
def sa_mssql_binary(_, satype, nullable=True):
    return dt.Binary(nullable=nullable)

@dt.dtype.register(MSDialect, (mssql.NCHAR, mssql.NTEXT, mssql.NVARCHAR))
def sa_mssql_binary(_, satype, nullable=True):
    return dt.String(nullable=nullable)

# Needs to be VARCHAR insteaad of NVARCHAR for Hash function
_MSSQL_TYPE_MAP[dt.String] = mssql.VARCHAR

# PyArrow support for decimal; should be fixed in Ibis v5.1.0+
# @dt.dtype.register((pa.Decimal128Type, pa.Decimal256Type))
# def from_pyarrow_decimal(
#     arrow_type: pa.Decimal128Type | pa.Decimal256Type, nullable: bool = True
# ) -> dt.Decimal:
#     return dt.Decimal(
#         precision=arrow_type.precision, scale=arrow_type.scale, nullable=nullable
#     )


# def _infer_object_array_dtype(x):
#     # Added support for decimal in pandas
#     import ibis.backends.pyarrow.datatypes  # noqa: F401

#     try:
#         pa_type = pa.array(x, from_pandas=True).type
#     except pa.ArrowInvalid:
#         try:
#             # handle embedded series objects
#             return dt.highest_precedence(map(infer, x))
#         except IbisTypeError:
#             # we can still have a type error, e.g., float64 and string in the
#             # same array
#             return dt.unknown
#     except pa.ArrowTypeError:
#         # arrow can't infer the type
#         return dt.unknown
#     else:
#         if isinstance(pa_type, pa.Decimal256Type):
#             return dt.Decimal(10,0, nullable=True)
#         return dt.dtype(pa_type)

# dt.value._infer_object_array_dtype = _infer_object_array_dtype
# IBIS_TYPE_TO_PANDAS_TYPE[dt.Decimal(precision=10, scale=0, nullable=True)] = np.float64

