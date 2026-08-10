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

from collections.abc import Sized
from datetime import date
import dateutil.parser
import numpy as np
import pandas as pd

from ibis.backends.pandas.dispatch import execute_node
import ibis.backends.pandas.execution.constants as pandas_constants
import ibis.backends.pandas.execution.generic as gp
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.formats.pandas as fp


class PandasTypeMapping(dict):
    def __getitem__(self, key):
        if isinstance(key, dt.Decimal):
            return object
        if isinstance(key, dt.Date):
            return object
        try:
            return super().__getitem__(key)
        except KeyError:
            if isinstance(key, dt.DataType):
                for k, v in self.items():
                    if isinstance(key, type(k)) or key == k:
                        return v
            raise

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        if isinstance(key, (dt.Decimal, dt.Date)):
            return True
        if super().__contains__(key):
            return True
        if isinstance(key, dt.DataType):
            for k in self.keys():
                if isinstance(key, type(k)) or key == k:
                    return True
        return False


pandas_constants.IBIS_TYPE_TO_PANDAS_TYPE = PandasTypeMapping(
    pandas_constants.IBIS_TYPE_TO_PANDAS_TYPE
)

# Monkey-patch pandas backend compute_row_reduction to handle string/bytes/dict scalars correctly
orig_compute_row_reduction = gp.compute_row_reduction


def dvt_compute_row_reduction(func, values, **kwargs):
    final_sizes = {
        len(x)
        for x in values
        if isinstance(x, Sized) and not isinstance(x, (str, bytes, dict))
    }
    if not final_sizes:
        return func(values)
    (final_size,) = final_sizes
    raw = func(list(map(gp.promote_to_sequence(final_size), values)), **kwargs)
    return pd.Series(raw).squeeze()


gp.compute_row_reduction = dvt_compute_row_reduction

# Monkey-patch PandasData.convert_Date to handle out-of-bounds / tricky dates safely
orig_convert_Date = fp.PandasData.convert_Date


def dvt_convert_Date(s, dtype, pandas_type):
    if isinstance(s.dtype, pd.DatetimeTZDtype):
        s = s.dt.tz_convert("UTC").dt.tz_localize(None)
    try:
        return s.astype(pandas_type).dt.normalize()
    except Exception:

        def to_date_safe(x):
            if isinstance(x, date):
                return x
            elif isinstance(x, str):
                try:
                    parts = list(map(int, x.split("-")))
                    return date(*parts)
                except Exception:
                    try:
                        return dateutil.parser.parse(x).date()
                    except Exception:
                        return x
                return x

        return s.map(to_date_safe)


fp.PandasData.convert_Date = dvt_convert_Date


@execute_node.register(ops.Cast, pd.Series, dt.String)
def execute_cast_series_string(op, data, type, **kwargs):
    """Cast a Pandas Series to string, representing binary data as hex.

    This specializes the cast operation from dt.Binary to dt.String to prevent
    UnicodeDecodeError crashes in Pandas when the binary data contains bytes
    that are not valid UTF-8.
    """
    if op.arg.dtype.is_binary():
        return data.map(lambda x: x.hex() if isinstance(x, bytes) else x)
    return data.astype(pandas_constants.IBIS_TYPE_TO_PANDAS_TYPE[type])
