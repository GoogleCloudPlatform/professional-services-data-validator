# Copyright 2025 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sqlalchemy as sa

from ibis.backends.base.sql.alchemy.registry import _cast as sa_fixed_cast
from third_party.ibis.ibis_mssql.registry import sa_cast_mssql


def sa_cast_sybase(t, op):
    arg = op.arg
    typ = op.to
    arg_dtype = arg.output_dtype

    sa_arg = t.translate(arg)

    # Specialize going from DECIMAL(p,s>0) to string
    if (
        arg_dtype.is_decimal()
        and arg_dtype.scale
        and arg_dtype.scale > 0
        and typ.is_string()
    ):
        # Prevent SQL Server specific workaround.
        return sa_fixed_cast(t, op)

    # Follow our SQL Server code path.
    return sa_cast_mssql(t, op)


def sa_format_string_length(translator, op):
    """Sybase string length function is char_length()."""
    arg = translator.translate(op.arg)
    return sa.func.char_length(arg)


def sa_epoch_seconds(translator, op):
    """Override for standard ExtractEpochSeconds but catering for larger second values."""
    arg = translator.translate(op.arg)
    return sa.cast(
        sa.func.datediff(
            sa.text("SECOND"), sa.literal_column("'1970-01-01 00:00:00'"), arg
        ),
        sa.BIGINT,
    )
