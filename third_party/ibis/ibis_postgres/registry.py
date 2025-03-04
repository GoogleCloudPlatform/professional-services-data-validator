# Copyright 2024 Google Inc.
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


def sa_format_hashbytes(translator, op):
    arg = translator.translate(op.arg)
    convert = sa.func.convert_to(arg, sa.sql.literal_column("'UTF8'"))
    hash_func = sa.func.sha256(convert)
    return sa.func.encode(hash_func, sa.sql.literal_column("'hex'"))


def sa_epoch_seconds(translator, op):
    """Override for standard ExtractEpochSeconds but catering for larger second values.

    This expression also truncates fractional seconds from the incoming datetime.
    This matches behaviour on other SQL engines' epoch seconds expressions."""
    arg = translator.translate(op.arg)
    return sa.cast(
        sa.extract("epoch", sa.func.date_trunc(sa.sql.literal_column("'second'"), arg)),
        sa.BIGINT,
    )
