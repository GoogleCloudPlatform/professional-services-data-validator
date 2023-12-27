# Copyright 2023 Google Inc.
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
import ibm_db_dbi

import ibis.expr.datatypes as dt
from ibis.backends.base.sql.alchemy.datatypes import ibis_type_to_sqla

# Types from https://github.com/ibmdb/python-ibmdb/blob/master/IBM_DB/ibm_db/ibm_db_dbi.py
_type_mapping = {
    ibm_db_dbi.STRING: dt.String,
    ibm_db_dbi.NUMBER: dt.Int32,
    ibm_db_dbi.TEXT: dt.String,
    ibm_db_dbi.BINARY: dt.Binary,
    ibm_db_dbi.BIGINT: dt.Int64,
    ibm_db_dbi.FLOAT: dt.Float64,
    ibm_db_dbi.DECIMAL: dt.Decimal,
    ibm_db_dbi.DATE: dt.Date,
    ibm_db_dbi.TIME: dt.Time,
    ibm_db_dbi.DATETIME: dt.Timestamp,
    ibm_db_dbi.BOOLEAN: dt.Boolean,
    ibm_db_dbi.ROWID: dt.String,
}

ibis_type_to_sqla[dt.String] = sa.sql.sqltypes.String(length=3000)


def _get_type(typename) -> dt.DataType:
    typ = _type_mapping.get(typename)
    if typ is None:
        raise NotImplementedError(f"DB2 type {typename} is not supported")
    return typ
