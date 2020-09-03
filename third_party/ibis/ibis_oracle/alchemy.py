import sqlalchemy as sa
import third_party.ibis.ibis_oracle.expr.datatypes as dt
from sqlalchemy.dialects.oracle.base import OracleDialect

import ibis.expr.datatypes as dt11
import ibis.sql.alchemy as s_al

_ibis_type_to_sqla = {
    dt.CLOB: sa.CLOB,
    # dt.NCLOB: sa.NCLOB,
    # dt.LONG: sa.LONG,
    # dt.NUMBER: sa.NUMBER,
    # dt.BFILE: sa.BFILE,
    # dt.RAW: sa.RAW,
    dt.LONGRAW: sa.Binary,
}
_ibis_type_to_sqla.update(s_al._ibis_type_to_sqla)


def _to_sqla_type(itype, type_map=None):
    if type_map is None:
        type_map = _ibis_type_to_sqla
    if isinstance(itype, dt11.Decimal):
        return sa.types.NUMERIC(itype.precision, itype.scale)
    elif isinstance(itype, dt11.Date):
        return sa.Date()
    elif isinstance(itype, dt11.Timestamp):
        # SQLAlchemy DateTimes do not store the timezone, just whether the db
        # supports timezones.
        return sa.TIMESTAMP(bool(itype.timezone))
    elif isinstance(itype, dt11.Array):
        ibis_type = itype.value_type
        if not isinstance(ibis_type, (dt11.Primitive, dt11.String)):
            raise TypeError(
                'Type {} is not a primitive type or string type'.format(
                    ibis_type
                )
            )
        return sa.ARRAY(_to_sqla_type(ibis_type, type_map=type_map))
    else:
        return type_map[type(itype)]


class AlchemyExprTranslator(s_al.AlchemyExprTranslator):
    s_al.AlchemyExprTranslator._type_map = _ibis_type_to_sqla


class AlchemyDialect(s_al.AlchemyDialect):
    s_al.translator = AlchemyExprTranslator


@dt.dtype.register(OracleDialect, sa.dialects.oracle.CLOB)
def sa_oracle_CLOB(_, satype, nullable=True):
    return dt.CLOB(nullable=nullable)


@dt.dtype.register(OracleDialect, sa.dialects.oracle.NCLOB)
def sa_oracle_NCLOB(_, satype, nullable=True):
    return dt.NCLOB(nullable=nullable)


@dt.dtype.register(OracleDialect, sa.dialects.oracle.LONG)
def sa_oracle_LONG(_, satype, nullable=True):
    return dt.LONG(nullable=nullable)


@dt.dtype.register(OracleDialect, sa.dialects.oracle.NUMBER)
def sa_oracle_NUMBER(_, satype, nullable=True):
    return dt.Number(satype.precision, satype.scale, nullable=nullable)


@dt.dtype.register(OracleDialect, sa.dialects.oracle.BFILE)
def sa_oracle_BFILE(_, satype, nullable=True):
    return dt.BFILE(nullable=nullable)


@dt.dtype.register(OracleDialect, sa.dialects.oracle.RAW)
def sa_oracle_RAW(_, satype, nullable=True):
    return dt.RAW(nullable=nullable)


@dt.dtype.register(OracleDialect, sa.types.BINARY)
def sa_oracle_LONGRAW(_, satype, nullable=True):
    return dt.LONGRAW(nullable=nullable)
