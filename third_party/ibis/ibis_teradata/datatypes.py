from multipledispatch import Dispatcher

import ibis.expr.datatypes as dt


class TypeTranslationContext:
    """A tag class to allow alteration of the way a particular type is
    translated.
    Notes
    -----
    This is used to raise an exception when INT64 types are encountered to
    avoid suprising results due to BigQuery's handling of INT64 types in
    JavaScript UDFs.
    """

    __slots__ = ()


class UDFContext(TypeTranslationContext):
    __slots__ = ()


# RAW TYPES MAPPED TODO all of these
# CASE ColumnType
#     WHEN 'BF' THEN 'BYTE('            || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
#     WHEN 'BV' THEN 'VARBYTE('         || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
#     WHEN 'CF' THEN 'CHAR('            || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
#     WHEN 'DA' THEN 'DATE'
#     WHEN 'F ' THEN 'FLOAT'
#     WHEN 'I1' THEN 'BYTEINT'
#     WHEN 'I2' THEN 'SMALLINT'
#     WHEN 'I8' THEN 'BIGINT'
#     WHEN 'I ' THEN 'INTEGER'
#     WHEN 'AT' THEN 'TIME('            || TRIM(DecimalFractionalDigits) || ')'
#     WHEN 'TS' THEN 'TIMESTAMP('       || TRIM(DecimalFractionalDigits) || ')'
#     WHEN 'TZ' THEN 'TIME('            || TRIM(DecimalFractionalDigits) || ')' || ' WITH TIME ZONE'
#     WHEN 'SZ' THEN 'TIMESTAMP('       || TRIM(DecimalFractionalDigits) || ')' || ' WITH TIME ZONE'
#     WHEN 'YR' THEN 'INTERVAL YEAR('   || TRIM(DecimalTotalDigits) || ')'
#     WHEN 'YM' THEN 'INTERVAL YEAR('   || TRIM(DecimalTotalDigits) || ')'      || ' TO MONTH'
#     WHEN 'MO' THEN 'INTERVAL MONTH('  || TRIM(DecimalTotalDigits) || ')'
#     WHEN 'DY' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits) || ')'
#     WHEN 'DH' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits) || ')'      || ' TO HOUR'
#     WHEN 'DM' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits) || ')'      || ' TO MINUTE'
#     WHEN 'DS' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits) || ')'      || ' TO SECOND('
#                                       || TRIM(DecimalFractionalDigits) || ')'
#     WHEN 'HR' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits) || ')'
#     WHEN 'HM' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits) || ')'      || ' TO MINUTE'
#     WHEN 'HS' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits) || ')'      || ' TO SECOND('
#                                       || TRIM(DecimalFractionalDigits) || ')'
#     WHEN 'MI' THEN 'INTERVAL MINUTE(' || TRIM(DecimalTotalDigits) || ')'
#     WHEN 'MS' THEN 'INTERVAL MINUTE(' || TRIM(DecimalTotalDigits) || ')'      || ' TO SECOND('
#                                       || TRIM(DecimalFractionalDigits) || ')'
#     WHEN 'SC' THEN 'INTERVAL SECOND(' || TRIM(DecimalTotalDigits) || ','
#                                       || TRIM(DecimalFractionalDigits) || ')'
#     WHEN 'BO' THEN 'BLOB('            || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
#     WHEN 'CO' THEN 'CLOB('            || TRIM(CAST(ColumnLength AS INTEGER)) || ')'

#     WHEN 'PD' THEN 'PERIOD(DATE)'
#     WHEN 'PM' THEN 'PERIOD(TIMESTAMP('|| TRIM(DecimalFractionalDigits) || ')' || ' WITH TIME ZONE'
#     WHEN 'PS' THEN 'PERIOD(TIMESTAMP('|| TRIM(DecimalFractionalDigits) || '))'
#     WHEN 'PT' THEN 'PERIOD(TIME('     || TRIM(DecimalFractionalDigits) || '))'
#     WHEN 'PZ' THEN 'PERIOD(TIME('     || TRIM(DecimalFractionalDigits) || '))' || ' WITH TIME ZONE'
#     WHEN 'UT' THEN COALESCE(ColumnUDTName,  '<Unknown> ' || ColumnType)

#     WHEN '++' THEN 'TD_ANYTYPE'
#     WHEN 'N'  THEN 'NUMBER('          || CASE WHEN DecimalTotalDigits = -128 THEN '*' ELSE TRIM(DecimalTotalDigits) END
#                                       || CASE WHEN DecimalFractionalDigits IN (0, -128) THEN '' ELSE ',' || TRIM(DecimalFractionalDigits) END
#                                       || ')'
#     WHEN 'A1' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
#     WHEN 'AN' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)

#     ELSE '<Unknown> ' || ColumnType
#   END


class TeradataTypeTranslator(object):
    def __init__(self):
        pass

    @classmethod
    def to_ibis(cls, col_data):
        td_type = col_data["Type"].strip()

        to_ibis_func_name = "to_ibis_from_{}".format(td_type)
        if hasattr(cls, to_ibis_func_name):
            return getattr(cls, to_ibis_func_name)(col_data, return_ibis_type=True)

        return cls.to_ibis_from_other(col_data, return_ibis_type=True)

    @classmethod
    def to_ibis_from_other(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.string

        print(
            'Unsupported Date Type Seen; "%s" (Also I should be a log)'
            % col_data["Type"]
        )
        return "VARCHAR"

    @classmethod
    def to_ibis_from_CV(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.string

        return "VARCHAR"

    @classmethod
    def to_ibis_from_D(cls, col_data, return_ibis_type=True):
        precision = int(col_data.get("DecimalTotalDigits", col_data.get("Decimal Total Digits", 20)))
        scale = int(col_data.get("DecimalFractionalDigits", col_data.get("Decimal Fractional Digits", 4)))
        if return_ibis_type:
            return dt.float64
        value_type = "DECIMAL(%d, %d)" % (precision, scale)
        return value_type

    @classmethod
    def to_ibis_from_DA(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.date

        return "DATE"

    @classmethod
    def to_ibis_from_TS(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.timestamp

        return "TIMESTAMP"

    @classmethod
    def to_ibis_from_SZ(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.timestamp

        return "TIMESTAMP"


ibis_type_to_teradata_type = Dispatcher("ibis_type_to_teradata_type")


@ibis_type_to_teradata_type.register(str)
def trans_string_default(datatype):
    return ibis_type_to_teradata_type(dt.dtype(datatype))


@ibis_type_to_teradata_type.register(dt.DataType)
def trans_default(t):
    return ibis_type_to_teradata_type(t, TypeTranslationContext())


@ibis_type_to_teradata_type.register(str, TypeTranslationContext)
def trans_string_context(datatype, context):
    return ibis_type_to_teradata_type(dt.dtype(datatype), context)


@ibis_type_to_teradata_type.register(dt.Floating, TypeTranslationContext)
def trans_float64(t, context):
    return "FLOAT64"


@ibis_type_to_teradata_type.register(dt.Integer, TypeTranslationContext)
def trans_integer(t, context):
    return "INT64"


@ibis_type_to_teradata_type.register(dt.UInt64, (TypeTranslationContext, UDFContext))
def trans_lossy_integer(t, context):
    raise TypeError("Conversion from uint64 to BigQuery integer type (int64) is lossy")


@ibis_type_to_teradata_type.register(dt.Date, TypeTranslationContext)
def trans_date(t, context):
    return "DATE"


@ibis_type_to_teradata_type.register(dt.Timestamp, TypeTranslationContext)
def trans_timestamp(t, context):
    if t.timezone is not None:
        raise TypeError("BigQuery does not support timestamps with timezones")
    return "TIMESTAMP"


@ibis_type_to_teradata_type.register(dt.DataType, TypeTranslationContext)
def trans_type(t, context):
    return str(t).upper()


@ibis_type_to_teradata_type.register(dt.Decimal, TypeTranslationContext)
def trans_numeric(t, context):
    return "NUMERIC"
