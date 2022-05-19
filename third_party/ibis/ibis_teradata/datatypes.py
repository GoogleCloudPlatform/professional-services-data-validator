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

        print('Unsupported Date Type Seen: "%s"' % col_data["Type"])
        return "VARCHAR"

    @classmethod
    def to_ibis_from_CV(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.string

        return "VARCHAR"

    @classmethod
    def to_ibis_from_N(cls, col_data, return_ibis_type=True):
        return cls.to_ibis_from_D(col_data, return_ibis_type=return_ibis_type)

    @classmethod
    def to_ibis_from_D(cls, col_data, return_ibis_type=True):
        precision = int(col_data.get("DecimalTotalDigits", col_data.get("Decimal Total Digits", 20)))
        scale = int(col_data.get("DecimalFractionalDigits", col_data.get("Decimal Fractional Digits", 4)))
        if return_ibis_type:
            return dt.float64
        value_type = "DECIMAL(%d, %d)" % (precision, scale)
        return value_type

    @classmethod
    def to_ibis_from_F(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.float64
        return "FLOAT"

    @classmethod
    def to_ibis_from_I(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.int64
        return "INT"

    @classmethod
    def to_ibis_from_I1(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.int8
        return "INT"

    @classmethod
    def to_ibis_from_I2(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.int8
        return "INT"

    @classmethod
    def to_ibis_from_I8(cls, col_data, return_ibis_type=True):
        if return_ibis_type:
            return dt.int16
        return "INT"

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
    # return ibis_type_to_teradata_type(t, TypeTranslationContext())
    return "VARCHAR(255)"


@ibis_type_to_teradata_type.register(str, TypeTranslationContext)
def trans_string_context(datatype, context):
    return "VARCHAR"


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
