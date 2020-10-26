import ibis.expr.types as tp


class TIMESTAMP_NTZValue(tp.StringValue):
    pass


class TIMESTAMP_NTZScalar(tp.StringScalar, TIMESTAMP_NTZValue):
    pass


class TIMESTAMP_NTZColumn(tp.StringColumn, TIMESTAMP_NTZValue):
    pass


