import ibis.expr.types as tp


class CLOBValue(tp.StringValue):
    pass  # noqa: E701,E302


class CLOBScalar(tp.StringScalar, CLOBValue):
    pass  # noqa: E701,E302


class CLOBColumn(tp.StringColumn, CLOBValue):
    pass  # noqa: E701,E302


# -------------------------------------------


class NCLOBValue(tp.StringValue):
    pass  # noqa: E701,E302


class NCLOBScalar(tp.StringScalar, NCLOBValue):
    pass  # noqa: E701,E302


class NCLOBColumn(tp.StringColumn, NCLOBValue):
    pass  # noqa: E701,E302


# -------------------------------------------


class LONGValue(tp.StringValue):
    pass  # noqa: E701,E302


class LONGScalar(tp.StringScalar, LONGValue):
    pass  # noqa: E701,E302


class LONGColumn(tp.StringColumn, LONGValue):
    pass  # noqa: E701,E302


# --------------Binary type-----------------------------


class BFILEValue(tp.BinaryValue):
    pass  # noqa: E701,E302


class BFILEScalar(tp.BinaryScalar, BFILEValue):
    pass  # noqa: E701,E302


class BFILEColumn(tp.BinaryColumn, BFILEValue):
    pass  # noqa: E701,E302


# -------------------------------------------


class RAWValue(tp.BinaryValue):
    pass  # noqa: E701,E302


class RAWScalar(tp.BinaryScalar, RAWValue):
    pass  # noqa: E701,E302


class RAWColumn(tp.BinaryColumn, RAWValue):
    pass  # noqa: E701,E302


# -------------------------------------------


class LONGRAWValue(tp.BinaryValue):
    pass  # noqa: E701,E302


class LONGRAWScalar(tp.BinaryScalar, LONGRAWValue):
    pass  # noqa: E701,E302


class LONGRAWColumn(tp.BinaryColumn, LONGRAWValue):
    pass  # noqa: E701,E302


# -------------Number type------------------------------


class NumberValue(tp.NumericValue):
    pass  # noqa: E701,E302


class NumberScalar(tp.NumericScalar, NumberValue):
    pass  # noqa: E701,E302


class NumberColumn(tp.NumericColumn, NumberValue):
    pass  # noqa: E701,E302
