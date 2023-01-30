from __future__ import annotations
from ibis.expr.datatypes import *
import ibis.expr.datatypes as dt


class Decimal(Numeric):
    """Fixed-precision decimal values."""

    precision = optional(instance_of(int))
    """The number of decimal places values of this type can hold."""

    scale = optional(instance_of(int))
    """The number of values after the decimal point."""

    scalar = ir.DecimalScalar
    column = ir.DecimalColumn

    def __init__(
        self,
        precision: int | None = None,
        scale: int | None = None,
        **kwargs: Any,
    ) -> None:
        if precision is not None:
            if not isinstance(precision, numbers.Integral):
                raise TypeError(
                    "Decimal type precision must be an integer; "
                    f"got {type(precision)}"
                )
            if precision < 0:
                raise ValueError("Decimal type precision cannot be negative")
            if not precision:
                raise ValueError("Decimal type precision cannot be zero")
        if scale is not None:
            if not isinstance(scale, numbers.Integral):
                raise TypeError("Decimal type scale must be an integer")
            if scale < 0:
                raise ValueError("Decimal type scale cannot be negative")
            if precision is not None and precision < scale:
                raise ValueError(
                    "Decimal type precision must be greater than or equal to "
                    "scale. Got precision={:d} and scale={:d}".format(precision, scale)
                )
        super().__init__(precision=precision, scale=scale, **kwargs)

    @property
    def largest(self):
        """Return the largest type of decimal."""
        return self.__class__(
            precision=max(self.precision, 38) if self.precision is not None else None,
            scale=max(self.scale, 2) if self.scale is not None else None,
        )

    @property
    def _pretty_piece(self) -> str:
        precision = self.precision
        scale = self.scale
        if precision is None and scale is None:
            return ""

        args = [str(precision) if precision is not None else "_"]

        if scale is not None:
            args.append(str(scale))

        return f"({', '.join(args)})"


# decimal = Decimal()
dt.Decimal = Decimal()
