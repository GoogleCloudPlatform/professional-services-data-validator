import collections
from multipledispatch import Dispatcher
import third_party.ibis.ibis_snowflake.expr.types as tp
from typing import TypeVar

import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import re
import typing

Token = collections.namedtuple('Token', ('type', 'value'))

_token_names = dict(
    (getattr(dt.Tokens, n), n)
    for n in dir(dt.Tokens)
    if n.isalpha() and n.isupper()
)


class TIMESTAMP_NTZ(dt.String):
    pass


timestamp_ntz = TIMESTAMP_NTZ()


class Array(dt.Variadic):
    scalar = ir.ArrayScalar
    column = ir.ArrayColumn
    __slots__ = ()


class Token_Snowflake(dt.Tokens):
    __slots__ = ()
    TIMESTAMP_NTZ = 1
    ARRAY = 2
    LBRACKET = 3
    RBRACKET = 4

    @staticmethod
    def name(value):
        return _token_names[value]


_TYPE_RULES = collections.OrderedDict(
    [
        # decimal + complex types
        (
            '(?P<{}>{})'.format(token.upper(), token),
            typing.cast(
                dt.Action, lambda token, toktype=toktype: Token(toktype, token)
            ),
        )
        for token, toktype in zip(
        (
            'timestamp_ntz',
            'array',
        ),
        (
            Token_Snowflake.TIMESTAMP_NTZ,
            Token_Snowflake.ARRAY,
        ),
    )
    ]
)
_TYPE_RULES.update(dt._TYPE_RULES)
_TYPE_KEYS = tuple(_TYPE_RULES.keys())
_TYPE_PATTERN = re.compile('|'.join(_TYPE_KEYS), flags=re.IGNORECASE)


class TypeParser_Snowflake(dt.TypeParser):
    def type(self) -> dt.DataType:
        if self._accept(Token_Snowflake.TIMESTAMP_NTZ):
            return TIMESTAMP_NTZ()
        elif self._accept(Token_Snowflake.ARRAY):
            self._expect(Token_Snowflake.LBRACKET)

            value_type = self.type()

            self._expect(Token_Snowflake.RBRACKET)
            return Array(value_type)
        else:
            raise SyntaxError('Type cannot be parsed: {}'.format(self.text))


dtype = Dispatcher('dtype')
validate_type = dtype

castable = Dispatcher('castable')

Collection = TypeVar('Collection', Array, dt.Set)


@castable.register(Array, Array)
def can_cast_variadic(
        source: Collection, target: Collection, **kwargs
) -> bool:
    return castable(source.value_type, target.value_type)


"""
@castable.register(TIMESTAMP_NTZ, TIMESTAMP_NTZ)
def can_cast_timestamp_ntz(source, target, **kwargs):
    return True
"""