# Copyright 2020 Google Inc.
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

import collections
import numbers
import re
import typing

import third_party.ibis.ibis_oracle.expr.types as ir
from multipledispatch import Dispatcher

import ibis.expr.datatypes as dt

Token = collections.namedtuple('Token', ('type', 'value'))

_token_names = dict(
    (getattr(dt.Tokens, n), n)
    for n in dir(dt.Tokens)
    if n.isalpha() and n.isupper()
)


class CLOB(dt.String):
    scalar = ir.CLOBScalar
    column = ir.CLOBColumn
    __slots__ = ()


class NCLOB(dt.String):
    scalar = ir.NCLOBScalar
    column = ir.NCLOBColumn
    __slots__ = ()


class LONG(dt.String):
    scalar = ir.LONGScalar
    column = ir.LONGColumn
    __slots__ = ()


class BFILE(dt.Binary):
    scalar = ir.BFILEScalar
    column = ir.BFILEColumn
    __slots__ = ()


class RAW(dt.Binary):
    scalar = ir.RAWScalar
    column = ir.RAWColumn
    __slots__ = ()


class LONGRAW(dt.Binary):
    scalar = ir.LONGRAWScalar
    column = ir.LONGRAWColumn
    __slots__ = ()


class Number(dt.DataType):
    scalar = ir.NumberScalar
    column = ir.NumberColumn

    __slots__ = 'precision', 'scale'

    def __init__(
        self, precision: int, scale: int, nullable: bool = True
    ) -> None:
        print(precision)
        if not isinstance(precision, numbers.Integral):
            raise TypeError('Number type precision must be an integer')
        if not isinstance(scale, numbers.Integral):
            raise TypeError('Number type scale must be an integer')
        if precision < 0:
            raise ValueError('Number type precision cannot be negative')
        if not precision:
            raise ValueError('Number type precision cannot be zero')
        if scale < 0:
            raise ValueError('Number type scale cannot be negative')
        if precision < scale:
            raise ValueError(
                'Number type precision must be greater than or equal to '
                'scale. Got precision={:d} and scale={:d}'.format(
                    precision, scale
                )
            )
        super().__init__(nullable=nullable)
        self.precision = precision  # type: int
        self.scale = scale  # type: int

    def __str__(self) -> str:
        return '{}({:d}, {:d})'.format(
            self.name.lower(), self.precision, self.scale
        )

    @property
    def largest(self) -> 'Number':
        return Number(38, self.scale)


clob = CLOB()
nclob = NCLOB()
long = LONG()
bfile = BFILE()
raw = RAW()
longraw = LONGRAW()


class Token_Oracle(dt.Tokens):
    __slots__ = ()
    CLOB = 1
    NCLOB = 2
    LONG = 3
    NUMBER = 4
    BFILE = 5
    RAW = 6
    LONGRAW = 7
    VARCHAR = 8
    CHAR = 9

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
            ('varchar', 'char', 'number', 'time',),
            (
                Token_Oracle.VARCHAR,
                Token_Oracle.CHAR,
                Token_Oracle.NUMBER,
                Token_Oracle.TIME,
            ),
        )
    ]
)
_TYPE_RULES.update(dt._TYPE_RULES)
_TYPE_KEYS = tuple(_TYPE_RULES.keys())
_TYPE_PATTERN = re.compile('|'.join(_TYPE_KEYS), flags=re.IGNORECASE)


class TypeParser_Oracle(dt.TypeParser):
    def type(self) -> dt.DataType:
        if self._accept(Token_Oracle.CLOB):
            return CLOB()
        elif self._accept(Token_Oracle.NCLOB):
            return NCLOB()
        elif self._accept(Token_Oracle.LONG):
            return LONG()
        elif self._accept(Token_Oracle.NUMBER):
            if self._accept(Token_Oracle.LPAREN):
                self._expect(Token_Oracle.INTEGER)
                assert self.tok is not None
                precision = self.tok.value
                self._expect(Token_Oracle.COMMA)
                self._expect(Token_Oracle.INTEGER)
                scale = self.tok.value
                self._expect(Token_Oracle.RPAREN)
            else:
                precision = 9
                scale = 0
            return Number(precision, scale)
        elif self._accept(Token_Oracle.BFILE):
            return BFILE()
        elif self._accept(Token_Oracle.RAW):
            return RAW()
        elif self._accept(Token_Oracle.LONGRAW):
            return LONGRAW()
        else:
            raise SyntaxError('Type cannot be parsed: {}'.format(self.text))


dtype = Dispatcher('dtype')
validate_type = dtype

castable = Dispatcher('castable')


@castable.register(CLOB, CLOB)
def can_cast_clob(source, target, **kwargs):
    return True


@castable.register(NCLOB, NCLOB)
def can_cast_nclob(source, target, **kwargs):
    return True


@castable.register(LONG, LONG)
def can_cast_long(source, target, **kwargs):
    return True


@castable.register(BFILE, BFILE)
def can_cast_bfile(source, target, **kwargs):
    return True


@castable.register(RAW, RAW)
def can_cast_raw(source, target, **kwargs):
    return True


@castable.register(LONGRAW, LONGRAW)
def can_cast_longraw(source, target, **kwargs):
    return True


@castable.register(Number, Number)
def can_cast_numbers(source: Number, target: Number, **kwargs) -> bool:
    return (
        target.precision >= source.precision and target.scale >= source.scale
    )
