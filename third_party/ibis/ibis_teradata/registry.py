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
import itertools
import locale
import platform
import re
import string
import warnings
from multipledispatch import Dispatcher

import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
from ibis.common.exceptions import UnsupportedOperationError
from ibis.backends.base.sql.registry import operation_registry, fixed_arity
from third_party.ibis.ibis_teradata.datatypes import ibis_type_to_teradata_type

_operation_registry = operation_registry.copy()


def _quote_identifier(name, quotechar='"', force=False):
    if force or name.count(" ") or name in identifiers.base_identifiers:
        return "{0}{1}{0}".format(quotechar, name)
    else:
        return name


teradata_cast = Dispatcher("teradata_cast")


@teradata_cast.register(str, dt.Timestamp, dt.Integer)
def teradata_cast_timestamp_to_integer(compiled_arg, from_, to):
    return "UNIX_MICROS({})".format(compiled_arg)


@teradata_cast.register(str, dt.Binary, dt.String)
def teradata_cast_binary_to_string(compiled_arg, from_, to):
    return "LOWER(FROM_BYTES({}, 'base16'))".format(compiled_arg)


@teradata_cast.register(str, dt.String, dt.Binary)
def teradata_cast_string_to_binary(compiled_arg, from_, to):
    return "TO_BYTES({}, 'base16')".format(compiled_arg)


@teradata_cast.register(str, dt.String, dt.Boolean)
def teradata_cast_string_to_bool(compiled_arg, from_, to):
    return (
        "CASE  "
        f"WHEN {compiled_arg} IN ('0','N') THEN 0 "
        f"WHEN {compiled_arg} IN ('1','Y') THEN 1 "
        "ELSE TO_NUMBER(NULL) END"
    )


def _teradata_cast_number_to_bool(compiled_arg, from_, to):
    return (
        "CASE  "
        f"WHEN {compiled_arg} IN (0) THEN 0 "
        f"WHEN {compiled_arg} IN (1) THEN 1 "
        "ELSE TO_NUMBER(NULL) END"
    )


@teradata_cast.register(str, dt.Decimal, dt.Boolean)
def teradata_cast_decimal_to_bool(compiled_arg, from_, to):
    return _teradata_cast_number_to_bool(compiled_arg, from_, to)


@teradata_cast.register(str, dt.Integer, dt.Boolean)
def teradata_cast_int_to_bool(compiled_arg, from_, to):
    return _teradata_cast_number_to_bool(compiled_arg, from_, to)


@teradata_cast.register(str, dt.Decimal, dt.String)
def teradata_cast_decimal_to_string(compiled_arg, from_, to):
    """Specialize cast from decimal to string.

    Teradata has two behaviours we need to avoid:
    1) SELECT CAST(CAST(7 AS DECIMAL(5,2)) AS VARCHAR(64));
    7.00
    2) SELECT CAST(CAST(-0.7 AS DECIMAL(5,2)) AS VARCHAR(64));
    -.70

    This function uses a format mask to give these results from above examples:
    1) 7
    2) -0.7
    """
    precision = from_.precision or 38
    if from_.scale and from_.scale > 0:
        fmt = "FM" + ("9" * (precision - from_.scale)) + "." + ("9" * from_.scale)
        return f"RTRIM(TO_CHAR({compiled_arg},'{fmt}'),'.')"
    elif from_.scale is not None and from_.scale == 0:
        fmt = "FM" + ("9" * (precision - from_.scale))
        return f"TO_CHAR({compiled_arg},'{fmt}')"
    return "TO_CHAR({},'TM9')".format(compiled_arg)


@teradata_cast.register(str, dt.Time, dt.String)
def teradata_cast_time_to_string(compiled_arg, from_, to):
    # Time always has a time zone associated with it in Teradata
    # No format here, so only providing HH:MM:SS, issue #1189
    return f"CAST({compiled_arg} at time zone 'gmt' as Varchar(8))"


@teradata_cast.register(str, dt.DataType, dt.DataType)
def teradata_cast_generate(compiled_arg, from_, to):
    sql_type = ibis_type_to_teradata_type(to)
    return "CAST({} AS {})".format(compiled_arg, sql_type)


def _cast(t, op):
    arg, target_type = op.args
    arg_formatted = t.translate(arg)
    input_dtype = arg.output_dtype

    # Specialize going from a binary float type to a string.
    # Trying to avoid scientific notation.
    if (input_dtype.is_float32() or input_dtype.is_float64()) and op.to.is_string():
        # Cannot return sa.func.to_char because t (TeradataExprTranslator) returns
        # a string containing the column and not a SQLAlchemy Column object.
        return f"TO_CHAR({arg_formatted},'TM9')"

    return teradata_cast(arg_formatted, input_dtype, target_type)


def _table_column(t, op):
    field_name = op.name
    quoted_name = _quote_identifier(field_name, force=True)

    table = op.table
    ctx = t.context

    # If the column does not originate from the table set in the current SELECT
    # context, we should format as a subquery
    # if translator.permit_subquery and ctx.is_foreign_expr(table):
    #     proj_expr = table.projection([field_name]).to_array()
    #     return _table_array_view(translator, proj_expr)

    alias = ctx.get_ref(table, search_parents=True)
    if alias is not None:
        quoted_name = f"{alias}.{quoted_name}"
    if op.output_dtype.is_timestamp():
        timezone = op.output_dtype.timezone
        if timezone is not None:
            timezone = "GMT" if timezone == "UTC" else timezone
            quoted_name = f"{quoted_name} AT TIME ZONE '{timezone}'"
    return quoted_name


# Teradata rules copied from ibis PostgreSQL compiler
_strftime_to_teradata_rules = {
    "%a": "Dy",
    "%A": "Day",
    "%w": "D",  # 1-based day of week, see below for how we make this 0-based
    "%d": "DD",  # day of month
    "%-d": "FMDD",  # - is no leading zero for Python same for FM in Teradata
    "%b": "Mon",  # Sep
    "%B": "Month",  # September
    "%m": "MM",  # 01
    "%-m": "FMMM",  # 1
    "%y": "YY",  # 15
    "%Y": "YYYY",  # 2015
    "%H": "HH24",  # 09
    "%-H": "FMHH24",  # 9
    "%I": "HH12",  # 09
    "%-I": "FMHH12",  # 9
    "%p": "AM",  # AM or PM
    "%M": "MI",  # zero padded minute
    "%-M": "FMMI",  # Minute
    "%S": "SS",  # zero padded second
    "%-S": "FMSS",  # Second
    "%f": "FF6",  # zero padded microsecond
    "%z": "TZR",  # utf offset
    "%Z": "TZR",  # uppercase timezone name
    "%j": "DDD",  # zero padded day of year
    "%-j": "FMDDD",  # day of year
    "%U": "WW",  # 1-based week of year
    # 'W': ?,  # meh
}

try:
    _strftime_to_teradata_rules.update(
        {
            "%c": locale.nl_langinfo(locale.D_T_FMT),  # locale date and time
            "%x": locale.nl_langinfo(locale.D_FMT),  # locale date
            "%X": locale.nl_langinfo(locale.T_FMT),  # locale time
        }
    )
except AttributeError:
    warnings.warn(
        "locale specific date formats (%%c, %%x, %%X) are not yet implemented "
        "for %s" % platform.system()
    )

# Translate strftime spec into mostly equivalent Teradata spec
_scanner = re.Scanner(
    # double quotes need to be escaped
    [('"', lambda scanner, token: r"\"")]
    + [
        (
            "|".join(
                map(
                    "(?:{})".format,
                    itertools.chain(
                        _strftime_to_teradata_rules.keys(),
                        [
                            # "%e" is in the C standard and Python actually
                            # generates this if your spec contains "%c" but we
                            # don't officially support it as a specifier so we
                            # need to special case it in the scanner
                            "%e",
                            r"\s+",
                            r"[{}]".format(re.escape(string.punctuation)),
                            r"[^{}\s]+".format(re.escape(string.punctuation)),
                        ],
                    ),
                )
            ),
            lambda scanner, token: token,
        )
    ]
)

_lexicon_values = frozenset(_strftime_to_teradata_rules.values())

_strftime_blacklist = frozenset(["%w", "%U", "%c", "%x", "%X", "%e"])


def _reduce_tokens(tokens):
    """
    Reduce strftime for,mat elements to an equivalent string of Teradata elements.
    Ideally this would be a full reduction like for Oracle and PostgreSQL but the "arg" parameter received
    by _strftime is not equivalent and ends up as a string rather than a column name.
    I've followed pattern set by other functions in this module and build a string function spec rather than
    using sa.func.to_char.
    """
    # TODO It would be nice to bring this into line with Oracle/PostgreSQL strftime function processing.

    # current list of tokens
    curtokens = []

    non_special_tokens = frozenset(_strftime_to_teradata_rules) - _strftime_blacklist

    # TODO: how much of a hack is this?
    for token in tokens:
        # we are a non-special token %A, %d, etc.
        if token in non_special_tokens:
            curtokens.append(_strftime_to_teradata_rules[token])

        # we have a string like DD, to escape this we
        # surround it with double quotes
        elif token in _lexicon_values:
            curtokens.append('"{}"'.format(token))

        # we have a token that needs special treatment
        elif token in _strftime_blacklist:
            raise UnsupportedOperationError("Not right now")

        # uninteresting text
        else:
            curtokens.append(token)
    return "".join(curtokens)


def _strftime(translator, op):
    arg, format_string = op.args
    fmt_string = translator.translate(format_string)
    arg_formatted = translator.translate(arg)
    tokens, _ = _scanner.scan(fmt_string)
    translated_format = _reduce_tokens(tokens)
    return "TO_CHAR({}, {})".format(arg_formatted, translated_format)


_date_units = {
    "Y": "YEAR",
    "Q": "QUARTER",
    "W": "WEEK",
    "M": "MONTH",
    "D": "DAY",
}


_timestamp_units = {
    "us": "MICROSECOND",
    "ms": "MILLISECOND",
    "s": "SECOND",
    "m": "MINUTE",
    "h": "HOUR",
}
_time_units = _timestamp_units.copy()
_timestamp_units.update(_date_units)


def _string_join(translator, op):
    sep, args = op.args
    return "||".join(map(translator.translate, op.arg))


def _extract_epoch(translator, op):
    arg = translator.translate(op.arg)
    # Since the change to table_column, column is already at UTC we don't need to set the time zone here
    extract_arg = f"CAST({arg} AS TIMESTAMP)" if op.arg.output_dtype.is_date() else arg
    return (
        f"(CAST ({arg} AS DATE) - DATE '1970-01-01') * 86400 + "
        f"(EXTRACT(HOUR FROM {extract_arg} ) * 3600) + "
        f"(EXTRACT(MINUTE FROM {extract_arg} ) * 60) + "
        f"(EXTRACT(SECOND FROM {extract_arg}))"
    )


def _rstrip(translator, op):
    arg = translator.translate(op.arg)
    # Rtrim parameter accounts for ' \t\x0b\n\r\x0c' as per #1272
    return f"RTRIM({arg}, _latin '20090B0A0D0C'XCV)"


""" Add New Customizations to Operations registry """
_operation_registry.update(
    {
        ops.TableColumn: _table_column,
        ops.Strftime: _strftime,
        ops.Cast: _cast,
        ops.IfNull: fixed_arity("NVL", 2),
        ops.StringJoin: _string_join,
        ops.ExtractEpochSeconds: _extract_epoch,
        ops.RStrip: _rstrip,
    }
)
