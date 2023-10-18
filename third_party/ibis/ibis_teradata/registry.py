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


@teradata_cast.register(str, dt.DataType, dt.DataType)
def teradata_cast_generate(compiled_arg, from_, to):
    sql_type = ibis_type_to_teradata_type(to)
    return "CAST({} AS {})".format(compiled_arg, sql_type)


def _cast(t, op):
    arg, target_type = op.args
    arg_formatted = t.translate(arg)
    input_dtype = arg.output_dtype
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
    utc_expression = (
        "AT TIME ZONE 'GMT'" if getattr(op.arg.output_dtype, "timezone", None) else ""
    )
    extract_arg = f"CAST({arg} AS TIMESTAMP)" if op.arg.output_dtype.is_date() else arg

    return (
        f"(CAST ({arg} AS DATE {utc_expression}) - DATE '1970-01-01') * 86400 + "
        f"(EXTRACT(HOUR FROM {extract_arg} {utc_expression}) * 3600) + "
        f"(EXTRACT(MINUTE FROM {extract_arg} {utc_expression}) * 60) + "
        f"(EXTRACT(SECOND FROM {extract_arg} {utc_expression}))"
    )


""" Add New Customizations to Operations registry """
_operation_registry.update(
    {
        ops.TableColumn: _table_column,
        ops.Strftime: _strftime,
        ops.Cast: _cast,
        ops.IfNull: fixed_arity("NVL", 2),
        ops.StringJoin: _string_join,
        ops.ExtractEpochSeconds: _extract_epoch,
    }
)
