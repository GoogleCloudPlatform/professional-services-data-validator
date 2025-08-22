# Copyright 2025 Google Inc.
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

from ibis.backends.bigquery.registry import (
    STRFTIME_FORMAT_FUNCTIONS as BQ_STRFTIME_FORMAT_FUNCTIONS,
    bigquery_cast,
)
import ibis.expr.datatypes as dt


def format_hashbytes(translator, op):
    arg = translator.translate(op.arg)
    if op.how == "sha256":
        return f"TO_HEX(SHA256({arg}))"
    elif op.how == "farm_fingerprint":
        return f"FARM_FINGERPRINT({arg})"
    else:
        raise ValueError(f"unexpected value for 'how': {op.how}")


def strftime(translator, op):
    """Timestamp formatting."""
    arg = op.arg
    format_str = op.format_str
    arg_type = arg.output_dtype
    strftime_format_func_name = BQ_STRFTIME_FORMAT_FUNCTIONS[arg_type]
    fmt_string = translator.translate(format_str)
    # Deal with issue 1181 due a GoogleSQL bug with dates before 1000 CE affects both date and timestamp types
    if format_str.value.startswith("%Y"):
        fmt_string = fmt_string.replace("%Y", "%E4Y", 1)
    arg_formatted = translator.translate(arg)
    if isinstance(arg_type, dt.Timestamp):
        return "FORMAT_{}({}, {}({}){})".format(
            strftime_format_func_name,
            fmt_string,
            strftime_format_func_name,
            arg_formatted,
            f",'{arg_type.timezone}'" if arg_type.timezone else "",
        )
    return "FORMAT_{}({}, {})".format(
        strftime_format_func_name, fmt_string, arg_formatted
    )


def epoch_seconds(translator, op):
    arg = translator.translate(op.arg)
    return f"UNIX_SECONDS(CAST({arg} AS TIMESTAMP))"


@bigquery_cast.register(str, dt.Binary, dt.String)
def bigquery_cast_from_binary_generate(compiled_arg, from_, to):
    """Cast of binary to string should be hex conversion."""
    return f"TO_HEX({compiled_arg})"


@bigquery_cast.register(str, dt.String, dt.Binary)
def bigquery_cast_to_binary_generate(compiled_arg, from_, to):
    """Cast of binary to string should be hex conversion."""
    return f"FROM_HEX({compiled_arg})"


# @bigquery_cast.register(str, (dt.Date, dt.Timestamp), dt.Timestamp)
# def bigquery_cast_to_timestamp(compiled_arg, from_, to):
#    """Cast of Ibis Date to Ibis Timestamp uses BigQuery TIMESTAMP.
#
#    This is replicating what happened in Ibis 5. In newer versions this would result
#    in DATETIME but we only do this for epoch seconds, which requires TIMESTAMP."""
#    return f"CAST({compiled_arg} AS TIMESTAMP)"
