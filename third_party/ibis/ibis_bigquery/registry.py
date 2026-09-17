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
    arg_type = arg.dtype

    if arg_type.is_date():
        strftime_format_func_name = "DATE"
    elif arg_type.is_time():
        strftime_format_func_name = "TIME"
    elif arg_type.is_timestamp():
        if getattr(arg_type, "timezone", None) is None:
            strftime_format_func_name = "DATETIME"
        else:
            strftime_format_func_name = "TIMESTAMP"
    else:
        raise ValueError(f"Unsupported type for strftime: {arg_type}")
    fmt_string = translator.translate(format_str)
    # Deal with issue 1181 due a GoogleSQL bug with dates before 1000 CE affects both date and timestamp types
    if format_str.value.startswith("%Y"):
        fmt_string = fmt_string.replace("%Y", "%E4Y", 1)
    arg_formatted = translator.translate(arg)
    if arg_type.is_timestamp():
        if getattr(arg_type, "timezone", None) is None:
            return "FORMAT_DATETIME({}, DATETIME({}))".format(fmt_string, arg_formatted)
        else:
            return "FORMAT_TIMESTAMP({}, TIMESTAMP({}), {!r})".format(
                fmt_string,
                arg_formatted,
                arg_type.timezone if arg_type.timezone is not None else "UTC",
            )
    return "FORMAT_{}({}, {})".format(
        strftime_format_func_name, fmt_string, arg_formatted
    )


def format_binary_length(translator, op):
    arg = translator.translate(op.arg)
    return f"LENGTH({arg})"


def extract_epoch_seconds(translator, op):
    arg = op.arg
    arg_formatted = translator.translate(arg)
    if arg.dtype.is_date() or (arg.dtype.is_timestamp() and arg.dtype.timezone is None):
        return f"UNIX_SECONDS(CAST({arg_formatted} AS TIMESTAMP))"
    return f"UNIX_SECONDS({arg_formatted})"


def format_literal(translator, op):
    """Format literals for BigQuery SQL.

    Fixes Decimal literal rendering in BigQuery by:
    1. Using standard fixed-point string representation (format(v, 'f')) instead of
       scientific notation strings (e.g. 3.23E+29) which BigQuery cannot parse.
    2. Choosing BIGNUMERIC when precision > 38, scale > 9, or integer digits > 29,
       which exceed standard BigQuery NUMERIC bounds.
    """
    import ibis.backends.bigquery.registry as orig_bqr

    dtype = op.dtype
    value = op.value

    if dtype.is_decimal() and value is not None:
        if value.is_nan():
            return "CAST('NaN' AS FLOAT64)"
        elif value.is_infinite():
            prefix = "-" * value.is_signed()
            return f"CAST('{prefix}inf' AS FLOAT64)"
        else:
            val_str = format(value, "f")
            parts = val_str.lstrip("-").split(".")
            int_len = len(parts[0])
            frac_len = len(parts[1]) if len(parts) > 1 else 0

            if (
                int_len > 29
                or frac_len > 9
                or (getattr(dtype, "precision", None) and dtype.precision > 38)
                or (getattr(dtype, "scale", None) and dtype.scale > 9)
            ):
                bq_type = "BIGNUMERIC"
            else:
                bq_type = "NUMERIC"
            return f"{bq_type} '{val_str}'"

    return orig_bqr._literal(translator, op)
