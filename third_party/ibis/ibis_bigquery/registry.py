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
            return "FORMAT_DATETIME({}, DATETIME({}))".format(
                fmt_string, arg_formatted
            )
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
