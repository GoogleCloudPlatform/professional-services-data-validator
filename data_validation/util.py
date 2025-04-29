# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import re
import time

from data_validation import exceptions

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import ibis.expr.types.Table


def timed_call(log_txt, fn, *args, **kwargs):
    t0 = time.time()
    result = fn(*args, **kwargs)
    elapsed = time.time() - t0
    logging.debug(f"{log_txt} elapsed: {round(elapsed, 2)}s")
    return result


def split_not_in_quotes(
    to_split: str, sep: str = " ", exclude_empty_tokens: bool = False
) -> list:
    """Split a string by a separator but only when the separator is not inside quotes.
    re pattern taken from this comment:
        https://stackoverflow.com/a/2787979/10979853
    The commenter's words should the link ever go stale:
        Each time it finds a semicolon, the lookahead scans the entire remaining string,
        making sure there's an even number of single-quotes and an even number of double-quotes.
        (Single-quotes inside double-quoted fields, or vice-versa, are ignored.) If the
        lookahead succeeds, the semicolon is a delimiter.
    The pattern doesn't cope with whitespace as sep, back to back spaces are multiple seps, therefore
    we have exclude_empty_tokens parameter.
    """
    pattern = r"""%(sep)s(?=(?:[^'"]|'[^']*'|"[^"]*")*$)""" % {"sep": sep}
    if exclude_empty_tokens:
        return [t for t in re.split(pattern, to_split) if t]
    else:
        return re.split(pattern, to_split)


def dvt_config_string_to_dict(config_string: str) -> dict:
    """Convert JSON in a string to a dict."""
    if not config_string:
        return None
    if isinstance(config_string, dict):
        return config_string
    try:
        param_dict = json.loads(config_string.replace("'", '"'))
        return param_dict
    except json.JSONDecodeError as exc:
        raise exceptions.ValidationException(
            f"Invalid JSON format in connection parameter dictionary string: {config_string}"
        ) from exc


def ibis_table_to_sql(ibis_table: "ibis.expr.types") -> str:
    """Function to generate the SQL string for the table based on the backend"""
    sql_alchemy_clients = [
        "mysql",
        "oracle",
        "postgres",
        "db2",
        "mssql",
        "redshift",
        "snowflake",
    ]
    # If the backend uses sqlalchemy, we will need to request sqla to bind variables
    # for a non sqlalchemy backend, the parameters are already bound
    backend_name = ibis_table._find_backend().name
    return (
        ibis_table.compile().compile(compile_kwargs={"literal_binds": True}).string
        if backend_name in sql_alchemy_clients
        else ibis_table.compile()
    )
