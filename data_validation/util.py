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

from datetime import datetime
from pandas import Timestamp as PandasTimestamp
from data_validation import exceptions


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


def datetime_to_iso_format(obj):
    """
    Converts a datetime object, pandas Timestamp, or integer timestamp to an ISO 8601 formatted string.

    Args:
        obj (datetime.datetime or pandas.Timestamp or int): The object to convert.

    Returns:
        str: An ISO 8601 formatted string representing the datetime.

    Raises:
        TypeError: If the object is not a datetime, pandas Timestamp, or integer timestamp.

    Examples:
        >>> from datetime import datetime
        >>> import pandas as pd
        >>> datetime_to_iso(datetime(2023, 10, 26, 10, 30, 0))
        '2023-10-26T10:30:00'

        >>> datetime_to_iso(pd.Timestamp('2023-10-26 10:30:00'))
        '2023-10-26T10:30:00'

        >>> datetime_to_iso(1677609600)  # Seconds since epoch
        '2023-03-01T00:00:00+00:00'

        >>> datetime_to_iso(1677609600000) # milliseconds since epoch
        '2023-03-01T00:00:00+00:00'

        >>> datetime_to_iso("invalid")
        Traceback (most recent call last):
            ...
        TypeError: Object of type str is not JSON serializable
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, PandasTimestamp):
        return obj.to_pydatetime().isoformat()
    elif isinstance(obj, int):
        if obj > 1000000000000:  # milliseconds
            return datetime.fromtimestamp(
                obj / 1000, tz=datetime.timezone.utc
            ).isoformat()
        else:  # seconds
            return datetime.fromtimestamp(obj, tz=datetime.timezone.utc).isoformat()
    else:
        raise TypeError(
            f"Object of type {obj.__class__.__name__} is not JSON serializable"
        )
