# Copyright 2020 Google LLC
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

import ibis
import ibis.expr.operations as ops
import ibis.expr.rules as rlz
import ibis.expr.types as ir
from ibis.backends.pandas.client import PandasClient

from typing import List
from ibis import util
from data_validation import clients


""" The QueryBuilder for retreiving random row values to filter against."""


RANDOM_SORT_SUPPORTS = [PandasClient]

class RandomSortKey(ops.SortKey):

    def equals(self, other, cache=None):
        return isinstance(other, RandomSortKey)


class RandomRowBuilder(object):
    def __init__(self, primary_keys: List[str], batch_size: int):
        """ Build a RandomRowBuilder objct which is ready to build a random row filter query.

    Args:
        primary_keys: A list of primary key field strings used to find random rows.
        batch_size: A max size for the number of random row values to find.
    """
        self.primary_keys = primary_keys
        self.batch_size = batch_size

    def compile(
        self, data_client: ibis.client, schema_name: str, table_name: str
    ) -> ibis.Expr:
        """Return an Ibis query object

    Args:
        data_client (IbisClient): The client used to query random rows.
        schema_name (String): The name of the schema for the given table.
        table_name (String): The name of the table to query.
    """
        table = clients.get_ibis_table(data_client, schema_name, table_name)
        randomly_sorted_table = self.maybe_add_random_sort(data_client, table)
        query = randomly_sorted_table[self.primary_keys].limit(self.batch_size)

        return query

    def maybe_add_random_sort(self, data_client: ibis.client, table: ibis.Expr) -> ibis.Expr:
        """ Return a randomly sorted query if it is supported for the client."""
        if type(data_client) in RANDOM_SORT_SUPPORTS:
          return table.sort_by(RandomSortKey(table[table.columns[0]], ascending=False).to_expr())

        return table


##################################
### Custom Pandas Client Logic ###
##################################
import operator
import toolz
import random
import ibis.backends.pandas.execution.util as pandas_util

# Save old version to be wrapped
_compute_sorted_frame = pandas_util.compute_sorted_frame

class RandomColumn:
    def __init__(self, name):
        self._name = name

    def op(self):
        return self

    def to_expr(self):
        return self._name

def build_and_assign_random_column(df, key, random_name):
    if (not isinstance(key.op(), RandomSortKey)):
        return key

    random.shuffle(df.index.values)
    df[random_name] = df.index.values
    return RandomColumn(random_name)

def compute_sorted_frame(
    df, order_by, group_by=(), timecontext=None, **kwargs):
    new_order_by = [build_and_assign_random_column(df, key, f"__random_sort_{i}__") for i, key in enumerate(order_by)]
    sorted_results = _compute_sorted_frame(df, new_order_by, group_by=group_by, timecontext=timecontext, **kwargs)

    return sorted_results

pandas_util.compute_sorted_frame = compute_sorted_frame
