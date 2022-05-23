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

import random
import logging
import ibis
import ibis.expr.operations as ops
import ibis.expr.types as tz
import ibis.expr.rules as rlz
import ibis.backends.base_sqlalchemy.compiler as sql_compiler
from ibis_bigquery import BigQueryClient
from ibis.backends.impala.client import ImpalaClient
from ibis.backends.pandas.client import PandasClient
import ibis.backends.pandas.execution.util as pandas_util

from ibis.expr.signature import Argument as Arg
from typing import List
from data_validation import clients
from io import StringIO


""" The QueryBuilder for retreiving random row values to filter against."""


######################################
### Adding new datasources should be
### done by adding the Client and
### syntax for random sorts in the
### dict below. If upstream injection
### is required, feel free to reach
### out to dhercher
######################################
RANDOM_SORT_SUPPORTS = {
    PandasClient: "NA",
    BigQueryClient: "RAND()",
    ImpalaClient: "RAND()",
}


class RandomSortExpr(tz.AnyValue, tz.SortExpr):
    _dtype = rlz.string
    _name = "RandomSortExpr"

    def __init__(self, arg):
        self._arg = arg

    def type(self):
        return self._dtype


class RandomSortKey(ops.SortKey):
    expr = Arg(rlz.any)
    value = None

    def equals(self, other, cache=None):
        return isinstance(other, RandomSortKey)

    def output_type(self):
        return RandomSortExpr

    def resolve_name(self):
        return "RandomSortKey"


class RandomRowBuilder(object):
    def __init__(self, primary_keys: List[str], batch_size: int):
        """Build a RandomRowBuilder object which is ready to build a random row filter query.

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
        query = randomly_sorted_table.limit(self.batch_size)[self.primary_keys]

        return query

    def maybe_add_random_sort(
        self, data_client: ibis.client, table: ibis.Expr
    ) -> ibis.Expr:
        """Return a randomly sorted query if it is supported for the client."""
        if type(data_client) in RANDOM_SORT_SUPPORTS:
            return table.sort_by(
                RandomSortKey(RANDOM_SORT_SUPPORTS[type(data_client)]).to_expr()
            )

        logging.warning(
            "Data Client %s Does Not Enforce Random Sort on Sample",
            str(type(data_client)),
        )
        return table


##################################
### Custom Pandas Client Logic ###
##################################

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
    if not isinstance(key.op(), RandomSortKey):
        return key

    random.shuffle(df.index.values)
    df[random_name] = df.index.values
    return RandomColumn(random_name)


def compute_sorted_frame(df, order_by, group_by=(), timecontext=None, **kwargs):
    new_order_by = [
        build_and_assign_random_column(df, key, f"__random_sort_{i}__")
        for i, key in enumerate(order_by)
    ]
    sorted_results = _compute_sorted_frame(
        df, new_order_by, group_by=group_by, timecontext=timecontext, **kwargs
    )

    return sorted_results


pandas_util.compute_sorted_frame = compute_sorted_frame


#####################################
##### Override Order By for SQL #####
#####################################


def format_order_by(self):
    if not self.order_by:
        return None

    buf = StringIO()
    buf.write("ORDER BY ")

    formatted = []
    for expr in self.order_by:
        ##### ADDING CODE TO FORMAT #####
        if isinstance(expr, RandomSortExpr):
            literal_sql = expr.op().expr.op()
            formatted.append(literal_sql.value)
            continue
        ##### END ADDING CODE TO FORMAT #####

        key = expr.op()
        translated = self._translate(key.expr)
        if not key.ascending:
            translated += " DESC"
        formatted.append(translated)

    buf.write(", ".join(formatted))
    return buf.getvalue()


sql_compiler.Select.format_order_by = format_order_by
