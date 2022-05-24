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
from data_validation import clients, consts
from ibis.expr.types import StringScalar
from third_party.ibis.ibis_addon import api, operations


class AggregateField(object):
    def __init__(self, ibis_expr, field_name=None, alias=None, cast=None):
        """A representation of a table or column aggregate in Ibis

        Args:
            ibis_expr (ColumnExpr): A column aggregation to use from Ibis
            field_name (String: A field to act on in the table.
                Table level expr do not have a field name
            alias (String): A field to use as the aggregate alias name
        """
        self.expr = ibis_expr
        self.field_name = field_name
        self.alias = alias
        self.cast = cast

    @staticmethod
    def count(field_name=None, alias=None, cast=None):
        if field_name:
            return AggregateField(
                ibis.expr.types.ColumnExpr.count,
                field_name=field_name,
                alias=alias,
                cast=cast,
            )
        else:
            return AggregateField(
                ibis.expr.types.TableExpr.count,
                field_name=field_name,
                alias=alias,
                cast=cast,
            )

    @staticmethod
    def min(field_name=None, alias=None, cast=None):
        return AggregateField(
            ibis.expr.types.ColumnExpr.min,
            field_name=field_name,
            alias=alias,
            cast=cast,
        )

    @staticmethod
    def avg(field_name=None, alias=None, cast=None):
        return AggregateField(
            ibis.expr.types.NumericColumn.mean,
            field_name=field_name,
            alias=alias,
            cast=cast,
        )

    @staticmethod
    def max(field_name=None, alias=None, cast=None):
        return AggregateField(
            ibis.expr.types.ColumnExpr.max,
            field_name=field_name,
            alias=alias,
            cast=cast,
        )

    @staticmethod
    def sum(field_name=None, alias=None, cast=None):
        return AggregateField(
            ibis.expr.api.IntegerColumn.sum,
            field_name=field_name,
            alias=alias,
            cast=cast,
        )

    @staticmethod
    def bit_xor(field_name=None, alias=None, cast=None):
        return AggregateField(
            ibis.expr.api.IntegerColumn.bit_xor,
            field_name=field_name,
            alias=alias,
            cast=cast,
        )

    def compile(self, ibis_table):
        if self.field_name:
            agg_field = self.expr(ibis_table[self.field_name])
        else:
            agg_field = self.expr(ibis_table)

        if self.cast:
            agg_field = agg_field.cast(self.cast)

        if self.alias:
            agg_field = agg_field.name(self.alias)

        return agg_field


class FilterField(object):
    def __init__(
        self, ibis_expr, left=None, right=None, left_field=None, right_field=None
    ):
        """A representation of a query filter to be used while building a query.
            You can alternatively use either (left or left_field) and
            (right or right_field).

        Args:
            ibis_expr (ColumnExpr): A column expression to be used for comparisons (None represents a custom filter).
            left (Object): A value to compare on the left side of the expression
            left_field (String): A column name to be used to filter against
            right (Object): A value to compare on the right side of the expression
            right_field (String): A column name to be used to filter against

        """
        self.expr = ibis_expr
        self.left = left
        self.right = right
        self.left_field = left_field
        self.right_field = right_field

    @staticmethod
    def greater_than(field_name, value):
        # Build Left and Right Objects
        return FilterField(
            ibis.expr.types.ColumnExpr.__gt__, left_field=field_name, right=value
        )

    @staticmethod
    def less_than(field_name, value):
        # Build Left and Right Objects
        return FilterField(
            ibis.expr.types.ColumnExpr.__lt__, left_field=field_name, right=value
        )

    @staticmethod
    def equal_to(field_name, value):
        # Build Left and Right Objects
        return FilterField(
            ibis.expr.types.ColumnExpr.__eq__, left_field=field_name, right=value
        )

    @staticmethod
    def isin(field_name, values):
        # Build Left and Right Objects
        return FilterField(
            ibis.expr.types.ColumnExpr.isin, left_field=field_name, right=values
        )

    @staticmethod
    def custom(expr):
        """Returns a FilterField instance built for any custom SQL using a supported operator.

        Args:
            expr (Str): A custom SQL expression used to filter a query.
        """
        return FilterField(None, left=expr)

    def compile(self, ibis_table):
        if self.expr is None:
            return operations.compile_raw_sql(ibis_table, self.left)

        if self.left_field:
            self.left = ibis_table[self.left_field]
            # Cast All Datetime to Date (TODO this may be a bug in BQ)
            if isinstance(
                ibis_table[self.left_field].type(), ibis.expr.datatypes.Timestamp
            ):
                self.left = self.left.cast("date")
        if self.right_field:
            self.right = ibis_table[self.right_field]
            # Cast All Datetime to Date (TODO this may be a bug in BQ)
            if isinstance(
                ibis_table[self.right_field].type(), ibis.expr.datatypes.Timestamp
            ):
                self.right = self.right.cast("date")

        return self.expr(self.left, self.right)


class ComparisonField(object):
    def __init__(self, field_name, alias=None, cast=None):
        """A representation of a comparison field used to build a query.

        Args:
            field_name (String): A field to act on in the table
            alias (String): An alias to use for the group
            cast (String): A cast on the column if required
        """
        self.field_name = field_name
        self.alias = alias
        self.cast = cast

    def compile(self, ibis_table):
        # Fields are supplied on compile or on build
        comparison_field = ibis_table[self.field_name]
        alias = self.alias or self.field_name
        comparison_field = comparison_field.name(alias)

        return comparison_field


class GroupedField(object):
    def __init__(self, field_name, alias=None, cast=None):
        """A representation of a group by field used to build a query.

        Args:
            field_name (String): A field to act on in the table
            alias (String): An alias to use for the group
            cast (String): A cast on the column if required
        """
        self.field_name = field_name
        self.alias = alias
        self.cast = cast

    def compile(self, ibis_table):
        # Fields are supplied on compile or on build
        group_field = ibis_table[self.field_name]

        # TODO: generate cast for known types not specified
        if self.cast:
            group_field = group_field.cast(self.cast)
        elif isinstance(group_field.type(), ibis.expr.datatypes.Timestamp):
            group_field = group_field.cast("date")
        else:
            # TODO: need to build Truncation Int support
            # TODO: should be using a logger
            print("WARNING: Unknown cast types can cause memory errors")

        # The Casts require we also supply a name.
        alias = self.alias or self.field_name
        group_field = group_field.name(alias)

        return group_field


class ColumnReference(object):
    def __init__(self, column_name):
        """A representation of an calculated field to build a query.

        Args:
            column_name (String): The column name used in a complex expr
        """
        self.column_name = column_name

    def compile(self, ibis_table):
        """Return an ibis object referencing the column.

        Args:
            ibis_table (IbisTable): The table obj reference
        """
        return ibis_table[self.column_name]


class CalculatedField(object):
    def __init__(self, ibis_expr, config, fields, cast=None, **kwargs):

        """A representation of an calculated field to build a query.

        Args:
            config dict: Configurations object explaining calc field details
            fields list: List of fields to transform into a single column
        """
        self.expr = ibis_expr
        self.config = config
        self.fields = fields
        self.cast = cast
        self.kwargs = kwargs

    @staticmethod
    def concat(config, fields):
        if config.get("default_concat_separator") is None:
            config["default_concat_separator"] = ibis.literal("")
        fields = [config["default_concat_separator"], fields]
        cast = "string"
        return CalculatedField(
            ibis.expr.api.StringValue.join,
            config,
            fields,
            cast=cast,
        )

    @staticmethod
    def hash(config, fields):
        if config.get("default_hash_function") is None:
            how = "sha256"
            return CalculatedField(
                ibis.expr.api.StringValue.hashbytes,
                config,
                fields,
                how=how,
            )
        else:
            how = "farm_fingerprint"
            return CalculatedField(
                ibis.expr.api.ValueExpr.hash,
                config,
                fields,
                how=how,
            )

    @staticmethod
    def ifnull(config, fields):
        config["default_string"] = (
            ibis.literal("DEFAULT_REPLACEMENT_STRING")
            if config.get("default_null_string") is None
            else config.get("default_null_string")
        )
        fields = [fields[0], config["default_string"]]
        return CalculatedField(
            ibis.expr.api.ValueExpr.fillna,
            config,
            fields,
        )

    @staticmethod
    def length(config, fields):
        return CalculatedField(
            ibis.expr.api.StringValue.length,
            config,
            fields,
        )

    @staticmethod
    def rstrip(config, fields):
        return CalculatedField(
            ibis.expr.api.StringValue.rstrip,
            config,
            fields,
        )

    @staticmethod
    def upper(config, fields):
        return CalculatedField(
            ibis.expr.api.StringValue.upper,
            config,
            fields,
        )

    @staticmethod
    def epoch_seconds(config, fields):
        return CalculatedField(
            ibis.expr.api.TimestampValue.epoch_seconds,
            config,
            fields,
        )

    @staticmethod
    def cast(config, fields):
        target_type = config.get(consts.CONFIG_DEFAULT_CAST, "string")
        return CalculatedField(
            api.cast,
            config,
            fields,
            target_type=target_type,
        )

    @staticmethod
    def custom(expr):
        """Returns a CalculatedField instance built for any custom SQL using a supported operator.
        Args:
            expr (Str): A custom SQL expression used to filter a query
        """
        return CalculatedField(expr)

    def _compile_fields(self, ibis_table, fields):
        compiled_fields = []

        for field in fields:
            if type(field) in [StringScalar]:
                compiled_fields.append(field)
            elif isinstance(field, list):
                compiled_fields.append(self._compile_fields(ibis_table, field))
            else:
                if self.cast:
                    compiled_fields.append(ibis_table[field].cast(self.cast))
                else:
                    compiled_fields.append(ibis_table[field])
        return compiled_fields

    def compile(self, ibis_table):
        compiled_fields = self._compile_fields(ibis_table, self.fields)
        calc_field = self.expr(*compiled_fields, **self.kwargs)
        if self.config["field_alias"]:
            calc_field = calc_field.name(self.config["field_alias"])

        return calc_field


class QueryBuilder(object):
    def __init__(
        self,
        aggregate_fields,
        calculated_fields,
        filters,
        grouped_fields,
        comparison_fields,
        limit=None,
    ):
        """Build a QueryBuilder object which can be used to build queries easily

        Args:
            aggregate_fields (list[AggregateField]): AggregateField instances with Ibis expressions
            calculated_fields (list[CalculatedField]): A list of CalculatedField instances
            filters (list[FilterField]): A list of FilterField instances
            grouped_fields (list[GroupedField]): A list of GroupedField instances
            limit (int): A limit value for the number of records to pull (used for testing)
        """
        self.aggregate_fields = aggregate_fields
        self.calculated_fields = calculated_fields
        self.filters = filters
        self.grouped_fields = grouped_fields
        self.comparison_fields = comparison_fields
        self.limit = limit

    @staticmethod
    def build_count_validator(limit=None):
        """Return a basic template builder for most validations"""
        aggregate_fields = []
        filters = []
        grouped_fields = []
        comparison_fields = []
        calculated_fields = []

        return QueryBuilder(
            aggregate_fields,
            filters=filters,
            grouped_fields=grouped_fields,
            comparison_fields=comparison_fields,
            calculated_fields=calculated_fields,
        )

    def compile_aggregate_fields(self, table):
        aggs = [field.compile(table) for field in self.aggregate_fields]

        return aggs

    def compile_filter_fields(self, table):
        return [field.compile(table) for field in self.filters]

    def compile_group_fields(self, table):
        return [field.compile(table) for field in self.grouped_fields]

    def compile_comparison_fields(self, table):
        return [field.compile(table) for field in self.comparison_fields]

    def compile_calculated_fields(self, table, n=0):
        return [
            field.compile(table)
            for field in self.calculated_fields
            if field.config[consts.CONFIG_DEPTH] == n
        ]
        # if n is not None:
        #     return [
        #         field.compile(table)
        #         for field in self.calculated_fields
        #         if field.config[consts.CONFIG_DEPTH] == n
        #     ]
        # else:
        #     return [field.compile(table) for field in self.calculated_fields]

    def compile(self, data_client, schema_name, table_name):
        """Return an Ibis query object

        Args:
            data_client (IbisClient): The client used to validate the query.
            schema_name (String): The name of the schema for the given table.
            table_name (String): The name of the table to query.
        """
        table = clients.get_ibis_table(data_client, schema_name, table_name)

        # Build Query Expressions
        calc_table = table
        if self.calculated_fields:
            depth_limit = max(
                field.config.get(consts.CONFIG_DEPTH, 0)
                for field in self.calculated_fields
            )
            for n in range(0, (depth_limit + 1)):
                calc_table = calc_table.mutate(
                    self.compile_calculated_fields(calc_table, n)
                )
        compiled_filters = self.compile_filter_fields(table)
        filtered_table = (
            calc_table.filter(compiled_filters) if compiled_filters else calc_table
        )
        compiled_groups = self.compile_group_fields(filtered_table)
        grouped_table = (
            filtered_table.groupby(compiled_groups)
            if compiled_groups
            else filtered_table
        )
        if self.aggregate_fields:
            query = grouped_table.aggregate(
                self.compile_aggregate_fields(filtered_table)
            )
        else:
            query = grouped_table

        if self.limit:
            query = query.limit(self.limit)

        return query

    def add_aggregate_field(self, aggregate_field):
        """Add an AggregateField instance to the query which
            will be used when compiling your query (ie. SUM(a))

        Args:
            aggregate_field (AggregateField): An AggregateField instance
        """
        self.aggregate_fields.append(aggregate_field)

    def add_comparison_field(self, comparison_field):
        """Add an ComparisonField instance to the query which
            will be used when compiling your query (ie. SUM(a))

        Args:
            comparison_field (ComparisonField): An ComparisonField instance
        """
        self.comparison_fields.append(comparison_field)

    def add_grouped_field(self, grouped_field):
        """Add a GroupedField instance to the query which
            represents adding a column to group by in the
            query being built.
        Args:
            grouped_field (GroupedField): A GroupedField instance
        """
        self.grouped_fields.append(grouped_field)

    def add_filter_field(self, filter_obj):
        """Add a FilterField instance to your query which
            will add the desired filter to your compiled
            query (ie. WHERE query_filter=True)
        Args:
            filter_obj (FilterField): A FilterField instance
        """
        self.filters.append(filter_obj)

    def add_calculated_field(self, calculated_field):
        """Add a CalculatedField instance to your query which
            will add the desired scalar function to your compiled
            query (ie. CONCAT(field_a, field_b))
        Args:
            calculated_field (CalculatedField): A CalculatedField instance
        """
        self.calculated_fields.append(calculated_field)
