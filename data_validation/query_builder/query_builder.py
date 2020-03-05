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
from data_validation import consts

from datetime import datetime, timedelta

class AggregateField(object):

    def __init__(self, ibis_expr, field_name=None, name=None):
        """
            field_name: A field to act on in the table.  Table level expr do not have a field name
        """
        self.expr = ibis_expr
        self.field_name = field_name
        self.name = name

    @staticmethod
    def count(name=None):
        return AggregateField(ibis.expr.types.ColumnExpr.count, field_name=None, name=name)

    def compile(self, ibis_table):
        if self.field_name:
            agg_field = self.expr(ibis_table[self.field_name])
        else:
            agg_field = self.expr(ibis_table)
        
        if self.name:
            agg_field = agg_field.name(self.name)
        
        return agg_field

class FilterField(object):

    def __init__(self, ibis_expr, left, right):
        """
            field_name: A field to act on in the table.  Table level expr do not have a field name
        """
        self.expr = ibis_expr
        self.left = left
        self.right = right

    @staticmethod
    def greater_than(table, field_name, value):
        # Build Left and Right Objects
        left = table[field_name]
        right = value

        # Cast All Datetime to Date (TODO this may be a bug in BQ)
        if isinstance(table[field_name].type(), ibis.expr.datatypes.Timestamp):
            left = left.cast("date")

        return FilterField(ibis.expr.types.ColumnExpr.__gt__, left, right)

    def compile(self):
        return self.expr(self.left, self.right)

class GroupedField(object):

    def __init__(self, field_name=None, name=None, cast=None):
        """
            field_name: A field to act on in the table.  Table level expr do not have a field name
        """
        self.field_name = field_name
        self.name = name
        self.cast = cast

    def compile(self, ibis_table, field_name=None):
        # Fields are supplied on compile or on build
        field_name = field_name or self.field_name
        group_field = ibis_table[field_name]
        
        # TODO: generate cast for known types not specified
        if self.cast:
            group_field = group_field.cast(self.cast)
        elif isinstance(group_field.type(), ibis.expr.datatypes.Timestamp):
            group_field = group_field.cast("date")
        else:
            # TODO: need to build Truncation Int support
            print("WARNING: Non timestamp fields not supported")
            group_field = group_field.cast("date")

        # The Casts require we also supply a name.  TODO this culd be a requirement in the init
        name = self.name or field_name
        group_field = group_field.name(name)
        
        return group_field

class QueryBuilder(object):

    def __init__(self, aggregate_fields, filters, grouped_fields, days_past=30, limit=None):
        """ Build a QueryBuilder object which can be used to build queries easily

            :param aggregate_fields: List of AggregateField objects with Ibis expressions
            :param filters: A list of FilterField objects
            :param grouped_fields: A list of GroupedField objects
            :param days_past: Number of days past to use as date partition filter
            :param limit: A limit value for the number of records to pull (used for testing)
        """
        self.aggregate_fields = aggregate_fields
        self.filters = filters
        self.grouped_fields = grouped_fields
        self.days_past = days_past
        self.limit = limit

    # A Static function with prebuilt comparisons to use in query building
    @staticmethod
    def build_count_validator(limit=None):
        aggregate_fields = [AggregateField.count("count")]
        filters = []
        grouped_fields = []

        return QueryBuilder(aggregate_fields, filters=filters, grouped_fields=grouped_fields, limit=limit)

    # A Static function with prebuilt comparisons to use in query building
    @staticmethod
    def build_partition_count_validator(days_past=30, limit=None):
        aggregate_fields = [AggregateField.count("count")]
        filters = []
        grouped_fields = [GroupedField(name=consts.DEFAULT_PARTITION_KEY)]

        return QueryBuilder(aggregate_fields, filters=filters, grouped_fields=grouped_fields,
                            days_past=days_past, limit=limit)

    def compile_aggregate_fields(self, table):
        aggs = [field.compile(table) for field in self.aggregate_fields]

        return aggs

    def compile_filter_fields(self):
        return [field.compile() for field in self.filters]

    def compile_group_fields(self, table, partition_column=None):
        return [field.compile(table, field_name=partition_column) for field in self.grouped_fields]

    def compile(self, data_client, schema_name, table_name, partition_column=None):
        table = data_client.table(table_name, database=schema_name)

        # Build Query Expressions
        aggs = self.compile_aggregate_fields(table)
        filters = self.compile_filter_fields()
        groups = self.compile_group_fields(table, partition_column=partition_column)

        # Check if a Dast Past Filter should be added
        # TODO this is ugly but difficult to know anywhere else
        if partition_column and self.days_past and groups:
            days_past_ts = datetime.utcnow() - timedelta(days=self.days_past)
            date_fiter = FilterField.greater_than(table, partition_column, days_past_ts)
            filters.append(date_fiter.compile())

        query = table.filter(filters)
        query = query.groupby(groups)
        query = query.aggregate(aggs)

        # if groups:
        #     query = table.groupby(groups).aggregate(aggs)
        # else:
        #     query = table.aggregate(aggs)

        if self.limit:
            query = query.limit(self.limit)

        return query

    # TODO: these should be submitted as QueryField objects
    def add_query_field(self, query_field):
        self.query_fields.append(query_field)

    def add_filter_field(self, filter_obj):
        """ Add a filter object to your query

            :param filter_obj: A FilterField Object
        """
        self.filters.append(filter_obj)

