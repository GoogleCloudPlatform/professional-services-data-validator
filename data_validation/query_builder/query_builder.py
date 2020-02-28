import ibis
from data_validation import consts

class AggregateField(object):

    def __init__(self, ibis_expr, field_name=None, name=None):
        """
            field_name: A field to act on in the table.  Table level expr do not have a field name
        """
        self.expr = ibis_expr
        self.field_name = field_name
        self.name = name

        print("USING IBIS")

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
        else:
            group_field = group_field.cast("date")

        # The Casts require we also supply a name.  TODO this culd be a requirement in the init
        name = self.name or field_name
        group_field = group_field.name(name)
        
        return group_field

class QueryBuilder(object):

    def __init__(self, aggregate_fields, filters, grouped_fields, limit=None):
        """ Build a QueryBuilder object which can be used to build queries easily

            :param query_fields: List of AggregateField objects with Ibis expressions
            :param filters: A list of dicts. TODO: change this
                Each dict: {"type": "(DATE|INT|OTHER)", "column": "...", "value": 0, "comparison": ">="}

        """
        self.aggregate_fields = aggregate_fields
        self.filters = filters
        self.grouped_fields = grouped_fields
        self.limit = limit

    @staticmethod
    def build_count_validator(limit=None):
        aggregate_fields = [AggregateField.count("count")]
        filters = []
        grouped_fields = []

        return QueryBuilder(aggregate_fields, filters=filters, grouped_fields=grouped_fields, limit=limit)

    @staticmethod
    def build_partition_count_validator(limit=None):
        aggregate_fields = [AggregateField.count("count")]
        filters = []
        grouped_fields = [GroupedField(name=consts.DEFAULT_PARTITION_KEY)]

        return QueryBuilder(aggregate_fields, filters=filters, grouped_fields=grouped_fields, limit=limit)

    def compile_aggregate_fields(self, table):
        aggs = [field.compile(table) for field in self.aggregate_fields]

        return aggs

    def compile_group_fields(self, table, partition_column=None):
        groups = [field.compile(table, field_name=partition_column) for field in self.grouped_fields]
        return groups

    def compile(self, data_client, schema_name, table_name, partition_column=None, partition_column_type=None):
        table = data_client.table(table_name, database=schema_name)

        aggs = self.compile_aggregate_fields(table)
        filters = []
        groups = self.compile_group_fields(table, partition_column=partition_column)
        if groups:
            query = table.groupby(groups).aggregate(aggs)
        else:
            query = table.aggregate(aggs)

        if self.limit:
            query = query.limit(self.limit)

        return query

    # TODO: these should be submitted as QueryField objects
    def add_query_field(self, query_field):
        self.query_fields.append(query_field)

    def add_filter_field(self, filter_obj):
        """ Add a filter object to your query

            :param filter_obj: A dictionary object with filter field attrs  
                Each dict: {"type": "(DATE|INT|OTHER)", "column": "...", "value": 0, "comparison": ">="}
        """
        self.filters.append(filter_obj)

