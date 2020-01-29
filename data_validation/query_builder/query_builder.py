from data_validation import consts

class QueryBuilder(object):

    SQL_TEMPLATE = """
{select} {query_fields}
{from}
    {table_object}
{filters}
{groups}
    """

    def __init__(self, query_fields, filters, grouped_fields):
        """ Build a QueryBuilder object which can be used to build queries easily

            :param query_fields: Fields to query in body
            :param filters: A list of dicts.  
                Each dict: {"type": "(DATE|INT|OTHER)", "column": "...", "value": 0, "comparison": ">="}

        """
        self.query_fields = query_fields
        self.filters = filters
        self.grouped_fields = grouped_fields

    @staticmethod
    def build_count_validator():
        query_fields = ["{count_star}"]
        filters = []
        grouped_fields = []

        return QueryBuilder(query_fields, filters=filters, grouped_fields=grouped_fields)

    @staticmethod
    def build_partition_count_validator():
        query_fields = ["{partition_column_sql}", "{count_star}"]
        filters = []
        grouped_fields = ["{grouped_column_sql}"]

        return QueryBuilder(query_fields, filters=filters, grouped_fields=grouped_fields)

    def render_query(self, data_client, schema_name, table_name, partition_column=None, partition_column_type=None):
        sql_template = self._render_query_template(data_client)

        sql_formatting = {
            "q": data_client.DEFAULT_QUOTE,
            "count_star": data_client.get_count_star(name="rows"),
            "schema_name": schema_name,
            "table_name": table_name,
        }
        if partition_column and partition_column_type:
            sql_formatting[consts.PARTITION_COLUMN_SQL] = data_client.get_partition_column_sql(partition_column, partition_column_type)
            sql_formatting[consts.GROUP_COLUMN_SQL] = data_client.get_group_column_sql(partition_column, partition_column_type)

        sql_query = sql_template.format(**sql_formatting)
        return sql_query

    # TODO: replace with consts
    def _render_query_template(self, data_client):
        template_formatting = {
            "select": data_client.SELECT,
            "from": data_client.FROM,
            "query_fields": self._render_query_fields(data_client),
            "table_object": data_client.get_table_object_template(),
            "filters": self._render_filters(data_client),
            "groups": self._render_groups(data_client)
        }
        return self.SQL_TEMPLATE.format(**template_formatting)

    def _render_query_fields(self, data_client):
        return ",".join(self.query_fields)

    def _render_filters(self, data_client):
        if not self.filters:
            return ""

        filter_templates = []
        for filter_obj in self.filters:
            data_client.get_filter_template(filter_obj)

        return data_client.get_where() + " " + data_client.get_filter_joiner.join(filter_templates)

    def _render_groups(self, data_client):
        if not self.grouped_fields:
            return ""

        return data_client.get_group() + " " + ",".join(self.grouped_fields)

    def add_query_field(self, query_field):
        self.query_fields.append(query_field)

    def add_filter_field(self, filter_obj):
        """ Add a filter object to your query

            :param filter_obj: A dictionary object with filter field attrs  
                Each dict: {"type": "(DATE|INT|OTHER)", "column": "...", "value": 0, "comparison": ">="}
        """
        self.filters.append(filter_obj)

