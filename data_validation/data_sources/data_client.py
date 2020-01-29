import pandas
from abc import ABC, abstractmethod

from data_validation import consts


class DataClient(ABC):

    SOURCE_TYPE = "Generic" # Recommend to overwrite this var

    COUNT_SQL = """
    SELECT COUNT(1) {q}rows{q} {aggregate_cols} FROM {q}{schema}{q}.{q}{table}{q} {where};
    """.strip()
    
    DEFAULT_QUOTE = '"'
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    GROUP_BY = "GROUP BY"
    FILTER_JOINER = " AND "
    COUNT_STAR = "COUNT(*) {q}{name}{q}"
    TABLE_OBJECT = "{q}{schema_name}{q}.{q}{table_name}{q}"
    DATE_COLUMN_CAST = "DATE({q}{partition_column}{q})"
    PARTITION_COLUMN_NAME = " {q}{partition_key}{q}"
    INT_COLUMN_CAST = "INTEGER({partition_key}/{partition_size})*{partition_size}"

    def __init__(self, **config):
        self.config = config
        self.conn = self.get_connection()

    def get_table_object_template(self):
        return self.TABLE_OBJECT

    def get_where(self):
        return self.WHERE

    def get_group(self):
        return self.GROUP_BY

    def get_filter_joiner(self):
        return self.FILTER_JOINER

    def get_filter_template(self, filter_obj):
        """ Return String of built filter template

            :param filter_obj: A dictionary representing a query filter  
                Each dict: {"type": "(DATE|INT|OTHER)", "column": "...", "value": 0, "comparison": ">="}
        """
        return ""

    def get_partition_column_sql(self, partition_column, partition_column_type, partition_size=None):
        partition_column_name = self.PARTITION_COLUMN_NAME.format(q=self.DEFAULT_QUOTE,
                                                                  partition_key=consts.DEFAULT_PARTITION_KEY)
        if partition_column_type == "DATE":
            date_column_cast = self.get_date_column_cast(partition_column)
            return date_column_cast + partition_column_name
        elif partition_column_type == "INT":
            int_column_cast = self.get_int_column_cast(partition_column, partition_size)
            return int_column_cast + partition_column_name
        else:
            raise Exception("Unsupported Partition Column Type: {}".format(partition_column_type))

    def get_group_column_sql(self, partition_column, partition_column_type, partition_size=None):
        if partition_column_type == "DATE":
            return self.get_date_column_cast(partition_column)
        elif partition_column_type == "INT":
            return self.get_int_column_cast(partition_column, partition_size)
        else:
            raise Exception("Unsupported Partition Column Type: {}".format(partition_column_type))

    def get_date_column_cast(self, partition_column):
        return self.DATE_COLUMN_CAST.format(q=self.DEFAULT_QUOTE, partition_column=partition_column)

    def get_int_column_cast(self, partition_column, partition_size):
        return self.INT_COLUMN_CAST.format(q=self.DEFAULT_QUOTE, partition_column=partition_column,
                                           partition_size=partition_size)

    def get_count_star(self, name="rows"):
        return self.COUNT_STAR.format(q=self.DEFAULT_QUOTE, name=name)

    def __del__(self):
        if self.is_connected(self.conn) and hasattr(self.conn, "close"):
            self.conn.close()

    def _query(self, sql):
        """ Return record results from query """
        if not sql:
            raise Exception("Invalid Query: sql is None")

        return pandas.read_sql(sql, self.conn)

    @abstractmethod
    def is_connected(self, conn):
        """ Checks if a connection is still active """
        raise Exception("Not Implemented")

    # @abstractmethod
    # @classmethod
    # def get_connection_string(cls, config, template=None):
    #     """ Return String Used to Connect to client """
    #     raise Exception("Not Implemented")

    @abstractmethod
    def get_connection(self):
        """ Get a new connection to the client """
        raise Exception("Not Implemented")

    def validate_connection(self, allow_reload=False):
        """ Execute query """
        if self.is_connected(self.conn):
            pass

        elif allow_reload:
            self.reload()

        else:
            raise exceptions.ClientDisconnected()

    def reload(self):
        """ Re-connect to client """
        self.conn = self.get_connection(self.config)

    def read_sql(self, sql, allow_reload=True):
        """ Return record results from query """
        self.validate_connection(allow_reload=allow_reload)

        result = self._query(sql)

        if isinstance(result, pandas.DataFrame):
            return result.to_dict(orient='record')

        return result

    def df_read_sql(self, sql, allow_reload=True):
        """ Return pandas.DataFrame results from query """
        self.validate_connection(allow_reload=allow_reload)
        result = self._query(sql)

        if not isinstance(result, pandas.DataFrame):
            return pandas.DataFrame(result)

        return result

    # def _execute(self, sql):
    #     """ Execute query """
    #     if not sql:
    #         raise exceptions.InvalidQuery.default(query=sql)

    #     cursor = self.conn.cursor()

    #     try:
    #         result = cursor.execute(sql)
    #         self.conn.commit()

    #     except Exception:
    #         self.conn.rollback()
    #         raise

    #     finally:
    #         cursor.close()

    #     return result

    # def get_schemas(self):
    #     """ Get a list of all schemas """
    #     raise Exception("Not Implemented")

    # def get_tables(self, schema=None):
    #     """ Get a list of all tables """
    #     raise Exception("Not Implemented")

    # def get_table_schema(self, schema, table):
    #     """ Return Schema For Given Table """
    #     raise Exception("Not Implemented")

    # def alter_column_type(self, schema, table, column, new_column_type, verbose=False, dry_run=False):
    #     raise Exception("Not Implemented")

    # def count(self, schema, table, aggregate_cols="", where="", **kwargs):
    #     """ Return Count and Other Metrics for Given Filter
    
    #         :param schema: Schema for Table to Count
    #         :param table: Name of Table to Count
    #         :param aggregate_cols: SQL for Extra Aggregations to run (ie "MAX(ID) max_id")
    #         :param where: Where clause to use in query (ie. "id>100 AND id<200")
    #     """
    #     if where:
    #         where = "WHERE %s" % where
    #     if aggregate_cols:
    #         aggregate_cols = ", %s" % aggregate_cols
    #     quote_char = self.DEFAULT_QUOTE
    #     count_sql = self.COUNT_SQL.format(schema=schema, table=table, where=where, 
    #                                       aggregate_cols=aggregate_cols, q=quote_char)
    #     res = self.read_sql(count_sql)[0]
    #     if "ROWS" in res:
    #         res["rows"] = res["ROWS"]
        
        
    #     return res

    # def count_by_partition(self, schema, table, column, **kwargs):
    #     """ Run a count on a table's partitions """
    #     raise exceptions.MustImplement.default()

    # def execute_sql(self, sql, allow_reload=True):
    #     """ Execute query """
    #     self.validate_connection(allow_reload=allow_reload)
    #     return self._execute(sql)
