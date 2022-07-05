import IfxPyDbi as ifx
import pandas
from ibis.client import SQLClient, Database, Query
import ibis.expr.schema as sch
import ibis.expr.operations as ops

from . import api
from . import compiler


class InformixDatabase(Database):
    pass


class InformixQuery(Query):

    def __init__(self, client, ddl):
        super().__init__(client, ddl)

    def execute(self):
        sql = self.compiled_sql
        return pandas.read_sql(sql, self.client.conn)


class InformixTable(ops.DatabaseTable):
    pass


class InformixClient(SQLClient):

    query_class = InformixQuery
    database_class = InformixDatabase
    table_class = InformixTable
    dialect = compiler.InformixDialect

    def __init__(self, password, database, host='localhost', port=9088, user='informix', server='informix'):
        connect_string = f"SERVER={server};DATABASE={database};HOST={host};SERVICE={port};UID={user};PWD={password};"
        self.conn = ifx.connect(connect_string, "", "")

    def _build_ast(self, expr, context):  # TODO NEXT
        result = compiler.build_ast(expr, context)
        return result
    
    def _fully_qualified_name(self, name, database):
        return "{}.{}".format(database, name)

    def _get_table_schema(self, qualified_name):
        dataset, table = qualified_name.split(".")
        return self.get_schema(table, database=dataset)

    def _execute(self, dml, results=False, **kwargs):
        query = InformixQuery(self, dml)
        df = self._execute_query(query, **kwargs)
        if results:
            return df

        return None
    
    def _get_schema(self, database, tablename):
        sql = api.SCHEMA_SQL.format(tablename)
        schema_df = self._execute(sql, results=True)
        return self._schema_reformat(schema_df)

    def _schema_reformat(self, schema_df):
        schema_dict = schema_df.to_dict(orient='record')
        return_schema = []
        for entry in schema_dict:
            schema_field = {
                "names": entry['colname'].lower(),
                "types": api.to_ibis_type(entry['datatype'])
            }
            return_schema.append(schema_field)
        return pandas.DataFrame(return_schema)
    
    def get_schema(self, name, database=None):
        schema_df = self._get_schema(database, name)
        return sch.Schema(schema_df.names, schema_df.types)
