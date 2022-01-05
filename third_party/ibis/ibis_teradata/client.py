""" Teradata ibis client implementation """

# from typing import Optional, Tuple
import pandas
import teradatasql

import ibis.expr.lineage as lin
import ibis.expr.operations as ops
import ibis.expr.schema as sch

from ibis.client import Database, Query, SQLClient

from . import compiler  # TODO non local import ie from ibis.teradata import compiler
from .datatypes import TeradataTypeTranslator  # TODO non local import


def _find_scalar_parameter(expr):
    """Find all :class:`~ibis.expr.types.ScalarParameter` instances.
    Parameters
    ----------
    expr : ibis.expr.types.Expr
    Returns
    -------
    Tuple[bool, object]
        The operation and the parent expresssion's resolved name.
    """
    op = expr.op()

    if isinstance(op, ops.ScalarParameter):
        result = op, expr.get_name()
    else:
        result = None
    return lin.proceed, result


class TeradataQuery(Query):

    NO_LOCK_SQL = "LOCKING ROW FOR ACCESS "

    def __init__(self, client, ddl):
        super().__init__(client, ddl)

    def execute(self):
        sql = self.compiled_sql
        if self.client.use_no_lock_tables and sql.strip().startswith("SELECT"):
            sql = self.NO_LOCK_SQL + self.compiled_sql

        return pandas.read_sql(sql, self.client.client)


class TeradataDatabase(Database):
    """A Teradata dataset """

    pass


class TeradataTable(ops.DatabaseTable):
    pass


class TeradataClient(SQLClient):
    """ An ibis Teradata client implementation """

    query_class = TeradataQuery
    database_class = TeradataDatabase
    table_class = TeradataTable
    dialect = compiler.TeradataDialect

    def __init__(self, host, user_name, password, port=1025, logmech='TD2', use_no_lock_tables=False):
        """Construct a TeradataClient.

        Parameters
        ----------
        host : str
            The Database host to connect to
        user_name : str
            A Database username to connect with
        password : str
            Password for supplied username
        port : Optional[int]
            The database port to connect to (default. 1025)
        logmech : Optional[int]
            The logmech type to connect to (default. TD2)
        """
        self.teradata_config = {
            "host": host,
            "user": user_name,
            "password": password,
            "dbs_port": port,
            "logmech": logmech,
        }

        self.client = teradatasql.connect(**self.teradata_config)
        self.use_no_lock_tables = use_no_lock_tables

    def _execute(self, dml, results=False, **kwargs):
        query = TeradataQuery(self, dml)
        df = self._execute_query(query, **kwargs)
        if results:
            return df

        return None

    def sql(
        self, query
    ):  # TODO is this ever used?  its uselss, queries dont return schemas
        """ Convert a SQL query to an Ibis table expression.
        Parameters
        ----------
        query : string
        Returns
        -------
        table : TableExpr
        """
        # Get the schema by adding a LIMIT 0 on to the end of the query. If
        # there is already a limit in the query, we find and remove it
        limited_query = "SELECT * FROM ({}) t0 SAMPLE 0".format(query)
        schema = self._get_schema_using_query(limited_query)
        return ops.SQLQueryResult(query, schema, self).to_expr()

    def _build_ast(self, expr, context):  # TODO NEXT
        result = compiler.build_ast(expr, context)
        return result

    def _fully_qualified_name(self, name, database):
        return "{}.{}".format(database, name)

    def _breakdown_qualified_name(self, qualified_name):
        database, table = qualified_name.split(".")
        return database, table

    def _get_table_schema(self, qualified_name):  # TODO should any validation happen?
        dataset, table = self._breakdown_qualified_name(qualified_name)
        return self.get_schema(table, database=dataset)

    def get_schema(self, name, database=None):
        schema_df = self._get_teradata_schema(database, name)
        return sch.Schema(schema_df.names, schema_df.types)

    TABLE_SCHEMA_SQL = """
    HELP COLUMN {database}.{table}.*;
    """  # TODO move somewhere better

    def _get_teradata_schema(self, database, table):
        table_schema_sql = self.TABLE_SCHEMA_SQL.format(database=database, table=table)
        schema_df = self._execute(table_schema_sql, results=True)

        clean_schema_df = self._clean_teradata_schema(schema_df)
        return clean_schema_df

    def _clean_teradata_schema(self, schema_df):
        schema_list = schema_df.to_dict(orient="record")
        clean_schema = []
        for col_data in schema_list:
            schema_field = {
                "names": col_data["Column SQL Name"].rstrip(),
                "types": TeradataTypeTranslator.to_ibis(col_data),
            }
            clean_schema.append(schema_field)
        return pandas.DataFrame(clean_schema)

    def _get_schema_using_query(self, limited_query):
        schema_df = self._execute(limited_query, results=True)
        names, ibis_types = self._adapt_types(schema_df)

        return sch.Schema(names, ibis_types)

    def _adapt_types(self, schema_df):
        names = []
        adapted_types = []
        # for col_data in schema_df.to_dict(orient="record"):
        #     names.append(col_data["TODO"])
        #     typename = bigquery_field_to_ibis_dtype(col_data)
        #     adapted_types.append(typename)

        return names, adapted_types

    def database(self, name=None):
        if name is None and self.dataset is None:
            raise ValueError(
                "Unable to determine BigQuery dataset. Call "
                "client.database('my_dataset') or set_database('my_dataset') "
                "to assign your client a dataset."
            )
        return self.database_class(name or self.dataset, self)

    # @property
    # def current_database(self):
    #     return self.database(self.dataset)

    # def set_database(self, name):
    #     self.data_project, self.dataset = self._parse_project_and_dataset(name)

    # def exists_database(self, name):
    #     project, dataset = self._parse_project_and_dataset(name)
    #     client = self.client
    #     dataset_ref = client.dataset(dataset, project=project)
    #     try:
    #         client.get_dataset(dataset_ref)
    #     except NotFound:
    #         return False
    #     else:
    #         return True

    LIST_DATABASE_SQL = """
    SELECT * FROM DBC.Databases
    WHERE DatabaseName LIKE '%{database_like}%'
    """  # TODO move somewhere better

    def list_databases(self, like=None):
        database_like = like or ""

        list_database_sql = self.LIST_DATABASE_SQL.format(database_like=database_like)
        databases_df = self._execute(list_database_sql, results=True)

        return list(databases_df.DatabaseName)

    LIST_TABLE_SQL = """
        SELECT * FROM DBC.Tables
        WHERE DatabaseName LIKE '%{database_like}%'
              AND TableName LIKE '%{table_like}%'
    """  # TODO move somewhere better

    def list_tables(self, like=None, database=None):
        database = database or ""
        table = like or ""

        list_table_sql = self.LIST_TABLE_SQL.format(
            database_like=database, table_like=table
        )
        tables_df = self._execute(list_table_sql, results=True)

        return list(tables_df.TableName)

    @property
    def version(self):
        return teradatasql.vernumber.sVersionNumber

    # def exists_table(self, name, database=None):
    #     project, dataset = self._parse_project_and_dataset(database)
    #     client = self.client
    #     dataset_ref = self.client.dataset(dataset, project=project)
    #     table_ref = dataset_ref.table(name)
    #     try:
    #         client.get_table(table_ref)
    #     except NotFound:
    #         return False
    #     else:
    #         return True
