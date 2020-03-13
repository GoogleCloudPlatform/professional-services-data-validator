""" Teradata ibis client implementation """

# import datetime
# from collections import OrderedDict
# from typing import Optional, Tuple

# import google.cloud.bigquery as bq
import pandas
import teradatasql
# import regex as re
# from google.api_core.exceptions import NotFound
from multipledispatch import Dispatcher
# from pkg_resources import parse_version

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.lineage as lin
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir

from ibis.client import Database, Query, SQLClient
from ibis.common.exceptions import UnsupportedBackendType

from . import compiler # TODO non local import ie from ibis.teradata import compiler
from .datatypes import ibis_type_to_teradata_type # TODO non local import


NATIVE_PARTITION_COL = '_PARTITIONTIME'


_DTYPE_TO_IBIS_TYPE = {
    'INT64': dt.int64,
    'FLOAT64': dt.double,
    'BOOL': dt.boolean,
    'STRING': dt.string,
    'DATE': dt.date,
    # FIXME: enforce no tz info
    'DATETIME': dt.timestamp,
    'TIME': dt.time,
    'TIMESTAMP': dt.timestamp,
    'BYTES': dt.binary,
    'NUMERIC': dt.Decimal(38, 9),
}

# TODO implement
# @dt.dtype.register(bq.schema.SchemaField)
def teradata_field_to_ibis_dtype(col_data):
    """ Convert Teradata data to an ibis type."""
    typ = field.field_type
    if typ == 'RECORD':
        fields = field.fields
        assert fields, 'RECORD fields are empty'
        names = [el.name for el in fields]
        ibis_types = list(map(dt.dtype, fields))
        ibis_type = dt.Struct(names, ibis_types)
    else:
        ibis_type = _LEGACY_TO_STANDARD.get(typ, typ)
        ibis_type = _DTYPE_TO_IBIS_TYPE.get(ibis_type, ibis_type)
    if field.mode == 'REPEATED':
        ibis_type = dt.Array(ibis_type)
    return ibis_type


# @sch.infer.register(bq.table.Table)
# def bigquery_schema(table):
#     """Infer the schema of a BigQuery `table` object."""
#     fields = OrderedDict((el.name, dt.dtype(el)) for el in table.schema)
#     partition_info = table._properties.get('timePartitioning', None)

#     # We have a partitioned table
#     if partition_info is not None:
#         partition_field = partition_info.get('field', NATIVE_PARTITION_COL)

#         # Only add a new column if it's not already a column in the schema
#         fields.setdefault(partition_field, dt.timestamp)
#     return sch.schema(fields)


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


    def __init__(self, client, ddl):
        super().__init__(client, ddl)

        # self.expr comes from the parent class
        query_parameter_names = dict(
            lin.traverse(_find_scalar_parameter, self.expr)
        )
        # self.query_parameters = [
        #     teradata_param(
        #         param.to_expr().name(query_parameter_names[param]), value
        #     )
        #     for param, value in (query_parameters or {}).items()
        # ]

    def execute(self):
        return pandas.read_sql(self.compiled_sql, self.client)


class TeradataDatabase(Database):
    """A Teradata dataset """


# teradata_param = Dispatcher('teradata_param')


# @teradata_param.register(ir.StructScalar, OrderedDict)
# def bq_param_struct(param, value):
#     field_params = [teradata_param(param[k], v) for k, v in value.items()]
#     result = bq.StructQueryParameter(param.get_name(), *field_params)
#     return result


# @teradata_param.register(ir.ArrayValue, list)
# def bq_param_array(param, value):
#     param_type = param.type()
#     assert isinstance(param_type, dt.Array), str(param_type)

#     try:
#         bigquery_type = ibis_type_to_bigquery_type(param_type.value_type)
#     except NotImplementedError:
#         raise UnsupportedBackendType(param_type)
#     else:
#         if isinstance(param_type.value_type, dt.Struct):
#             query_value = [
#                 teradata_param(param[i].name('element_{:d}'.format(i)), struct)
#                 for i, struct in enumerate(value)
#             ]
#             bigquery_type = 'STRUCT'
#         elif isinstance(param_type.value_type, dt.Array):
#             raise TypeError('ARRAY<ARRAY<T>> is not supported in BigQuery')
#         else:
#             query_value = value
#         result = bq.ArrayQueryParameter(
#             param.get_name(), bigquery_type, query_value
#         )
#         return result


# @teradata_param.register(
#     ir.TimestampScalar, (str, datetime.datetime, datetime.date)
# )
# def bq_param_timestamp(param, value):
#     assert isinstance(param.type(), dt.Timestamp), str(param.type())

#     # TODO(phillipc): Not sure if this is the correct way to do this.
#     timestamp_value = pandas.Timestamp(value, tz='UTC').to_pydatetime()
#     return bq.ScalarQueryParameter(
#         param.get_name(), 'TIMESTAMP', timestamp_value
#     )


# @teradata_param.register(ir.StringScalar, str)
# def bq_param_string(param, value):
#     return bq.ScalarQueryParameter(param.get_name(), 'STRING', value)


# @teradata_param.register(ir.IntegerScalar, int)
# def bq_param_integer(param, value):
#     return bq.ScalarQueryParameter(param.get_name(), 'INT64', value)


# @teradata_param.register(ir.FloatingScalar, float)
# def bq_param_double(param, value):
#     return bq.ScalarQueryParameter(param.get_name(), 'FLOAT64', value)


# @teradata_param.register(ir.BooleanScalar, bool)
# def bq_param_boolean(param, value):
#     return bq.ScalarQueryParameter(param.get_name(), 'BOOL', value)


# @teradata_param.register(ir.DateScalar, str)
# def bq_param_date_string(param, value):
#     return teradata_param(param, pandas.Timestamp(value).to_pydatetime().date())


# @teradata_param.register(ir.DateScalar, datetime.datetime)
# def bq_param_date_datetime(param, value):
#     return teradata_param(param, value.date())


# @teradata_param.register(ir.DateScalar, datetime.date)
# def bq_param_date(param, value):
#     return bq.ScalarQueryParameter(param.get_name(), 'DATE', value)


class TeradataTable(ops.DatabaseTable):
    pass


class TeradataClient(SQLClient):
    """ An ibis Teradata client implementation """

    query_class = TeradataQuery
    database_class = TeradataDatabase
    table_class = TeradataTable
    dialect = compiler.TeradataDialect

    def __init__(self, host, user_name, password, port=1025):
        """Construct a BigQueryClient.
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
        """
        self.teradata_config = {
            "host": host, 
            "user": user_name, 
            "password": password, 
            "dbs_port": port
        }

        self.client = teradatasql.connect(**self.teradata_config)

    # def table(self, name, database=None):
    #     t = super().table(name, database=database)
    #     project, dataset, name = t.op().name.split('.')
    #     dataset_ref = self.client.dataset(dataset, project=project)
    #     table_ref = dataset_ref.table(name)
    #     bq_table = self.client.get_table(table_ref)
    #     return rename_partitioned_column(t, bq_table)

    def _build_ast(self, expr, context): # TODO NEXT
        result = comp.build_ast(expr, context)
        return result

    def _fully_qualified_name(self, name, database):
        return "{}.{}".format(database, name)

    def _get_table_schema(self, qualified_name):
        dataset, table = qualified_name.rsplit('.', 1)
        assert dataset is not None, "dataset is None"
        return self.get_schema(table, database=dataset)

    def sql(self, query): # TODO is this ever used?  its uselss, queries dont return schemas
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
        limited_query = 'SELECT * FROM ({}) t0 SAMPLE 0'.format(query)
        schema = self._get_schema_using_query(limited_query)
        return ops.SQLQueryResult(query, schema, self).to_expr()

    def _get_schema_using_query(self, limited_query):
        schema_df = self._execute(limited_query, results=True)
        names, ibis_types = self._adapt_types(schema_df)

        return sch.Schema(names, ibis_types)

    def _adapt_types(self, schema_df):
        names = []
        adapted_types = []
        for col_data in schema_df.to_dict(orient="record"):
            names.append(col_data["TODO"])
            typename = bigquery_field_to_ibis_dtype(col_data)
            adapted_types.append(typename)

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
    """ # TODO move somewhere better
    def list_databases(self, like=None):
        database_like = like or ""
        
        list_database_sql = self.LIST_DATABASE_SQL.format(database_like=database_like)
        databases_df = self._execute(list_database_sql, results=True)

        return list(databases_df.DatabaseName)

    LIST_TABLE_SQL = """
        SELECT * FROM DBC.Tables 
        WHERE DatabaseName LIKE '%{database_like}%'
              AND TableName LIKE '%{table_like}%'
    """ # TODO move somewhere better
    def list_tables(self, like=None, database=None):
        database = database or ""
        table = like or ""
        
        list_table_sql = self.LIST_TABLE_SQL.format(database_like=database, table_like=table)
        tables_df = self._execute(list_table_sql, results=True)

        return list(tables_df.TableName)

    def get_schema(self, name, database=None):
        project, dataset = self._parse_project_and_dataset(database)
        table_ref = self.client.dataset(dataset, project=project).table(name)
        bq_table = self.client.get_table(table_ref)
        return sch.infer(bq_table)

    @property
    def version(self):
        return parse_version(bq.__version__)

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
