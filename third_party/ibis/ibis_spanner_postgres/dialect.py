from sqlalchemy import sql, select, bindparam
from sqlalchemy.sql import sqltypes
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.dialects.postgresql import pg_catalog
from sqlalchemy.engine import reflection, ObjectKind
from sqlalchemy.engine.default import DefaultDialect

PG_TYPE_MAP = {
    "int8": "bigint",
    "numeric": "numeric",
    "float4": "real",
    "float8": "double precision",
    "varchar": "character varying",
    "date": "date",
    "timestamptz": "timestamp with time zone",
}


class SpannerPostgresDialectMixin(DefaultDialect):
    """
    Define Spanner-PostgreSQL-specific behavior.

    Most public methods are overrides of the underlying interfaces defined in
    :class:`~sqlalchemy.engine.interfaces.Dialect` and
    :class:`~sqlalchemy.engine.Inspector`.
    """

    name = "spanner_postgres"

    def _format_type(self, pg_type_str, pg_type_mod) -> str:
        """In Spanner the format_type() UDF is not present, this method converts pg_type values to something "similar" to format_type() output."""
        type_str = PG_TYPE_MAP[pg_type_str]
        if pg_type_mod != -1:
            type_str = f"{type_str}({pg_type_mod})"
        elif type_str == "numeric":
            # We cannot specify precision/scale when creating a table in Spanner, even in PostgreSQL dialect.
            type_str = f"{type_str}(38,9)"
        return type_str

    def get_multi_columns(self, connection, schema, filter_names, scope, kind, **kw):
        has_filter_names, params = self._prepare_filter_names(filter_names)
        relkinds = self._kind_to_relkinds(kind)

        # Build query similar to SQLAlchemy's _columns_query but without format_type
        query = (
            select(
                pg_catalog.pg_attribute.c.attname.label("name"),
                pg_catalog.pg_type.c.typname.label("typname"),
                pg_catalog.pg_attribute.c.atttypmod.label("atttypmod"),
                pg_catalog.pg_attribute.c.attnotnull.label("not_null"),
                pg_catalog.pg_class.c.relname.label("table_name"),
            )
            .select_from(pg_catalog.pg_class)
            .join(
                pg_catalog.pg_attribute,
                sql.and_(
                    pg_catalog.pg_class.c.oid == pg_catalog.pg_attribute.c.attrelid,
                    pg_catalog.pg_attribute.c.attnum > 0,
                    ~pg_catalog.pg_attribute.c.attisdropped,
                ),
            )
            .join(
                pg_catalog.pg_type,
                pg_catalog.pg_type.c.oid == pg_catalog.pg_attribute.c.atttypid,
            )
            .where(self._pg_class_relkind_condition(relkinds))
            .order_by(pg_catalog.pg_class.c.relname, pg_catalog.pg_attribute.c.attnum)
        )

        query = self._pg_class_filter_scope_schema(query, schema, scope=scope)
        if has_filter_names:
            query = query.where(
                pg_catalog.pg_class.c.relname.in_(bindparam("filter_names"))
            )

        rows = connection.execute(query, params).mappings()

        # Process rows to inject the computed format_type and dummy values for missing fields
        processed_rows = []
        for row in rows:
            processed_row = dict(row)
            processed_row["format_type"] = self._format_type(
                row["typname"], row["atttypmod"]
            )
            processed_row["default"] = None
            processed_row["comment"] = None
            processed_row["generated"] = None
            processed_row["identity_options"] = None
            processed_rows.append(processed_row)

        # Spanner PostgreSQL does not support domains or enums, and the SQLAlchemy
        # implementations use format_type() which is not available in Spanner.
        domains = {}
        enums = {}

        # Delegate to SQLAlchemy's internal helper to build the final structure
        columns = self._get_columns_info(processed_rows, domains, enums, schema)

        return columns.items()

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )
        # Why has table_oid got single quotes inside it?
        SQL_COLS = """
            SELECT a.attname,
              t.typname, a.atttypmod,
              a.attnotnull
            FROM pg_catalog.pg_attribute a
            INNER JOIN pg_catalog.pg_type t ON (t.oid = a.atttypid)
            WHERE a.attrelid = :table_oid
            AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
        """
        s = sql.text(SQL_COLS).bindparams(
            sql.bindparam("table_oid", type_=sqltypes.Integer)
        )
        c = connection.execute(s, dict(table_oid=table_oid))
        rows = c.fetchall()
        columns = []
        for name, pg_type_str, pg_type_mod, notnull in rows:
            column_info = self._get_column_info(
                name,
                self._format_type(pg_type_str, pg_type_mod),
                None,  # default_,
                notnull,
                None,  # domains,
                None,  # enums,
                schema,
                None,  # comment,
                None,  # generated,
                None,  # identity,
            )
            columns.append(column_info)
        return columns

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {"constrained_columns": [], "name": None}

    @reflection.cache
    def get_multi_pk_constraint(
        self, connection, schema, filter_names, scope, kind, **kw
    ):
        return []

    @reflection.cache
    def get_foreign_keys(
        self,
        connection,
        table_name,
        schema=None,
        postgresql_ignore_search_path=False,
        **kw,
    ):
        return []

    @reflection.cache
    def get_multi_foreign_keys(
        self, connection, schema, filter_names, scope, kind, **kw
    ):
        return []

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        return []

    @reflection.cache
    def get_multi_indexes(self, connection, schema, filter_names, scope, kind, **kw):
        return []

    @reflection.cache
    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        return []

    @reflection.cache
    def get_multi_check_constraints(
        self, connection, schema, filter_names, scope, kind, **kw
    ):
        return []

    @reflection.cache
    def get_multi_unique_constraints(
        self, connection, schema, filter_names, scope, kind, **kw
    ):
        return []

    @reflection.cache
    def get_multi_table_comment(
        self, connection, schema, filter_names, scope, kind, **kw
    ):
        return []


class SpannerPostgresDialect_psycopg2(SpannerPostgresDialectMixin, PGDialect_psycopg2):
    pass
