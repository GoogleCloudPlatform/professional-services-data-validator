import ibis.backends.postgres.compiler as postgresCompiler
import ibis.expr.operations as ops
import sqlalchemy as sa

def _string_join(t, expr):
    sep, elements = expr.op().args
    columns = [col.name for col in map(t.translate, elements)]
    return sa.sql.literal_column(" || ".join(columns))

_operation_registry = postgresCompiler._operation_registry.copy()
_operation_registry.update(
    {
        ops.StringJoin: _string_join,
    }
)


class RedShiftExprTranslator(postgresCompiler.PostgreSQLExprTranslator):
    _registry = _operation_registry


class RedShiftSQLDialect(postgresCompiler.PostgreSQLDialect):
    translator = RedShiftExprTranslator