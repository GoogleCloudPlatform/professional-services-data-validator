import ibis.backends.base_sqlalchemy.compiler as comp
from ibis.backends.base_sql.compiler import BaseSelect, BaseTableSetFormatter
from ibis.backends import base_sql

_operation_registry = {**base_sql.operation_registry}

def _name_expr(formatted_expr, quoted_name):
    return "{} as {}".format(formatted_expr, quoted_name)


def build_ast(expr, context):
    builder = InformixQueryBuilder(expr, context=context)
    return builder.get_result()


def to_sql(expr, context):
    query_ast = build_ast(expr, context)
    compiled = query_ast.compile()
    return compiled


class InformixTableSetFormatter(BaseTableSetFormatter):
    @classmethod
    def _quote_identifier(self, name, quotechar='"'):
        if name.count(" "):
            return "{0}{1}{0}".format(quotechar, name)
        else:
            return name


class InformixUDFDefinition(comp.DDL):
    def __init__(self, expr, context):
        self.expr = expr
        self.context = context

    def compile(self):
        return self.expr.op().js


class InformixContext(comp.QueryContext):
    def _to_sql(self, expr, ctx):
        return to_sql(expr, context=ctx)


class InformixExprTranslator(comp.ExprTranslator):

    _registry = _operation_registry
    context_class = InformixContext

    def name(self, translated, name):
        quoted_name = InformixTableSetFormatter._quote_identifier(name)
        return _name_expr(translated, quoted_name)


class InformixSelect(BaseSelect):

    translator = InformixExprTranslator

    @property
    def table_set_formatter(self):
        return InformixTableSetFormatter


class InformixSelectBuilder(comp.SelectBuilder):
    @property
    def _select_class(self):
        return InformixSelect


class InformixQueryBuilder(comp.QueryBuilder):
    select_builder = InformixSelectBuilder


class InformixDialect(comp.Dialect):
    translator = InformixExprTranslator


dialect = InformixDialect
