# Copyright(C) 2020 Google Inc.
#
# Licensed under the Apache License, Version 2.0(the "License"); you may not
# use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.See the
# License for the specific language governing permissions and limitations under
# the License.

import collections
import functools
import inspect
import itertools

import ibis.udf.validate as v
import ibis.expr.rules as rlz
import ibis.expr.datatypes as dt
from ibis.expr.signature import Argument as Arg
from ibis.snowflake.datatypes import UDFContext, ibis_type_to_snowflake_type
from ibis.bigquery.udf.core import PythonToJavaScriptTranslator

import ibis.expr.operations as ops

__all__ = ('udf',)

_udf_name_cache = collections.defaultdict(itertools.count)


class SnowflakeUDFNode(ops.ValueOp):
    pass


def create_udf_node(name, fields):
    """Create a new UDF node type
    Params:
    name: str
    The name of the UDF node
    fields: OrderedDict
    Mapping of class member name to definition

    Returns
    result: type
    SnowflakeUDFNode subclass
    """
    definition = next(_udf_name_cache[name])
    external_name = '{}_{:d}'.format(name, definition)
    return type(external_name, (SnowflakeUDFNode,), fields)

def udf(input_type, output_type, ):

    v.validate_output_type(output_type)

    def wrapper(f):
        if not callable(f):
            raise TypeError('f must be callable, got {}'.format(f))

        signature = inspect.signature(f)
        parameter_names = signature.parameters.keys()

        udf_node_fields = collections.OrderedDict(
            [
                (name, Arg(rlz.value(type)))
                for name, type in zip(parameter_names, input_type)
            ]
            + [
                (
                    'output_type',
                    lambda self, output_type=output_type: rlz.shape_like(
                        self.args, dtype=output_type
                    ),
                ),
                ('__slots__', ('js',)),
            ]
        )

        type_translation_context = UDFContext()
        return_type = ibis_type_to_snowflake_type(
            dt.dtype(output_type), type_translation_context
        )

        sf_signature = ', '.join(
            '{name} {type}'.format(
                name=name,
                type=ibis_type_to_snowflake_type(
                    dt.dtype(type), type_translation_context
                ),
            )
            for name, type in zip(parameter_names, input_type)
        )

        udf_node = create_udf_node(f.__name__, udf_node_fields)
        source = PythonToJavaScriptTranslator(f).compile() # Uses function translator implemented in ibis.bigquery.udf

        javascript_template = """\
CREATE {to_replace} FUNCTION {external_name}({signature})
RETURNS {return_type}
LANGUAGE JAVASCRIPT AS '{source}
return {internal_name}({args});'
""".format(to_replace="",
           external_name=udf_node.__name__,
           internal_name=f.__name__,
           signature = sf_signature,
           return_type=return_type,
           source = source)

        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            node = udf_node(*args, **kwargs)
            node.js = javascript_template
            return node.to_expr()

        wrapped.__signature__ = signature
        wrapped.js = javascript_template
        return wrapped
    return wrapper