# Copyright 2020 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import itertools

import sqlalchemy as sa
from ibm_db_sa import dialect as db2_dialect

import ibis.expr.rules as rlz
import ibis.udf.validate as v
from ibis import IbisError
from ibis.expr.signature import Argument as Arg
from third_party.ibis.ibis_DB2.alchemy import _to_sqla_type
from third_party.ibis.ibis_DB2.compiler import (
    DB2ExprTranslator,
    DB2UDFNode,
    add_operation,
)

_udf_name_cache = collections.defaultdict(itertools.count)


class DB2UDFError(IbisError):
    pass


def ibis_to_db2_sa_type(ibis_type):
    """Map an ibis DataType to a DB2-compatible sqlalchemy type"""
    return _to_sqla_type(ibis_type, type_map=DB2ExprTranslator._type_map)


def sa_type_to_db2_str(sa_type):
    """Map a DB2-compatible sqlalchemy type to a DB2-appropriate
    string"""
    if callable(sa_type):
        sa_type = sa_type()
    return sa_type.compile(dialect=db2_dialect())


def ibis_to_DB2_str(ibis_type):
    """Map an ibis DataType to a DB2-appropriate string"""
    return sa_type_to_db2_str(ibis_to_db2_sa_type(ibis_type))


#######################################################################
def create_udf_node(name, fields):
    """Create a new UDF node type.

    Parameters
    ----------
    name : str
        Then name of the UDF node
    fields : DB2Dict
        Mapping of class member name to definition

    Returns
    -------
    type
        A new DB2UDFNode subclass
    """
    definition = next(_udf_name_cache[name])
    external_name = '{}_{:d}'.format(name, definition)
    return type(external_name, (DB2UDFNode,), fields)


#######################################################################
def existing_udf(name, input_types, output_type, schema=None, parameters=None):
    """Create an ibis function that refers to an existing DB2 UDF already
    defined in database

    Parameters
    ----------
    name: str
    input_types : List[DataType]
    output_type : DataType
    schema: str - optionally specify the schema that the UDF is defined in
    parameters: List[str] - give names to the arguments of the UDF

    Returns
    -------
    Callable
        The wrapped function
    """

    if parameters is None:
        parameters = ['v{}'.format(i) for i in range(len(input_types))]
    elif len(input_types) != len(parameters):
        raise ValueError(
            (
                "Length mismatch in arguments to existing_udf: "
                "len(input_types)={}, len(parameters)={}"
            ).format(len(input_types), len(parameters))
        )

    v.validate_output_type(output_type)

    udf_node_fields = collections.OrderedDict(
        [
            (name, Arg(rlz.value(type_)))
            for name, type_ in zip(parameters, input_types)
        ]
        + [
            (
                'output_type',
                lambda self, output_type=output_type: rlz.shape_like(
                    self.args, dtype=output_type
                ),
            )
        ]
    )
    udf_node_fields['resolve_name'] = lambda self: name

    udf_node = create_udf_node(name, udf_node_fields)

    def _translate_udf(t, expr):
        func_obj = sa.func
        if schema is not None:
            func_obj = getattr(func_obj, schema)
        func_obj = getattr(func_obj, name)

        sa_args = [t.translate(arg) for arg in expr.op().args]

        return func_obj(*sa_args)

    add_operation(udf_node, _translate_udf)

    def wrapped(*args, **kwargs):
        node = udf_node(*args, **kwargs)
        return node.to_expr()

    return wrapped
