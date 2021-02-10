# Copyright 2021 Google Inc.
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

from __future__ import absolute_import
from google.api_core.page_iterator import HTTPIterator
from google.cloud.spanner_v1 import TypeCode
from third_party.ibis.ibis_cloud_spanner import dataset as dataset_class

import copy
import datetime
import functools
import logging
import operator
import pytz
import warnings
import six
import google.api_core.exceptions
import google.cloud._helpers

try:
    import pandas
except ImportError:  # pragma: NO COVER
    pandas = None

try:
    import pyarrow
except ImportError:  # pragma: NO COVER
    pyarrow = None



code_to_spanner_dtype_dict = {
    1 : 'bool',
    2 : 'int64',
    3 : 'float64',
    4 : 'timestamp',
    5 : 'date',
    6 : 'string',
    7 : 'binary',
    8 : 'array',
    10 : 'decimal'
}

def _parse_3_part_id(full_id, default_instance=None, property_name="table_id"):
    output_instance_id = default_instance
    output_dataset_id = None
    output_resource_id = None
    parts = full_id.split(".")
    parts = [part for part in parts if part]
    if len(parts) != 2 and len(parts) != 3:
        raise ValueError(
            "{property_name} must be a fully-qualified ID in "
            'standard SQL format, e.g., "instance.dataset.{property_name}", '
            "got {}".format(full_id, property_name=property_name)
        )
    if len(parts) == 2 and not default_instance:
        raise ValueError(
            "When default_instance is not set, {property_name} must be a "
            "fully-qualified ID in standard SQL format, "
            'e.g., "instance.dataset_id.{property_name}", got {}'.format(
                full_id, property_name=property_name
            )
        )
    if len(parts) == 2:
        output_dataset_id, output_resource_id = parts
    else:
        output_instance_id, output_dataset_id, output_resource_id = parts
    return output_instance_id, output_dataset_id, output_resource_id


class TableReference(object):
    """TableReferences are pointers to tables.

    Args:
        dataset_ref (cloud_spanner.dataset.DatasetReference):
            A pointer to the dataset
        table_id (str): The ID of the table
    """

    def __init__(self, dataset_ref, table_id):
        self._instance = dataset_ref.instance_id
        self._dataset_id = dataset_ref.dataset_id
        self._table_id = table_id

    @property
    def instance(self):
        """str: instance bound to the table"""
        return self._instance

    @property
    def dataset_id(self):
        """str: ID of dataset containing the table."""
        return self._dataset_id

    @property
    def table_id(self):
        """str: The table ID."""
        return self._table_id

		
    # we use this in Tableclass.py so keep it
    @classmethod
    def from_string(cls, table_id, default_instance=None):
        """Construct a table reference from table ID string.
        Args:
            table_id (str):
                A table ID in standard SQL format. If ``instance``
                is not specified, this must included a instance ID, dataset
                ID, and table ID, each separated by ``.``.
            default_instance (Optional[str]):
                The instance ID to use when ``table_id`` does not
                include a instance ID.
        Returns:
            TableReference: Table reference parsed from ``table_id``.
        Examples:
            >>> TableReference.from_string('my-instance.mydataset.mytable')
            TableRef...(DatasetRef...('my-instance', 'mydataset'), 'mytable')
        Raises:
            ValueError:
                If ``table_id`` is not a fully-qualified table ID in
                standard SQL format.
        """
        
        

        (
            output_instance_id,
            output_dataset_id,
            output_table_id,
        ) = _parse_3_part_id(
            table_id, default_instance=default_instance, property_name="table_id"
        )

        return cls(
            dataset_class.DatasetReference(output_instance_id, output_dataset_id), output_table_id
        )

    @classmethod
    def from_api_repr(cls, resource):
        """Factory:  construct a table reference given its API representation
        Args:
            resource (Dict[str, object]):
                Table reference representation returned from the API
        Returns:
            cloud_spanner.table.TableReference:
                Table reference parsed from ``resource``.
        """
        

        instance = resource["instanceId"]
        dataset_id = resource["datasetId"]
        table_id = resource["tableId"]
        return cls(dataset_class.DatasetReference(instance, dataset_id), table_id)

    def to_api_repr(self):
        """Construct the API resource representation of this table reference.
        Returns:
            Dict[str, object]: Table reference represented as an API resource
        """
        return {
            "instanceId": self._instance,
            "datasetId": self._dataset_id,
            "tableId": self._table_id,
        }

    

    def __eq__(self, other):
        if not isinstance(other, TableReference):
            return NotImplemented
        return self._key() == other._key()

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._key())

    def __repr__(self):
        
        dataset_ref = dataset_class.DatasetReference(self._instance, self._dataset_id)
        return "TableReference({}, '{}')".format(repr(dataset_ref), self._table_id)



def _table_arg_to_table_ref(value, default_instance=None):
    """Helper to convert a string or Table to TableReference.
    This function keeps TableReference and other kinds of objects unchanged.
    """
    if isinstance(value, six.string_types):
        value = TableReference.from_string(value, default_instance=default_instance)

    return value

class Table(object):
    """Tables represent a set of rows whose values correspond to a schema.

    Args:
        table_ref (Union[cloud_spanner.table.TableReference, str]):
            A pointer to a table. If ``table_ref`` is a string, it must
            included a instance ID, dataset ID, and table ID, each separated
            by ``.``.
        schema (Optional):
            The table's schema. If any item is a mapping, its content must be
            compatible with
    """


    def __init__(self, table_ref, schema=None):
        table_ref = _table_arg_to_table_ref(table_ref)
        self._properties = {"tableReference": table_ref.to_api_repr(), "labels": {}}
        # Let the @property do validation.
        if schema is not None:
            self.schema = schema

    @property
    def instance(self):
        """str: instance bound to the table."""
        return self._properties["tableReference"]["instanceId"]

    @property
    def dataset_id(self):
        """str: ID of dataset containing the table."""
        return self._properties["tableReference"]["datasetId"]

    @property
    def table_id(self):
        """str: ID of the table."""
        return self._properties["tableReference"]["tableId"]
    

    @property
    def path(self):
        """str: URL path for the table's APIs."""
        return "/instances/%s/datasets/%s/tables/%s" % (
            self.instance,
            self.dataset_id,
            self.table_id,
        )



    @property
    def schema(self):

        records_list=[]
        from google.cloud import spanner
        spanner_client = spanner.Client()
        instance_id = self.instance
        instance = spanner_client.instance(instance_id)
        db_id = self.dataset_id
        database = instance.database(db_id)
        with database.snapshot() as snapshot:
            query="select * from {} limit 0".format(self.table_id)
            results=snapshot.execute_sql(query)
            for row in results:
                records_list.append(row)

        schema_list=list(results.fields)

        final=[]

        for item in schema_list:
            field_name = item.name
            if(item.type_.code == TypeCode.ARRAY):
                field_type = 'array<{}>'.format(code_to_spanner_dtype_dict[item.type_.array_element_type.code])
            else :
                field_type = code_to_spanner_dtype_dict[item.type_.code]
            final_item = ( field_name , field_type )

            final.append(final_item)


        return final

    @schema.setter
    def schema(self, value):
        if value is None:
            self._properties["schema"] = None
        else:
            value = _to_schema_fields(value)
            self._properties["schema"] = {"fields": _build_schema_resource(value)}


    @classmethod
    def from_string(cls, full_table_id):
        """Construct a table from fully-qualified table ID.
        Args:
            full_table_id (str):
                A fully-qualified table ID in standard SQL format. Must
                included a instance ID, dataset ID, and table ID, each
                separated by ``.``.
        Returns:
            Table: Table parsed from ``full_table_id``.

        """
        return cls(TableReference.from_string(full_table_id))

    @classmethod
    def from_api_repr(cls, resource):
        """Factory: construct a table given its API representation
        Args:
            resource (Dict[str, object]):
                Table resource representation from the API
        Returns:
            cloud_spanner.table.Table: Table parsed from ``resource``.

        """


        if (
            "tableReference" not in resource
            or "tableId" not in resource["tableReference"]
        ):
            raise KeyError(
                "Resource lacks required identity information:"
                '["tableReference"]["tableId"]'
            )
        instance_id = resource["tableReference"]["instanceId"]
        table_id = resource["tableReference"]["tableId"]
        dataset_id = resource["tableReference"]["datasetId"]
        dataset_ref = dataset_class.DatasetReference(instance_id, dataset_id)

        table = cls(dataset_ref.table(table_id))
        table._properties = resource

        return table

    def to_api_repr(self):
        """Constructs the API resource of this table
        Returns:
            Dict[str, object]: Table represented as an API resource
        """
        return copy.deepcopy(self._properties)



    def __repr__(self):
        d_ref = dataset_class.DatasetReference(self.instance, self.dataset_id)
        return "Table({})".format(TableReference(d_ref, self.table_id))



