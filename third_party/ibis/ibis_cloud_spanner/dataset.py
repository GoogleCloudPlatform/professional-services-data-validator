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
from third_party.ibis.ibis_cloud_spanner import table

import six
import copy
import google.cloud._helpers
import ibis
import third_party.ibis.ibis_cloud_spanner



def _get_table_reference(self, table_id):
    """Constructs a TableReference.
    Args:
        table_id (str): The ID of the table.
    Returns:
        cloud_spanner.table.TableReference:
            A table reference for a table in this dataset.
    """
    return table.TableReference(self, table_id)

class DatasetReference(object):
    """DatasetReferences are pointers to datasets.

    Args: 
        instance_id (str): The ID of the instance
        dataset_id (str): The ID of the dataset
    Raises:
        ValueError: If either argument is not of type ``str``.
    """

    def __init__(self, instance_id, dataset_id):
        '''if not isinstance(instance_id, six.string_types):
            raise ValueError("Pass a string for instance_id")
        if not isinstance(dataset_id, six.string_types):
            raise ValueError("Pass a string for dataset_id")'''
        self._instance_id = instance_id
        self._dataset_id = dataset_id

    table = _get_table_reference

    @property
    def instance_id(self):
        """str: Project ID of the dataset."""
        return self._instance_id

    @property
    def dataset_id(self):
        """str: Dataset ID."""
        return self._dataset_id



    @classmethod
    def from_api_repr(cls, resource):
        """Factory: construct a dataset reference given its API representation
        Args:
            resource (Dict[str, str]):
                Dataset reference resource representation returned from the API
        Returns:
            cloud_spanner.dataset.DatasetReference:
                Dataset reference parsed from ``resource``.
        """
        instance_id = resource["instanceId"]
        dataset_id = resource["datasetId"]
        return cls(instance_id, dataset_id)

    @classmethod
    def from_string(cls, dataset_id, default_instance_id=None):
        """Construct a dataset reference from dataset ID string.
        Args:
            dataset_id (str):
                A dataset ID in standard SQL format. If ``instance_id``
                is not specified, this must include both the instance ID and
                the dataset ID, separated by ``.``.
            default_instance_id (Optional[str]):
                The instance ID to use when ``dataset_id`` does not include a
                instance ID.
        Returns:
            DatasetReference:
                Dataset reference parsed from ``dataset_id``.

        """
        output_dataset_id = dataset_id
        output_instance_id = default_instance_id
        parts = dataset_id.split(".")
        parts = [part for part in parts if part]


        if len(parts) == 1 and not default_instance_id:
            raise ValueError(
                "When instance is not set, dataset_id must be a "
                "fully-qualified dataset ID in standard SQL format, "
                'e.g., "instance_id.dataset_id" got {}'.format(dataset_id)
            )
        elif len(parts) == 2:
            output_instance_id, output_dataset_id = parts
        elif len(parts) > 2:
            raise ValueError(
                "Too many parts in dataset_id. Expected a fully-qualified "
                "dataset ID in standard SQL format. e.g. "
                '"instance _id.dataset_id", got {}'.format(dataset_id)
            )

        return cls(output_instance_id, output_dataset_id)

    def to_api_repr(self):
        """Construct the API resource representation of this dataset reference
        Returns:
            Dict[str, str]: dataset reference represented as an API resource
        """
        return {"instanceId": self._instance_id, "datasetId": self._dataset_id}

    def _key(self):
        """A tuple key that uniquely describes this field.
        Used to compute this instance's hashcode and evaluate equality.
        Returns:
            Tuple[str]: The contents of this :class:`.DatasetReference`.
        """
        return (self._instance_id, self._dataset_id)

    def __eq__(self, other):
        if not isinstance(other, DatasetReference):
            return NotImplemented
        return self._key() == other._key()

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._key())

    def __repr__(self):
        return "DatasetReference{}".format(self._key())

