# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import textwrap

import pytest
import ibis
import ibis.expr.datatypes as dt

# Import required in order to register operations.
import third_party.ibis.ibis_addon.operations  # noqa: F401


@pytest.fixture
def bigquery_client():
    return ibis.bigquery.connect()


def test_bit_xor_bigquery(bigquery_client):
    tbl = bigquery_client.table(
        "citibike_trips", database="bigquery-public-data.new_york_citibike"
    )
    expr = tbl["tripduration"].bit_xor().name("checksum")
    sql = expr.compile()
    assert (
        sql
        == textwrap.dedent(
            """
    SELECT BIT_XOR(t0.`tripduration`) AS `checksum`
    FROM `bigquery-public-data.new_york_citibike.citibike_trips` t0
    """
        ).strip()
    )


def test_hash_bigquery_string(bigquery_client):
    tbl = bigquery_client.table(
        "citibike_trips", database="bigquery-public-data.new_york_citibike"
    )
    expr = tbl[
        tbl["start_station_name"].hash(how="farm_fingerprint").name("station_hash")
    ]
    sql = expr.compile()
    assert (
        sql
        == textwrap.dedent(
            """
    SELECT farm_fingerprint(t0.`start_station_name`) AS `station_hash`
    FROM `bigquery-public-data.new_york_citibike.citibike_trips` t0
    """
        ).strip()
    )


def test_hash_bigquery_binary(bigquery_client):
    tbl = bigquery_client.table(
        "citibike_trips", database="bigquery-public-data.new_york_citibike"
    )
    expr = tbl[
        tbl["start_station_name"]
        .cast(dt.binary)
        .hash(how="farm_fingerprint")
        .name("station_hash")
    ]
    sql = expr.compile()
    # TODO: Update the expected SQL to be a valid query once
    #       https://github.com/ibis-project/ibis/issues/2354 is fixed.
    assert (
        sql
        == textwrap.dedent(
            """
    SELECT farm_fingerprint(CAST(t0.`start_station_name` AS BYTES)) AS `station_hash`
    FROM `bigquery-public-data.new_york_citibike.citibike_trips` t0
    """
        ).strip()
    )


def test_hashbytes_bigquery_string(bigquery_client):
    tbl = bigquery_client.table(
        "citibike_trips", database="bigquery-public-data.new_york_citibike"
    )
    expr = tbl[tbl["start_station_name"].hashbytes(how="sha256").name("station_hash")]
    sql = expr.compile()
    assert (
        sql
        == textwrap.dedent(
            """
    SELECT TO_HEX(SHA256(t0.`start_station_name`)) AS `station_hash`
    FROM `bigquery-public-data.new_york_citibike.citibike_trips` t0
    """
        ).strip()
    )


def test_hashbytes_bigquery_binary(bigquery_client):
    tbl = bigquery_client.table(
        "citibike_trips", database="bigquery-public-data.new_york_citibike"
    )
    expr = tbl[
        tbl["start_station_name"]
        .cast(dt.binary)
        .hashbytes(how="sha256")
        .name("station_hash")
    ]
    sql = expr.compile()
    # TODO: Update the expected SQL to be a valid query once
    #       https://github.com/ibis-project/ibis/issues/2354 is fixed.
    assert (
        sql
        == textwrap.dedent(
            """
    SELECT TO_HEX(SHA256(CAST(t0.`start_station_name` AS BYTES))) AS `station_hash`
    FROM `bigquery-public-data.new_york_citibike.citibike_trips` t0
    """
        ).strip()
    )
