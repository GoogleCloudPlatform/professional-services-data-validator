import ibis

import clickhouse_driver  # NOQA fail early if the driver is missing

from data_validation.util import dvt_config_string_to_dict


def clickhouse_connect(
    host: str = "localhost",
    port: int = 9000,
    database: str = None,
    user: str = "default",
    password: str = None,
    client_name: str = "ibis",
    compression: str = None,
    json_params: str = None,
):
    """Connect to ClickHouse database using ibis.clickhouse.connect().

    Parameters
    ----------
    host
        ClickHouse server hostname
    port
        ClickHouse TCP port (default 9000)
    database
        Database to connect to
    user
        Username for authentication
    password
        Password for authentication
    client_name
        Client name for connection
    compression
        Compression type (e.g., 'lz4')
    json_params
        Additional connection parameters as JSON string

    Returns
    -------
    ClickhouseClient
        Connected ClickHouse client instance
    """
    # Parse json_params if provided
    extra_params = {}
    if json_params:
        extra_params = dvt_config_string_to_dict(json_params)

    # Merge all parameters
    connect_params = {
        "host": host,
        "port": int(port),  # Convert to int (handles CLI string args)
        "database": database,
        "user": user,
        "password": password,
        "client_name": client_name,
        "compression": compression,
        **extra_params,
    }

    # Remove None values to use ibis defaults
    connect_params = {k: v for k, v in connect_params.items() if v is not None}

    return ibis.clickhouse.connect(**connect_params)
