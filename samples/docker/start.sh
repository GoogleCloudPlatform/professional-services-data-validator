#!/bin/bash

cat /etc/motd

echo "====================================="

echo "Data Validation Tool Help:"
echo "====================================="
echo "To view help the Data Validation Tool, run the following command:"
echo "dvt -h"
echo "====================================="

if [ -z "$CLOUD_SQL_INSTANCE" ]; then
    echo "CLOUD_SQL_INSTANCE environment variable is not set. Please export it to start Cloud SQL Proxy."
    echo "Example: export CLOUD_SQL_INSTANCE=project:region:instance"
    echo "Then start the proxy with: ./cloud_sql_proxy $CLOUD_SQL_INSTANCE=tcp:15432 &"
else
    echo "Starting Cloud SQL Proxy..."
    ./cloud_sql_proxy $CLOUD_SQL_INSTANCE=tcp:15432 &
fi

touch /etc/data/bash_history
ln -s /etc/data/bash_history /root/.bash_history

/bin/bash