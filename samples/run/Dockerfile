FROM python:3.9-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

# Install production dependencies.
RUN apt-get update \
    && apt-get install gcc -y \
    && apt-get clean
RUN pip install --upgrade pip
RUN pip install Flask gunicorn google_pso_data_validator

# Hive/Impala Dependencies 
# RUN pip install hdfs
# RUN pip install thrift-sasl

# Oracle Dependencies
# if you are using Oracle you should add .rpm files
# under your license to a directory called oracle/
# and then uncomment the setup below.

# ENV ORACLE_SID oracle
# ENV ORACLE_ODBC_VERSION 12.2
# ENV ORACLE_HOME /usr/lib/oracle/${ORACLE_ODBC_VERSION}/client64

# RUN pip install cx_Oracle
# RUN apt-get -y install --fix-missing --upgrade vim alien unixodbc-dev wget libaio1 libaio-dev

# COPY oracle/*.rpm ./
# RUN alien -i *.rpm && rm *.rpm \
#     && echo "/usr/lib/oracle/${ORACLE_ODBC_VERSION}/client64/lib/" > /etc/ld.so.conf.d/oracle.conf \
#     && ln -s /usr/include/oracle/${ORACLE_ODBC_VERSION}/client64 $ORACLE_HOME/include \
#     && ldconfig -v


# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
