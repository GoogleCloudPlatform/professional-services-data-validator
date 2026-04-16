FROM python:3.11-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

ENV PIPOPTS="--root-user-action=ignore"

# Install dependencies.
RUN pip install ${PIPOPTS} --upgrade pip
RUN pip install ${PIPOPTS} Flask gunicorn google_pso_data_validator

# Optional: Uncomment the following lines to install additional drivers.
# RUN pip install ${PIPOPTS} 'ibis-framework[impala]'
# RUN pip install ${PIPOPTS} oracledb
# RUN pip install ${PIPOPTS} pyodbc
# RUN pip install ${PIPOPTS} snowflake-sqlalchemy snowflake-connector-python
# RUN pip install ${PIPOPTS} teradatasql

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
