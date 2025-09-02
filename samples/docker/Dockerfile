FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:slim

# App version is passed as a build argument.
# If not provided, the latest version will be installed.
ARG _APP_VERSION=""

# Set working directory
WORKDIR /app

# Copy and set up the entrypoint script
COPY samples/docker/entrypoint.sh .
RUN chmod +x entrypoint.sh
# Need venv package
RUN apt-get update
RUN apt-get install -y python3-full
RUN python3 -m venv env
# Enable venv
ENV PATH="/app/env/bin:$PATH"

# Install the Data Validator from PyPI
RUN pip install --upgrade pip
RUN \
  if [ -z "$_APP_VERSION" ]; then \
    echo "Installing latest version of google-pso-data-validator"; \
    pip install google-pso-data-validator; \
  else \
    echo "Installing google-pso-data-validator version ${_APP_VERSION}"; \
    pip install google-pso-data-validator==${_APP_VERSION}; \
  fi

# Optional: Uncomment the following lines to install additional drivers.
# RUN pip install teradatasql
# RUN pip install oracledb
# RUN pip install pyodbc
# RUN pip install 'ibis-framework[impala]'
# RUN pip install snowflake-sqlalchemy
# RUN pip install snowflake-connector-python

# The entrypoint script will handle connections and run the application
ENTRYPOINT ["./entrypoint.sh"]
