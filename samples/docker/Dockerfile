FROM python:3.9.0-slim
ARG _APP_VERSION
COPY google_pso_data_validator-${_APP_VERSION}-py2.py3-none-any.whl .
RUN apt-get update \
&& apt-get install gcc -y \
&& apt-get clean
RUN pip install --upgrade pip
RUN pip install google_pso_data_validator-${_APP_VERSION}-py2.py3-none-any.whl
ENTRYPOINT ["python","-m","data_validation"]
