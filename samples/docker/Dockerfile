FROM python:3.7.0-slim
ARG APP_VERSION
COPY google_pso_data_validator-$APP_VERSION-py3-none-any.whl .
RUN apt-get update \
&& apt-get install gcc -y \
&& apt-get clean
RUN pip install --upgrade pip
RUN pip install google_pso_data_validator-$APP_VERSION-py3-none-any.whl
ENTRYPOINT ["python","-m","data_validation"]
