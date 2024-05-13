FROM python:3.9.0-slim

RUN apt-get update \
    && apt-get install -y git gcc \
    && apt-get clean
RUN pip install --upgrade pip
RUN git clone https://github.com/GoogleCloudPlatform/professional-services-data-validator.git
WORKDIR professional-services-data-validator
RUN pip install .
ENTRYPOINT ["python","-m","data_validation"]
