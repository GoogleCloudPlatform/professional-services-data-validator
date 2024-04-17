FROM python:3.9.0-slim
RUN apt-get update \
&& apt-get install gcc -y \
&& apt-get clean
RUN pip install --upgrade pip
RUN pip install google_pso_data_validator
ENTRYPOINT ["python","-m","data_validation"]
