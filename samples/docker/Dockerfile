FROM google/cloud-sdk:latest AS gcloud-builder

FROM python:3.10.0-slim-buster

COPY --from=gcloud-builder /usr/lib/google-cloud-sdk /usr/lib/google-cloud-sdk

ENV PATH "$PATH:/usr/lib/google-cloud-sdk/bin"

COPY start.sh /start.sh
COPY cloud_sql_proxy /cloud_sql_proxy

RUN chmod +x /cloud_sql_proxy

RUN apt-get update \
    && apt-get install -y unixodbc-dev gcc g++ wget vim jq --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install google-pso-data-validator pyodbc

RUN echo 'if [ -f ~/.bash_aliases ]; then . ~/.bash_aliases; fi' >> /root/.bashrc \
    && echo 'PS1="\[\e[34m\]\u@\h:\w\$\[\e[m\] "' >> /root/.bashrc \
    && echo "Welcome to the Data Valdation Tool" >> /etc/motd

RUN chmod +x /start.sh

ENTRYPOINT ["/start.sh"]