FROM python:3.7-slim-buster

RUN apt-get update
RUN apt-get -y install cron
RUN apt-get -y install vim

ENV DAGSTER_HOME /var/lib/data/dagster_home
RUN mkdir -p '/var/lib/data/dagster_home'

WORKDIR /app

COPY ./entrypoint.sh /
COPY ./app /app
COPY ./deploy /deploy

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN pip install -e /deploy/dagster_modules/dagster/ \
    -e /deploy/dagster_modules/dagster-graphql/ \
    -e /deploy/dagster_modules/dagit/ \
    -e /deploy/dagster_modules/libraries/dagster-cron/;

EXPOSE 3000

RUN chmod +x /entrypoint.sh
ENTRYPOINT /entrypoint.sh