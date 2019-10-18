FROM dagster/dagster-py37:2019-10-05

RUN apt-get update
RUN apt-get -y install cron

COPY ./entrypoint.sh /

WORKDIR ./app

EXPOSE 3000

ENTRYPOINT ./entrypoint.sh
