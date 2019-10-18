FROM dagster/dagster-py37:2019-10-05

RUN apt-get update
RUN apt-get -y install cron

COPY ./entrypoint.sh /
COPY ./app /app

WORKDIR /app

EXPOSE 3000

RUN chmod +x /entrypoint.sh
ENTRYPOINT /entrypoint.sh
