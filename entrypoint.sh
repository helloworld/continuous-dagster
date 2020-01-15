#!/bin/sh

touch /etc/crontab /etc/cron.*/*

service cron start

dagster schedule up

for name in `dagster schedule list --name`; do
    echo $name;
    dagster schedule stop $name || true;
    dagster schedule start $name;
done


dagit -h 0.0.0.0 -p 3000