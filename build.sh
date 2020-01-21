#! /bin/bash
ROOT=$(git rev-parse --show-toplevel)

set -eux

pushd $ROOT

rm -rf $ROOT/deploy/dagster_modules
mkdir -p deploy/dagster_modules
cp -r $DAGSTER_REPO/python_modules/* deploy/dagster_modules/
