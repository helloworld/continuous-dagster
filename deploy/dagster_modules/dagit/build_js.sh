#! /bin/bash
ROOT=$(git rev-parse --show-toplevel)
pushd $ROOT/js_modules/dagit
set -eux

yarn install --offline
yarn build-for-python