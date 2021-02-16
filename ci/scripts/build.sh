#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/gremgo-neptune
  make build
popd