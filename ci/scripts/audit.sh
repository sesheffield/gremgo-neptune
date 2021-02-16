#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/gremgo-neptune
  make audit
popd