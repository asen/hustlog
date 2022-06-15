#!/usr/bin/env bash

DOCKER=docker
if which podman 2>&1 >/dev/null ; then
  DOCKER=podman
fi

$DOCKER build -t hustlog .
