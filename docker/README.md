# Docker Test

## Supported OSes

* Debian 13
* Ubuntu 24 and 26
* Oracle Linux 9 and 10
* Amazon Linux 2023
* UBI 10

## Pre-built Images

Apache ORC community provides a set of pre-built docker images and uses it during testing.

    docker pull apache/orc-dev:ubuntu24

You can find all tags here.

    https://hub.docker.com/r/apache/orc-dev/tags

## Test

To test against all of the Linux OSes against Apache's main branch:

    cd docker
    ./run-all.sh apache main

Using `local` as the owner will cause the scripts to use the local repository.

The scripts are:

* `run-all.sh` *owner* *branch* - test the given owner's branch on all OSes
* `run-one.sh` *owner* *branch* *os* - test the owner's branch on one OS
* `reinit.sh` - rebuild all of the base images without the image cache

Each OS image uses a single JDK version: JDK 17 (oraclelinux9, amazonlinux23),
JDK 21 (debian13, ubuntu24, oraclelinux10) and JDK 25 (ubuntu26, ubi10).

A base image for each OS is built using:

    cd docker/$os
    docker build -t "apache/orc-dev:$os" .

For debian13, ubuntu24 and ubuntu26, the JDK version can be changed via
`--build-arg jdk=N`.

## Clean up

    docker container prune
    docker image prune
