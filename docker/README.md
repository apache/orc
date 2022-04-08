## Supported OSes

* CentOS 7
* Debian 9 and 10
* Ubuntu 16, 18, and 20

## Test

To test against all of the Linux OSes against Apache's master branch:

    cd docker
    ./run-all.sh apache master

Using `local` as the owner will cause the scripts to use the local repository.

The scripts are:
* `run-all.sh` *owner* *branch* - test the given owner's branch on all OSes
* `run-one.sh` *owner* *branch* *os* - test the owner's branch on one OS
* `reinit.sh` - rebuild all of the base images without the image cache

A base image for each OS is built using:

    cd docker/$os
    docker build -t orc-$os .

## Clean up

    docker container prune
    docker image prune
