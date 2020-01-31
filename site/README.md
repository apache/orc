# Apache ORC docs site

This directory contains the code for the Apache ORC web site,
[orc.apache.org](https://orc.apache.org/). The easiest way to build
the site is to use docker to use a standard environment.

## Setup

1. `cd site`
2. `git clone git@github.com:apache/orc.git -b asf-site target`

## Run the docker container with the preview of the site.

1. `docker build -t orc-site .`
2. `docker run -d -p 4000:4000 orc-site`

## Browsing

Look at the site by navigating to
[http://0.0.0.0:4000/](http://0.0.0.0:4000/) .

## Pushing to site

You'll copy the files from the container to the site/target directory and
commit those to the asf-site branch.

1. Find the name of the container using `docker ps`.
2. `docker cp $CONTAINER:/home/orc/site/target .`
3. `cd target`
4. Commit the files and push to Apache.

## Shutting down the docker container

1. `docker stop $CONTAINER`