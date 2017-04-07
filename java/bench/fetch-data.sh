#!/bin/bash
mkdir -p data/sources/taxi
(cd data/sources/taxi; wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-{11,12}.csv)
(cd data/sources/taxi; gzip *.csv)
mkdir -p data/sources/github
(cd data/sources/github; wget http://data.githubarchive.org/2015-11-{01..15}-{0..23}.json.gz)
