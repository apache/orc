#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GITHUB_USER=$1
URL=https://github.com/$GITHUB_USER/orc.git
BRANCH=$2
OS=$3

function failure {
    echo "FAILED $OS"
    exit 1
}

CLONE="git clone $URL -b $BRANCH"
MAKEDIR="mkdir orc/build && cd orc/build"
VOLUME="--volume m2cache:/root/.m2/repository"

echo "Started $GITHUB_USER/$BRANCH on $OS at $(date)"

case $OS in
centos6|ubuntu12)
   OPTS="-DSNAPPY_HOME=/usr/local -DPROTOBUF_HOME=/usr/local"
   ;;
centos7|debian8|ubuntu14)
   OPTS="-DSNAPPY_HOME=/usr/local"
   ;;
*)
   OPTS=""
   ;;
esac
docker run $VOLUME "orc-$OS" /bin/bash -c \
	 "$CLONE && $MAKEDIR && cmake $OPTS .. && make package test-out" \
         || failure
echo "Finished $OS at $(date)"
