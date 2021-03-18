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

CLONE="git clone $URL -b $BRANCH"
MAKEDIR="mkdir orc/build && cd orc/build"
VOLUME="--volume m2cache:/root/.m2/repository"
mkdir -p logs

function failure {
    echo "Failed tests"
    grep -h "FAILED " logs/*-test.log
    exit 1
}
rm -f logs/pids.txt logs/*.log

start=`date`
for jdk in 8 11; do
    for os in `cat os-list.txt`; do
        if [[ "$os" = "debian10" && "$jdk" = "8" ]] || [[ "$os" = "debian9" && "$jdk" = "11" ]] || [[ "$os" = "ubuntu16" && "$jdk" = "11" ]]; then
            echo "Skip building $os with $jdk"
            continue
        fi
        echo "Building $os for $jdk"
        ( cd $os && docker build -t "orc-$os-jdk${jdk}" --build-arg jdk=$jdk . ) > logs/${os}-jdk${jdk}-build.log 2>&1 || exit 1
    done
done
testStart=`date`

for os in `cat os-list.txt`; do
    ./run-one.sh $1 $2 $os > logs/$os-test.log 2>&1 &
    echo "$!" >> logs/pids.txt
    echo "Launching $os as $!"
done

for job in `cat logs/pids.txt`; do
    echo "Waiting for $job"
    wait $job || failure
done

echo ""
echo "Build start: $start"
echo "Test start: $testStart"
echo "End:" `date`
