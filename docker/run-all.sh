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

REPOSITORY="https://github.com/apache/orc.git"
BRANCH="master"

start=`date`
for os in centos5 centos6 centos7 debian6 debian7 ubuntu12 ubuntu14; do
  echo "Testing $os"
  ( cd $os && docker build -t "orc-$os" . )
  docker run "orc-$os" /bin/bash -c "git clone $REPOSITORY -b $BRANCH && mkdir orc/build && cd orc/build && cmake .. && make package test-out" || exit 1
done
echo "Start: $start"
echo "End:" `date`
