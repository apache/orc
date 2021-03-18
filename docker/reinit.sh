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

start=`date`
for jdk in 8 11; do
  for os in `cat os-list.txt`; do
    if [[ "$os" = "debian10" && "$jdk" = "8" ]] || [[ "$os" = "debian9" && "$jdk" = "11" ]] || [[ "$os" = "ubuntu16" && "$jdk" = "11" ]]; then
      echo "Skip an initialize $os with $jdk"
      continue
    fi
    echo "Re-initialize $os with $jdk"
    ( cd $os && docker build --no-cache -t "orc-${os}-jdk${jdk}" --build-arg jdk=${jdk} . )
  done
done
echo "Start: $start"
echo "End:" `date`
