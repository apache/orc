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

# ORC compile for Ubuntu 18
#

FROM ubuntu:18.04
MAINTAINER Owen O'Malley <owen@hortonworks.com>

RUN ln -fs /usr/share/zoneinfo/America/Los_Angeles /etc/localtime
RUN apt-get update
RUN apt-get install -y \
  cmake \
  gcc \
  g++ \
  git \
  libsasl2-dev \
  libssl-dev \
  make \
  maven \
  openjdk-8-jdk \
  tzdata
RUN update-java-alternatives --set java-1.8.0-openjdk-amd64

WORKDIR /root
VOLUME /root/.m2/repository

CMD git clone https://github.com/apache/orc.git -b master && \
  mkdir orc/build && \
  cd orc/build && \
  cmake .. && \
  make package test-out
