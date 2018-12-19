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

# ORC compile for Ubuntu 16 (clang 5)
#

FROM ubuntu:16.04
MAINTAINER Owen O'Malley <owen@hortonworks.com>

RUN apt-get update
RUN apt-get install -y wget software-properties-common
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN apt-add-repository "deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-5.0 main"
RUN apt-get update
RUN apt-get install -y \
  cmake \
  default-jdk \
  clang-5.0 \
  git \
  libsasl2-dev \
  libssl-dev \
  make \
  maven \
  tzdata

ENV CC=clang-5.0
ENV CXX=clang++-5.0

WORKDIR /root
VOLUME /root/.m2/repository

CMD git clone https://github.com/apache/orc.git -b master && \
  mkdir orc/build && \
  cd orc/build && \
  cmake .. && \
  make package test-out
