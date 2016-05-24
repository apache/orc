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

# ORC compile for CentOS 6
#

FROM centos:6
MAINTAINER Owen O'Malley <owen@hortonworks.com>

RUN yum check-update || true
RUN yum install -y \
  cmake \
  curl-devel \
  expat-devel \
  gcc \
  gcc-c++ \
  gettext-devel \
  git \
  java-1.7.0-openjdk \
  java-1.7.0-openjdk-devel \
  make \
  openssl-devel \
  tar \
  wget \
  which \
  zlib-devel

WORKDIR /root
RUN wget http://apache.mesi.com.ar/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz -O maven.tgz
RUN tar xzf maven.tgz
RUN ln -s /root/apache-maven-3.3.9/bin/mvn /usr/bin/mvn

CMD git clone https://github.com/apache/orc.git -b master && \
  mkdir orc/build && \
  cd orc/build && \
  cmake .. && \ 
  make package test-out
