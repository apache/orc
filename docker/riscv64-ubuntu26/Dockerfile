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

# ORC compile for RISC-V Ubuntu 26
#

FROM --platform=linux/riscv64 ubuntu:26.04
LABEL org.opencontainers.image.authors="Apache ORC project <dev@orc.apache.org>"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.ref.name="Apache ORC on RISC-V Ubuntu 26"
LABEL org.opencontainers.image.version=""
ARG jdk=21
ARG cc=gcc

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      cmake \
      git \
      libsasl2-dev \
      libssl-dev \
      make \
      curl \
      maven \
      openjdk-${jdk}-jdk-headless \
      tzdata \
      $( [ "${cc}" = "gcc" ] && echo "gcc g++" || echo "clang" ) && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    if [ "${cc}" != "gcc" ] ; then \
      update-alternatives --set cc /usr/bin/clang && \
      update-alternatives --set c++ /usr/bin/clang++; \
    fi
RUN update-alternatives --set java \
      $(update-alternatives --list java | grep "java-${jdk}-openjdk") && \
    update-alternatives --set javac \
      $(update-alternatives --list javac | grep "java-${jdk}-openjdk")

ENV CC=cc
ENV CXX=c++

RUN ln -fs /usr/share/zoneinfo/America/Los_Angeles /etc/localtime && \
    mkdir -p /usr/share/zoneinfo/US && \
    ln -fs /usr/share/zoneinfo/America/Los_Angeles /usr/share/zoneinfo/US/Pacific

WORKDIR /root
VOLUME /root/.m2/repository

CMD ["/bin/bash", "-c", "if [ ! -d orc ]; then echo \"No volume provided, building from apache main.\"; echo \"Pass '-v\`pwd\`:/root/orc' to docker run to build local source.\"; git clone https://github.com/apache/orc.git -b main; fi && mkdir build && cd build && cmake ../orc && make package test-out"]
