---
layout: docs
title: Building ORC
permalink: /docs/building.html
dockerUrl: https://github.com/apache/orc/blob/master/docker
---

## Building both C++ and Java

The C++ library is supported on the following operating systems:

* CentOS 6 or 7
* Debian 7 or 8
* MacOS 10.10 or 10.11
* Ubuntu 12.04, 14.04, or 16.04

You'll want to install the usual set of developer tools, but at least:

* cmake
* g++ or clang++
* java ( >= 1.7)
* make
* maven ( >= 3)

For each version of Linux, please check the corresponding Dockerfile, which
is in the docker subdirectory, for the list of packages required to build ORC:

* [CentOS 6]({{ page.dockerUrl }}/centos6/Dockerfile)
* [CentOS 7]({{ page.dockerUrl }}/centos7/Dockerfile)
* [Debian 7]({{ page.dockerUrl }}/debian7/Dockerfile)
* [Debian 8]({{ page.dockerUrl }}/debian8/Dockerfile)
* [Ubuntu 12]({{ page.dockerUrl }}/ubuntu12/Dockerfile)
* [Ubuntu 14]({{ page.dockerUrl }}/ubuntu14/Dockerfile)
* [Ubuntu 16]({{ page.dockerUrl }}/ubuntu16/Dockerfile)

To build a normal release:

~~~ shell
% mkdir build
% cd build
% cmake ..
% make package test-out
~~~

ORC's C++ build supports three build types, which are controlled by adding
`-DCMAKE_BUILD_TYPE=<type>` to the cmake command.

* **RELWITHDEBINFO** (default) - Optimized with debug information
* **DEBUG** - Unoptimized with debug information
* **RELEASE** - Optimized with no debug information

If your make command fails, it is useful to see the actual commands that make
is invoking:

~~~ shell
% make package test-out VERBOSE=1
~~~

## Building just Java

You'll need to install:

* java (>= 1.7)
* maven (>= 3)

To build:

~~~ shell
% cd java
% mvn package
~~~

