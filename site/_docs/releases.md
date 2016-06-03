---
layout: docs
title: Releases
permalink: /docs/releases.html
---

## Current Release - 1.1.0:

ORC 1.1.0 contains both the Java reader and writer and the C++ reader. It also
contains tools for working with ORC files and looking at their contents and
metadata.

* Released: 10 Jun 2016
* Source code: [orc-1.1.0.tgz]({{site.dist_mirror}}/orc-1.1.0/orc-1.1.0.tgz)
* [GPG Signature]({{site.dist}}/orc-1.1.0/orc-1.1.0.tgz.asc)
  signed by [Owen O'Malley (3D0C92B9)]({{site.dist}}/KEYS)
* Git tag: [fc571e77]({{site.tag_url}}/release-1.1.0)
* SHA 256: [8beea2be]({{site.dist}}/orc-1.1.0/orc-1.1.0.tgz.sha256)

Known issues:

* [ORC-40]({{site.jira}}/ORC-40) Predicate push down is not implemented in C++.

## Checking signatures

All GPG signatures should be verified as matching one of the keys in ORC's
committers' [key list]({{ site.dist }}/KEYS).

~~~ shell
% shasum -a 256 orc-1.1.0.tgz | diff - orc-1.1.0.tgz.sha256
% gpg --import KEYS
% gpg --verify orc-1.1.0.tgz.asc
~~~

## Previous releases:

| Version | Date        | Release   |
| :-----: | :---------: | :-------: |
| 1.0.0   | 25 Jan 2016 | [ORC-1.0.0]({{site.url}}/news/2016/01/25/ORC-1.0.0/)|
