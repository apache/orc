---
layout: docs
title: Releases
permalink: /docs/releases.html
---
{% for relItr in site.data.releases %}
  {% if relItr[1]["state"] == "latest" %}
     {% assign releaseName = relItr[0] %}
     {% break %}
  {% endif %}
{% endfor %}

## Current Release - {{ releaseName }}:

ORC {{ releaseName }} contains both the Java reader and writer and the C++
reader for ORC files. It also contains tools for working with ORC
files and looking at their contents and metadata.

{% include release_description.md %}
{% include orc_1.3.md %}
{% include known_issues.md %}

## Checking signatures

All GPG signatures should be verified as matching one of the keys in ORC's
committers' [key list]({{ site.dist }}/KEYS).

~~~ shell
% shasum -a 256 orc-X.Y.Z.tgz | diff - orc-X.Y.Z.tgz.sha256
% gpg --import KEYS
% gpg --verify orc-X.Y.Z.tgz.asc
~~~

## All releases:

{% include release_table.html %}
