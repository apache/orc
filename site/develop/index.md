---
layout: page
title: Developing
---

Information about the ORC project that is most important for
developers working on the project. The project has created
[bylaws](bylaws.html) for itself.

## Project Members

{% comment %}
please sort by Apache Id
{% endcomment %}
Name                    | Apache Id    | Role
:---------------------- | :----------- | :---
Aliaksei Sandryhaila    | asandryh     | PMC
Chris Douglas           | cdouglas     | PMC
Chinna Rao Lalam        | chinnaraol   | Committer
Chaoyu Tang             | ctang        | Committer
Carl Steinbach          | cws          | Committer
Daniel Dai              | daijy        | Committer
Eugene Koifman          | ekoifman     | Committer
Alan Gates              | gates        | PMC
Gopal Vijayaraghavan    | gopalv       | PMC
Gunther Hagleitner      | gunther      | Committer
Ashutosh Chauhan        | hashutosh    | Committer
Jesus Camacho Rodriguez | jcamacho     | Committer
Jason Dere              | jdere        | Committer
Jimmy Xiang             | jxiang       | Committer
Kevin Wilfong           | kevinwilfong | Committer
Lars Francke            | larsfrancke  | Committer
Lefty Leverenz          | leftyl       | PMC
Rui Li                  | lirui        | Committer
Mithun Radhakrishnan    | mithun       | Committer
Matthew McCline         | mmccline     | Committer
Naveen Gangam           | ngangam      | Committer
Owen O'Malley           | omalley      | PMC
Prasanth Jayachandran   | prasanthj    | PMC
Pengcheng Xiong         | pxiong       | Committer
Rajesh Balamohan        | rbalamohan   | Committer
Sergey Shelukhin        | sershe       | Committer
Sergio Pena             | spena        | Committer
Siddharth Seth          | sseth        | Committer
Stephen Walkauskas      | swalkaus     | Committer
Vaibhav Gumashta        | vgumashta    | Committer
Wei Zheng               | weiz         | Committer
Xuefu Zhang             | xuefu        | Committer
Ferdinand Xu            | xuf          | Committer
Yongzhi Chen            | ychena       | Committer
Aihua Xu                | zihuaxu      | Committer

Companies with employees that are committers:

* Cloudera
* Facebook
* Hewlett Packard Enterprise
* Hortonworks
* Intel
* LinkedIn
* Microsoft
* Yahoo

## Mailing Lists

There are several development mailing lists for ORC:

* [dev@orc.apache.org](mailto:dev@orc.apache.org) - Development discussions
  with archive [here](https://mail-archives.apache.org/mod_mbox/orc-dev/)
* [issues@orc.apache.org](mailto:issues@orc.apache.org) - Bug tracking
  with archive [here](https://mail-archives.apache.org/mod_mbox/orc-issues/)
* [commits@orc.apache.org](mailto:commits@orc.apache.org) - Git tracking
  with archive [here](https://mail-archives.apache.org/mod_mbox/orc-commits/)

You can subscribe to the lists by sending email to
*list*-subscribe@orc.apache.org and unsubscribe by sending email to
*list*-unsubscribe@orc.apache.org.

## Source code

ORC uses git for version control. Get the source code:

`% git clone https://git-wip-us.apache.org/repos/asf/orc.git`

The important branches are:

* [master](https://github.com/apache/orc/tree/master) -
  The trunk for all developement
* [asf-site](https://github.com/apache/orc/tree/asf-site) -
  The pages that are deployed to https://orc.apache.org/

Please check our [coding guidelines](/develop/coding.html).

## Reviews

All code must be +1'ed by a committer other than the author prior to its 
commit.