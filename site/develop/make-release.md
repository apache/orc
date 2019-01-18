---
layout: page
title: How To Release ORC
---

## Preparing for release

Set version so that it isn't a SNAPSHOT.

~~~
% git checkout branch-X.Y
% edit CMakeLists.txt
% (mkdir build; cd build; cmake ..)
~~~

Commit the changes back to Apache along with a tag for the release candidate.

~~~
% git commit -s -S -am 'Preparing for release X.Y.Z'
% git remote add apache https://gitbox.apache.org/repos/asf/orc.git
% git push apache branch-X.Y
% git tag release-X.Y.Zrc0
% git push apache release-X.Y.Zrc0
~~~

Generate the source tarball and checksums for the release.

~~~
% wget https://github.com/apache/orc/archive/release-X.Y.Zrc0.tar.gz
% tar xzf release-X.Y.Zrc0.tar.gz
% mv orc-release-X.Y.Zrc0 orc-X.Y.Z
% tar czf orc-X.Y.Zrc0.tar.gz orc-X.Y.Z
% shasum -a 256 orc-X.Y.Zrc0.tar.gz > orc-X.Y.Zrc0.tar.gz.sha256
% gpg --detach-sig --armor orc-X.Y.Zrc0.tar.gz
~~~

Copy the artifacts into your personal Apache website.

~~~
% sftp <apacheid>@home.apache.org
sftp> cd public_html
sftp> mkdir orc-X.Y.Zrc0
sftp> cd orc-X.Y.Zrc0
sftp> put orc-X.Y.Zrc0*
sftp> quit
~~~

Make sure your GPG key is present in [Apache
LDAP](https://id.apache.org) and the ORC [svn dist
area](https://dist.apache.org/repos/dist/release/orc/KEYS). That will
be necessary for others to verify the signatures on the release
candidate.

Click the version to release (X.Y.Z) [here](https://issues.apache.org/jira/projects/ORC?selectedItem=com.atlassian.jira.jira-projects-plugin:release-page)
to get the list of jiras that are fixed in X.Y.Z

Send email with the vote:

~~~
To: dev@orc.apache.org
Subject: [VOTE] Should we release ORC X.Y.Zrc0?

All,

Should we release the following artifacts as ORC X.Y.Z?

tar: http://home.apache.org/~omalley/orc-X.Y.Zrc0/
tag: https://github.com/apache/orc/releases/tag/release-X.Y.Zrc0
jiras: https://issues.apache.org/jira/browse/ORC/fixforversion/<fixid>

Thanks!
~~~

## To promote a release candidate (RC) to a real release.

Update the final tag and remove the rc tag.

~~~
% git tag rel/release-X.Y.Z -s
% git push apache rel/release-X.Y.Z
% git push apache :release-X.Y.Zrc0
~~~

Publish the artifacts to Maven central staging. Make sure to have this [setup](http://www.apache.org/dev/publishing-maven-artifacts.html#dev-env) for Apache releases

~~~
% cd java
% mvn -Papache-release clean deploy
~~~

Publish from the staging area:

* login to [Maven staging](https://repository.apache.org/index.html#stagingRepositories)
* find your staging repository (search for org.apache.org)
* close it
* release it

Publish the artifacts to Apache's dist area.

~~~
In a svn clone of https://dist.apache.org/repos/dist/release/orc
% mkdir orc-X.Y.Z
% cd orc-X.Y.Z
copy release artifacts with a rename from orc-X.Y.Zrc0* to orc-X.Y.Z*
% svn add .
% svn commit ---username <apacheid> -m "ORC X.Y.Z Release"

We keep the latest patch release for each of the last two branches, so remove
extra releases (say I.J.K) from the Apache dist area.

% cd ..
% svn rm orc-I.J.K
% svn commit --username <apacheid> -m "Removed old release ORC I.J.K"
~~~

Update the release branch with the version for the next release.

~~~
edit CMakeLists.txt to change version to X.Y.(Z+1)-SNAPSHOT
% cd build
% cmake ..
% git commit -a -s -S -am 'Preparing branch for post X.Y.Z development'
% git push apache branch-X.Y
~~~

Update the site with the new release.

* Check out the master branch (git co apache/master)

~~~
Change directory in to site.
% pwd
<path-to-master-src>
% cd site
% mkdir target
% cd target
Set up site/target to be a separate git workspace that tracks the asf-site branch.
% git init
% git remote add origin https://gitbox.apache.org/repos/asf/orc.git -t asf-site
% git fetch origin
% git checkout asf-site
% cd ..
% bundle install
~~~
* edit site/_data/releases.yml to add new release
   * update the state for the releases to match the changes in the Apache dist
      * latest = new release (only one of these!)
      * stable = other release still in dist
      * archived = removed from dist
* create a new file _posts/YYYY-MM-DD-ORC-X.Y.Z.md for the news section
* Run "bundle exec jekyll serve"
* Check the website on http://0.0.0.0:4000/
* If it looks good, use git add (from within site directory) to add the new files and commit to master with a message of "update site for X.Y.Z".

~~~
% pwd
<path-to-master-src>/site
% git commit -am "Update site for X.Y.Z"
% git push origin master
~~~

* Change directory into site/target for publishing the site.
* Add the new files that you just generated.
   * This assumes you've set up site/target to be a separate git workspace that tracks the asf-site branch.
* Commit to asf-site to publish the updated site.

~~~
% cd target
% pwd
<path-to-master-src>/site/target
% git commit -am "Publish site for X.Y.Z"
% git push origin asf-site
~~~

Update ORC's jira to reflect the released version.

* Select the resolved issues and bulk transition them to closed with following query [here](https://issues.apache.org/jira/issues/?filter=-1).
   * query: project = ORC AND fixVersion = X.Y.Z and status = Resolved ORDER BY created desc
* Mark the version as released and set the date [here](https://issues.apache.org/jira/projects/ORC?selectedItem=com.atlassian.jira.jira-projects-plugin:release-page).

It usually take up to 24 hours for the apache dist mirrors and maven central to update with the new release.
