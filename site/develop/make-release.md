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
% tar czf orc-X.Y.Z.tar.gz orc-X.Y.Z
% mkdir orc-X.Y.Z-rc0
% mv orc-X.Y.Z.tar.gz orc-X.Y.Z-rc0
% cd orc-X.Y.Z-rc0
% shasum -a 256 orc-X.Y.Z.tar.gz > orc-X.Y.Z.tar.gz.sha256
% gpg --detach-sig --armor orc-X.Y.Z.tar.gz
~~~

Verify the artifacts

~~~
% shasum -a256 orc-X.Y.Z.tar.gz | diff - orc-X.Y.Z.tar.gz.sha256
% gpg --verify orc-X.Y.Z.tar.gz.asc
% cd ..
~~~

Upload the artifacts into Apache dev distribution website.

~~~
% svn co --depth=files "https://dist.apache.org/repos/dist/dev/orc" svn-orc
% mv orc-X.Y.Z-rc0 svn-orc
% cd svn-orc
% svn add orc-X.Y.Z-rc0
% svn commit -m "Upload Apache ORC X.Y.Z RC0"
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
Subject: [VOTE] Release Apache ORC X.Y.Z (RC0)

Please vote on releasing the following candidate as Apache ORC version X.Y.Z.

[ ] +1 Release this package as Apache ORC X.Y.Z
[ ] -1 Do not release this package because ...

TAG:
https://github.com/apache/orc/releases/tag/release-X.Y.Zrc0

RELEASE FILES:
https://dist.apache.org/repos/dist/dev/orc/orc-X.Y.Z-rc0

LIST OF ISSUES:
https://issues.apache.org/jira/projects/ORC/versions/<fixid>

This vote will be open for 72 hours.

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
% ./mvnw -Papache-release clean deploy
~~~

Publish from the staging area:

* login to [Maven staging](https://repository.apache.org/index.html#stagingRepositories)
* find your staging repository (search for org.apache.orc)
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

* Check out the main branch (git checkout apache/main)

~~~
Change directory in to site.
% pwd
<path-to-main-src>
% cd site
Set up site/target to be a separate git workspace that tracks the asf-site branch.
% git clone git@github.com:apache/orc.git -b asf-site target
~~~
* edit site/_data/releases.yml to add new release
   * update the state for the releases to match the changes in the Apache dist
      * latest = new release (only one of these!)
      * stable = other release still in dist
      * archived = removed from dist
* create a new file _posts/YYYY-MM-DD-ORC-X.Y.Z.md for the news section
* Run `docker build -t orc-site .`
* Run `CONTAINER=$(docker run -d -p 4000:4000 orc-site)`
* Check the website on [http://0.0.0.0:4000/](http://0.0.0.0:4000/)
* If it looks good, copy the results out of docker:
   * Run `docker cp $CONTAINER:/home/orc/site/target .`
   * Run `docker stop $CONTAINER`

~~~
% git commit -am "Update site for X.Y.Z"
% git push origin main
~~~

* Change directory into site/target for publishing the site.
* Add the new files that you just generated.
   * This assumes you've set up site/target to be a separate git workspace that tracks the asf-site branch.
* Commit to asf-site to publish the updated site.
~~~
% cd target
% git commit -am "Publish site for X.Y.Z"
% git push origin asf-site
~~~

Update ORC's jira to reflect the released version.

* Select the resolved issues and bulk transition them to closed with following query [here](https://issues.apache.org/jira/issues/?filter=-1).
   * query: project = ORC AND fixVersion = X.Y.Z and status = Resolved ORDER BY created desc
* Mark the version as released and set the date [here](https://issues.apache.org/jira/projects/ORC?selectedItem=com.atlassian.jira.jira-projects-plugin:release-page).

It usually take up to 24 hours for the apache dist mirrors and maven central to update with the new release.

## To release ORC in vcpkg.
We could release the latest ORC version in [vcpkg](https://vcpkg.io/en/packages).

1.Download the source code of vcpkg:
~~~
% git clone git@github.com:microsoft/vcpkg.git
~~~

2.Compile vcpkg

Configure your vcpkg based on operating system versions. See [Getting Started](https://github.com/microsoft/vcpkg?tab=readme-ov-file#getting-started)

3.Update ORC version

Update the **version** field of [vcpkg.json](https://github.com/microsoft/vcpkg/blob/master/ports/orc/vcpkg.json). Run the following command to rebuild ORC:
```
% ./vcpkg build  orc
```

You will receive an error, which contains a SHA512 value, updating it to the [portfile.cmake](https://github.com/microsoft/vcpkg/blob/master/ports/orc/portfile.cmake) file.

> Expected hash: 141afbd6d83b8c8032df071838e7da61eedb3d22289642f76669b6efd167d7550b200bd3542f012d0b63c9ae2572d83fcb1b7f76537b6fa6f980aebf37e2cde2

> Actual hash (you should update): 7b9d6f9613f48b41843618465c0c71ed70e5695b4fc4b3db15d0bbfe4c5126e0c6c8f1114d6c450a56b4ddf0357d89bd40cb03c0964275c59761cc0f8fec1291

4.Commit current changes
```
% git commit -m ""
```

Run ./vcpkg x-add-version --all to auto generate version item in version database.
```
% ./vcpkg x-add-version --all
```

Test changes with the following command:
```
% ./vcpkg build orc
```

If anything is ok, re-commit changes and send a PR to [vcpkg repo](https://github.com/microsoft/vcpkg).

This is an [example](https://github.com/microsoft/vcpkg/pull/36098/files) of updating to ORC 1.9.2

> Note: You will not immediately find the merged PR version on the official website of vcpkg. check [vcpkg release](https://github.com/microsoft/vcpkg/releases) to know the latest release progress.

## To release ORC in Conan.

We could release the latest ORC version in [conan](https://conan.io/center/recipes/orc).

1.Install conan

Install your conan with the help of the official guide: [Install](https://docs.conan.io/2/installation.html)

2.Download the source code of conan-center-index:
~~~
% git clone git@github.com:conan-io/conan-center-index.git
~~~

3.Update ORC version

Add the new version to [config.yml](https://github.com/conan-io/conan-center-index/blob/master/recipes/orc/config.yml) and [conandata.yml](https://github.com/conan-io/conan-center-index/blob/master/recipes/orc/all/conandata.yml) locally in your conan-center-index. Run the following command in the conan-center-index/recipes/orc/all directory to build ORC of version X.Y.Z:
```
% conan create . --version=X.Y.Z
```

4.Commit current changes

If everything is ok, commit the local change and open a pull request to [conan-center-index repo](https://github.com/conan-io/conan-center-index).

This is an [example](https://github.com/conan-io/conan-center-index/pull/23046) of adding ORC 2.0.0 to it.
