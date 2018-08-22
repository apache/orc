This directory is for building [snaps](https://snapcraft.io/) of the ORC tools.

If you are running on a non-Linux system, you'll need to create a VM. Vagrant
scripts are provided to accomplish that.

```bash
% vagrant up
% vagrant ssh
% git clone https://github.com/apache/orc.git -o apache
```

If you don't use VM, you'll need to install snapcraft, which is the tool to build
snaps.

```bash
% sudo apt-get install snapcraft
```

In both cases, go into the snap directory and build the snap:

```bash
% cd orc/snap
% snapcraft
```

To publish to the snapcraft store, you need to:

```bash
% snapcraft login
% snapcraft push orc_*.snap
```

For users to install the snap, they'll use:

```bash
% sudo snap install orc
```

The commands are:

* orc.contents - C++ ORC tool for displaying data contents
* orc.java - Java ORC tool
* orc.metadata - C++ ORC metadata tool
* orc.statistics - C++ ORC statistics tool