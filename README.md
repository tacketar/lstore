LStore Release Tools
==============================================
 
Structure
-----------------------------------------------
* binding        - Bindings to third-party applications
* build          - Location where all sources are built
  * logs         - Build logs
  * package      - Storage with built RPMs
  * repo         - Default YUM/APT repositories
  * local        - Installation path for packages built with build-*.sh
  * ccache       - Default ccache location for docker builds
* cmake          - Additional CMake modules
* debian         - Configurations for dpkg-buildpackage
* doc            - Documentation source
* scripts        - Build scripts
  * docker       - Cached Dockerfiles
    * builder    - Bare images with only LStore dependencies and build tools
                   installed
    * buildslave - Larger image with a number of developer tools 
* src            - Source repositories
* test           - Test, benchmark, fuzz harness and cases
* vendor         - External dependencies

Dependencies
----------------------------------------------
You will need to bring your own copies of:

* apr-devel
* apr-util-devel
* fuse3-devel
* leveldb-devel
* openssl-devel
* zlib-devel
* zeromq3-devel
* rocksdb-devel
* libsodium-devel

In addition, LStore has build-time dependencies on

* C, C++ compiler
* cmake

For centos, at least, these dependencies can be installed with:

```
yum groupinstall "Development Tools"
yum install cmake openssl-devel libsodium-devel zeromq3-devel zlib-devel fuse3-devel rocksdb-devel apr-devel apr-util-devel
```

Although RocksDB is only available via Fedora on RedHat/CentOS and will need to be built
from scratch for other RedHat/CentOS distributions. RocksDB is available for Ubuntu.
RocksDB for RedHat/CentOS RocksDB is automatically built when creating RPMs which requires building
a few additional packages from scratch. Care needs to be taken to only use the static libraries
for those tools in order to keep the software portable and not require the propagation of
RocksDB and the other manually added source packages.  Take a look at
./scripts/generate-docker-base.sh for how this is done for packaging. Specifically
look at the logic around ROCKSDB_MANUAL variable for guidance.

If the local CMake installation is too old, we install a local copy into build/

Building
----------------------------------------------
LStore uses CMake as its meta build system. To initialize the build system,
execute:
```
cd build
cmake ..
```

Once the Makefile is initialized, commonly used targets include:
* `make externals` - build any neccessary external packages
* `make all`       - build LStore libraries and binaries
* `make docs`      - build LStore documentation

Packaging LStore
----------------------------------------------
LStore uses a docker-based system for packaging LStore for various linux
distributions. In general, the packaging scripts all accept a list of
distributions on the command line. By default, each distribution will be
attempted. These base images containing external dependencies and build tools
can be bootstrapped with:

>    ./scripts/build-docker-base.sh [distribution] [distribution] ...

For each supported distribution, a docker image named `lstore/builder:DIST`
will pe produced and tagged. For instance, a base Centos 7 image will be named
`lstore/builder:centos-7`. These images can be updated by executing
`build-docker-base.sh` again.

Once the base images are installed, the current source tree can be packaged
with:

>    ./scripts/package.sh [distribution] [distribution] ...

Once `package.sh` completes, the output binaries for each distribution will be
stored in `package/<distribution>/<package>/<revision>`. The revisions are
auto-generated by a heuristic that considers the number of git commits between
the working copy and the most recent release tag.

