# -*- rpm-spec -*-
%define _basename lstore
# Allow the version to be overridden from the command line
%define _dist_version 1.0.0
%define _dist_release 1
%define _version %{?my_version}%{?!my_version:%{_dist_version}}
%define _release %{?my_release}%{?!my_release:%{_dist_release}}
%define _prefix /usr

URL: http://www.lstore.org
Name: %{_basename}
Version: %{_version}
Release: %{_release}
Summary: LStore - Logistical Storage
License: Apache2
BuildRoot: %{_builddir}/%{_basename}-root
Source: https://github.com/accre/lstore-release/archive/LStore-%{_version}.tar.gz

%description
LStore - Logistical Storage.

%prep
%setup -q -n LStore-%{_version}

%build
CFLAGS="-I%{_prefix}/include $RPM_OPT_FLAGS"
CMAKE_FLAGS="-DLSTORE_VERSION=%{_version}"
CMAKE_FLAGS="$CMAKE_FLAGS -DCMAKE_INSTALL_PREFIX=%{_prefix}"
CMAKE_FLAGS="$CMAKE_FLAGS -DINSTALL_YUM_RELEASE:BOOL=ON "
CMAKE_FLAGS="$CMAKE_FLAGS -DINSTALL_META:BOOL=ON"
CMAKE_FLAGS="$CMAKE_FLAGS -DCMAKE_BUILD_TYPE:BOOL=RelWithDebInfo"
CMAKE_FLAGS="$CMAKE_FLAGS -DENABLE_FUSE3_LIO=on -DBUILD_FUSE3_LIO=on"
cmake $CMAKE_FLAGS .
make  %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make DESTDIR=$RPM_BUILD_ROOT install
cp -a /usr/lib64/libfuse3-lio.so* ${RPM_BUILD_ROOT}/usr/lib64/

%clean
rm -rf $RPM_BUILD_ROOT

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%files
%defattr(-,root,root,-)
%{_bindir}/*
%{_libdir}/*

%changelog
* Tue Mar 30 2021 Alan Tackett <alan.tackett@vanderbilt.edu> 1.0.0
- Initial version with Path based ACLs and local GID->LIO account mappings for FUSE
* Sat Apr 23 2016 Andrew Melo <andrew.m.melo@vanderbilt.edu> 0.5.1-1
- Several bug fixed.

%package devel
Summary: Development files for LStore
Group: Development/System
Requires: lstore
%description devel
Development files for LStore
%files devel
%{_includedir}/*

%package meta
Summary: Default LStore configuration
Group: Development/System
Requires: lstore
%description meta
Default LStore configuration
%files meta
%config(noreplace) /etc/lio
%config(noreplace) /etc/logrotate.d/lstore

%package release
Version: 1.0.0
Summary: LStore repository meta-package
Group: Development/System
%description release
Installs LStore yum repository
%files release
%config /etc/yum.repos.d/lstore.repo

