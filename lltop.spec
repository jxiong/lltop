Name:           lltop
Version:        1.0.0
Release:        1%{?dist}
Summary:        Lustre filesystem monitoring utilities

License:        MIT
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  make
BuildRequires:  python3
BuildRequires:  python3-wheel

%define debug_package %{nil}
%define _build_id_links none

%description
Lustre filesystem monitoring utilities:
* lltop: Gathers real-time I/O statistics from Lustre servers and displays them aggregated by client IP in a top-like interface.
* llostio: Tracks and displays OST metrics including throughput, IOPS, latency, and request sizes.

%prep
%setup -q

%build
# Ensure PyInstaller is installed via pip since it's not available in standard repos
python3 -m pip install --user pyinstaller
export PATH=$HOME/.local/bin:$PATH

# The Makefile uses PyInstaller to build standalone binaries
make

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT%{_bindir}
install -m 755 lltop $RPM_BUILD_ROOT%{_bindir}/lltop
install -m 755 llostio $RPM_BUILD_ROOT%{_bindir}/llostio

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%{_bindir}/lltop
%{_bindir}/llostio

%changelog
* Sat Apr 18 2026 Jinshan Xiong <jinshanx@google.com> - 1.0.0-1
- Initial RPM package for lltop and llostio
