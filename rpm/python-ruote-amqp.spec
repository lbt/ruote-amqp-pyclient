%define python python%{?__python_ver}
%define __python /usr/bin/%{python}
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}
%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib(1))")}


Summary: Python Ruote/AMQP client
Name: python-ruote-amqp
Version: 2.1.1
Release: 1
Source0: %{name}_%{version}.orig.tar.gz
License: UNKNOWN
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildRequires: python,python-setuptools
BuildArch: noarch
Vendor: David Greaves <david@dgreaves.com>
Url: http://github.com/lbt/ruote-amqp-pyclient

%description
UNKNOWN

%prep
%setup -n %{name}-%{version}

%build
python setup.py build

%install
python setup.py install --prefix=/usr --single-version-externally-managed --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)
%{python_sitelib}/RuoteAMQP
