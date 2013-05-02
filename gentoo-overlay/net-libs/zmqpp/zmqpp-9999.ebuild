# Copyright 1999-2013 Gentoo Foundation
# Distributed under the terms of the GNU General Public License v2
# $Header: $

EAPI=5

inherit git-2

DESCRIPTION="High-level C++ bindings for ZeroMQ"
HOMEPAGE="https://github.com/benjamg/zmqpp"
EGIT_REPO_URI="https://github.com/benjamg/zmqpp.git"
EGIT_BRANCH="develop"

LICENSE="MIT"
SLOT="0"
KEYWORDS="~amd64"
IUSE="client doc static-libs test"

RDEPEND="net-libs/zeromq
	client? ( dev-libs/boost )"
DEPEND="${RDEPEND}
	doc? ( app-doc/doxygen )
	test? ( dev-libs/boost )"

LIB_VERSION="2"

src_configure() {
	sed -i \
		-e '/^CPPFLAGS/d' \
		-e '/^CXXFLAGS/d' \
		-e '/^LDFLAGS/d' \
		-e '/^CXX/d' \
		-e 's|^CONFIG.*|CONFIG = gentoo|' \
		-e 's|^PREFIX = .*|PREFIX = /usr|' \
		Makefile || die "sed failed"
}

src_compile() {
	emake main
	use client && emake client
	if use doc ; then
		doxygen zmqpp.doxygen.conf || die "building documentation failed"
	fi		
}

src_install() {
	# work around primitive build system
	insinto /usr/include/zmqpp
	doins src/zmqpp/*.hpp
	newlib.so build/gentoo-*/*.so.* libzmqpp.so.${LIB_VERSION}
	use static-libs && dolib.a build/gentoo-*/*.a
	use client && dobin build/gentoo-*/zmqpp
	use doc && dohtml -r docs/html/*
}
