# Copyright 1999-2013 Gentoo Foundation
# Distributed under the terms of the GNU General Public License v2
# $Header: $

EAPI=5

DISTUTILS_OPTIONAL=1
PYTHON_COMPAT=( python{2_6,2_7} )

inherit eutils git-2 cmake-utils distutils-r1

DESCRIPTION="ZeroMQ protobuf RPC interface"
HOMEPAGE="https://code.google.com/p/rpcz/"
EGIT_REPO_URI="https://code.google.com/p/rpcz/"
EGIT_BRANCH="master"

LICENSE="Apache-2.0"
SLOT="0"
KEYWORDS="~amd64"
IUSE="+python static-libs test"

RDEPEND=">=net-libs/zeromq-3
	dev-libs/boost
	dev-libs/protobuf
    python? ( ${PYTHON_DEPS} )"
DEPEND="${RDEPEND}"

src_unpack() {
	git-2_src_unpack
}

src_prepare() {
	cmake-utils_src_prepare
	epatch "${FILESDIR}/${P}-zmqpp.patch"
	epatch "${FILESDIR}/${P}-fix-header-install.patch"
	epatch "${FILESDIR}/${P}-not-install-tests.patch"
	use python && distutils-r1_src_prepare
}

src_configure() {
    local mycmakeargs=(
        $(cmake-utils_use test rpcz_build_tests)
        $(cmake-utils_use static-libs rpcz_build_static)
    )

    cmake-utils_src_configure
	use python && distutils-r1_src_configure
}

python_compile() {
	cd "${S}/python"
	distutils-r1_python_compile "${@}"
}

python_install() {
	cd "${S}/python"
	distutils-r1_python_install "${@}"
}

src_compile() {
	cmake-utils_src_compile
	if use python; then
		sed -i \
			-e "s|../build|${BUILD_DIR}|g" \
			python/setup.py || die "sed failed"
		distutils-r1_src_compile
	fi
}

src_install() {
	cmake-utils_src_install
	insinto /usr/share/cmake/Modules
	doins cmake_modules/FindProtobufPlugin.cmake
	use python && distutils-r1_src_install
}
