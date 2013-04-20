
# - Try to find RPCZ
# Once done this will define
#  RPCZ_FOUND - System has LibXml2
#  RPCZ_INCLUDE_DIRS - The LibXml2 include directories
#  RPCZ_LIBRARIES - The libraries needed to use LibXml2
#  RPCZ_DEFINITIONS - Compiler switches required for using LibXml2

find_package(PkgConfig)
pkg_check_modules(PC_RPCZ QUIET librpcz)
set(RPCZ_DEFINITIONS ${PC_RPCZ_CFLAGS_OTHER})

find_path(RPCZ_INCLUDE_DIR rpcz/rpcz.hpp
          HINTS ${PC_RPCZ_INCLUDEDIR} ${PC_RPCZ_INCLUDE_DIRS} )

find_library(RPCZ_LIBRARY NAMES rpcz librpcz
             HINTS ${PC_RPCZ_LIBDIR} ${PC_RPCZ_LIBRARY_DIRS} )

set(RPCZ_LIBRARIES ${RPCZ_LIBRARY} )
set(RPCZ_INCLUDE_DIRS ${RPCZ_INCLUDE_DIR} )

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set RPCZ_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(RPCZ  DEFAULT_MSG
                                  RPCZ_LIBRARY RPCZ_INCLUDE_DIR)

mark_as_advanced(RPCZ_INCLUDE_DIR RPCZ_LIBRARY )

