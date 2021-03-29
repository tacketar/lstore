# -*- cmake -*-

# - Find libsodium libraries and includes
#
# This module defines
#    LIBSODIUM_INCLUDE_DIR - where to find header files
#    LIBSODIUM_LIBRARY - the libraries needed to use ZMQ.
#    LIBSODIUM_FOUND - If false didn't find ZMQ

# Find the include path
find_path(LIBSODIUM_INCLUDE_DIR sodium.h)

find_library(LIBSODIUM_LIBRARY NAMES sodium)

if (LIBSODIUM_LIBRARY AND LIBSODIUM_INCLUDE_DIR)
    SET(LIBSODIUM_FOUND "YES")
endif (LIBSODIUM_LIBRARY AND LIBSODIUM_INCLUDE_DIR)


if (LIBSODIUM_FOUND)
   message(STATUS "Found libsodium: ${LIBSODIUM_LIBRARY} ${LIBSODIUM_INCLUDE_DIR}")
else (LIBSODIUM_FOUND)
   message(STATUS "Could not find libsodium library")
endif (LIBSODIUM_FOUND)


MARK_AS_ADVANCED(
  LIBSODIUM_LIBRARY
  LIBSODIUM_INCLUDE_DIR
  LIBSODIUM_FOUND
)

