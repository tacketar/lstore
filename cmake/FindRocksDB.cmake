# -*- cmake -*-

# - Find RocksDB libraries and C includes
#
# This module defines
#    ROCKSDB_INCLUDE_DIR - where to find the header files
#    ROCKSDB_LIBRARY - the libraries needed.
#    ROCKSDB_FOUND - If false didn't find RocksDB

# Find the include path
find_path(ROCKSDB_INCLUDE_DIR rocksdb/c.h)

find_library(ROCKSDB_LIBRARY NAMES rocksdb)

if (ROCKSDB_LIBRARY AND ROCKSDB_INCLUDE_DIR)
    SET(ROCKSDB_FOUND "YES")
endif ()


if (ROCKSDB_FOUND)
   message(STATUS "Found ROCKSDB: ${ROCKSDB_LIBRARY} ${ROCKSDB_INCLUDE_DIR}")
else ()
   message(STATUS "Could not find ROCKSDB library LIB=${ROCKSDB_LIBRARY} INC=${ROCKSDB_INCLUDE_DIR}")
endif ()


MARK_AS_ADVANCED(
  ROCKSDB_LIBRARY
  ROCKSDB_INCLUDE_DIR
  ROCKSDB_FOUND
)

