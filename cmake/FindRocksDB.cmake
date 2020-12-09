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
    get_filename_component(ext ${ROCKSDB_LIBRARY} EXT)
    message(STATUS "lib=${ROCKSDB_LIBRARY} lext=${ext}")
    if (ext STREQUAL ".a")
        find_library(bz2_lib NAMES bz2)
        message(STATUS "Static version of RocksDB so checking for bzip2: ${bz2_lib}")
        find_library(lz4_lib NAMES lz4)
        message(STATUS "Static version of RocksDB so checking for lz4: ${lz4_lib}")
        find_library(zstd_lib NAMES zstd)
        message(STATUS "Static version of RocksDB so checking for zstd: ${zstd_lib}")
        find_library(snappy_lib NAMES snappy)
        message(STATUS "Static version of RocksDB so checking for snappy: ${snappy_lib}")

        if (bz2_lib AND lz4_lib AND zstd_lib AND snappy_lib)
            set(ROCKSDB_EXTRA_LIBRARY ${snappy_lib} ${zstd_lib} ${lz4_lib} ${bz2_lib})
        endif()

        if (ROCKSDB_EXTRA_LIBRARY)
           message(STATUS "Found extra RocksDB libs: ${ROCKSDB_EXTRA_LIBRARY}")
        else ()
           message(STATUS "Could not find bzip2 library which ROCKSDB requires LIB=${ROCKSDB_LIBRARY} INC=${ROCKSDB_INCLUDE_DIR}")
           unset(ROCKSDB_FOUND)
        endif ()
    endif()
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
  ROCKSDB_EXTRA_LIBRARY
)

