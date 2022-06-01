cmake_minimum_required(VERSION 2.8)

# Name this project
set(LSTORE_PROJECT_NAME "lio_stdio_wrapper")
project(${LSTORE_PROJECT_NAME} C)

# Include modules
if(NOT USE_SUPERBUILD)
    set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
endif()

include(CheckIncludeFile)
include(CheckSymbolExists)

# Detect compiler flags.
check_include_file(attr/xattr.h HAVE_ATTR_XATTR_H)
check_include_file(sys/xattr.h HAVE_SYS_XATTR_H)
configure_file(${PROJECT_SOURCE_DIR}/config.h.in
               ${PROJECT_SOURCE_DIR}/config.h)

set(CMAKE_C_FLAGS "-std=c11 -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE64_SOURCE -fvisibility=hidden -fno-strict-aliasing -Wall -Wextra -Werror -Wno-unused-parameter -Wno-deprecated-declarations -Wno-error=sign-compare")

# Find dependencies.
if(NOT USE_SUPERBUILD)
    find_package(Jerasure REQUIRED)
    find_package(ToolBox REQUIRED)
    find_package(GOP REQUIRED)
    find_package(IBP REQUIRED)
    find_package(LIO REQUIRED)
    find_package(ZMQ REQUIRED)
    find_package(FUSE3)
    if (NOT HAS_FUSE3)
        find_package(FUSE REQUIRED)
    endif()
    find_package(RocksDB REQUIRED)
    find_package(LibSodium REQUIRED)
    include_directories(
        ${JERASURE_INCLUDE_DIR} ${TOOLBOX_INCLUDE_DIR} ${GOP_INCLUDE_DIR} ${LIO_INCLUDE_DIR}
        ${IBP_INCLUDE_DIR} ${FUSE_INCLUDE_DIR} ${ROCKSDB_INCLUDE_DIR} ${LIBSODIUM_INCLUDE_DIR}
    )

    list(APPEND LIBS
        ${LIO_LIBRARY} ${IBP_LIBRARY} ${GOP_LIBRARY} ${TOOLBOX_LIBRARY}
        ${JERASURE_LIBRARY} ${ZMQ_LIBRARY} ${FUSE_LIBRARIES} ${ROCKSDB_LIBRARY} ${ROCKSDB_EXTRA_LIBRARY} ${LIBSODIUM_LIBRARY}
    )
endif()

if (HAS_FUSE3)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DHAS_FUSE3")
endif()

check_symbol_exists(statx "sys/stat.h" HAS_STATX)
if (HAS_STATX)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DHAS_STATX")
endif()

set(LSTORE_LIBS toolbox
                gop
                ibp
                lio
                ${APR_LIBRARY}
                ${APRUTIL_LIBRARY}
                ${ZLIB_LIBRARY}
                ${OPENSSL_LIBRARIES}
                ${ZMQ_LIBRARY}
                ${FUSE_LIBRARY}
                ${JERASURE_LIBRARY}
                ${ROCKSDB_LIBRARY}
                ${ROCKSDB_EXTRA_LIBRARY}
                ${LIBSODIUM_LIBRARY}
                dl
                )
set(LSTORE_INCLUDE_SYSTEM ${APR_INCLUDE_DIR}
                    ${APRUTIL_INCLUDE_DIR}
                    ${OPENSSL_INCLUDE_DIR}
                    ${ZMQ_INCLUDE_DIR}
                    ${ZLIB_INCLUDE_DIR}
                    ${FUSE_INCLUDE_DIR}
                    ${JERASURE_INCLUDE_DIR}
                    ${ROCKSDB_INCLUDE_DIR}
                    ${LIBSODIUM_INCLUDE_DIR})
set(LSTORE_INCLUDE_PUBLIC ${PROJECT_SOURCE_DIR})

#Temporary hack until the packaged version of RocksDB catches up with git
include(CheckLibraryExists)
if("${ROCKSDB_EXTRA_LIBRARY}" STREQUAL "")
    check_library_exists(${ROCKSDB_LIBRARY} rocksdb_cancel_all_background_work ""  HAS_ROCKSDB_CANCEL)
else()
    set(HAS_ROCKSDB_CANCEL "YES")
endif()
if (NOT HAS_ROCKSDB_CANCEL)
    message(STATUS "MISSING RocksDB_cancel!")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_ROCKSDB_CANCEL_MISSING")
    set (ROCKSDB_CANCEL "rocksdb_cancel_all_background_work.cc")
endif()

# common objects
set(LSTORE_PROJECT_OBJS
        lio_stdio_wrapper.c
)

set(LSTORE_PROJECT_INCLUDES_OLD)
set(LSTORE_PROJECT_INCLUDES_NAMESPACE lio_stdio_wrapper)
set(LSTORE_PROJECT_INCLUDES
        lio_stdio_wrapper/lio_stdio_wrapper.h
)

set(LSTORE_PROJECT_EXECUTABLES
)

set(LSTORE_INCLUDE_PUBLIC ${PROJECT_SOURCE_DIR})

# Common functionality is stored here
if(NOT USE_SUPERBUILD)
    include(LStoreCommon)
else()
    include(LStoreFunctions)
    lstore_project_common(lio_stdio_wrapper)
endif()
