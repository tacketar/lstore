cmake_minimum_required(VERSION 2.8)

# Name this project
set(LSTORE_PROJECT_NAME "toolbox")
project(${LSTORE_PROJECT_NAME} C)

# Include modules
if(NOT USE_SUPERBUILD)
    set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
endif()

include(CheckIncludeFile)
include(CheckSymbolExists)

# Detect compiler flags.
check_include_file("stdint.h" HAVE_STDINT_H)
check_include_file("inttypes.h" HAVE_INTTYPES_H)
configure_file(${PROJECT_SOURCE_DIR}/${LSTORE_PROJECT_NAME}_config.h.in
               ${PROJECT_SOURCE_DIR}/${LSTORE_PROJECT_NAME}_config.h)


# Find dependencies.
# common objects
set(NETWORK_OBJS
    net_sock.c network.c
)

set(TOOL_OBJS
    append_printf.c
    atomic_counter.c
    chksum.c
    constructor.c
    direct_io.c
    dns_cache.c
    fork_helper.c
    iniparse.c
    interval_skiplist.c
    io.c
    log.c
    lru.c
    monitor.c
    normalize_path.c
    packer.c
    pigeon_coop.c
    pigeon_hole.c
    pipe_helper.c
    que.c
    random.c
    range_stack.c
    siginfo.c
    skiplist.c
    stack.c
    stdinarray_iter.c
    string_token.c
    thread_pool.c
    transfer_buffer.c
    varint.c
)

set(LSTORE_PROJECT_OBJS ${TOOL_OBJS} ${NETWORK_OBJS})
set(LSTORE_PROJECT_INCLUDES
    tbx/append_printf.h
    tbx/apr_wrapper.h
    tbx/assert_result.h
    tbx/atomic_counter.h
    tbx/chksum.h
    tbx/constructor_wrapper.h
    tbx/direct_io.h
    tbx/dns_cache.h
    tbx/fork_helper.h
    tbx/fmttypes.h
    tbx/iniparse.h
    tbx/interval_skiplist.h
    tbx/io.h
    tbx/io_declare.h
    tbx/io_assign.h
    tbx/lio_monitor.h
    tbx/list.h
    tbx/log.h
    tbx/lru.h
    tbx/monitor.h
    tbx/net_sock.h
    tbx/network.h
    tbx/normalize_path.h
    tbx/object.h
    tbx/packer.h
    tbx/pigeon_coop.h
    tbx/pigeon_hole.h
    tbx/pipe_helper.h
    tbx/que.h
    tbx/random.h
    tbx/range_stack.h
    tbx/skiplist.h
    tbx/siginfo.h
    tbx/stack.h
    tbx/stdinarray_iter.h
    tbx/string_token.h
    tbx/tbx_decl.h
    tbx/thread_pool.h
    tbx/transfer_buffer.h
    tbx/type_malloc.h
    tbx/varint.h
    tbx/visibility.h
    toolbox_config.h
)

set(LSTORE_PROJECT_INCLUDES_NAMESPACE tbx)
set(LSTORE_PROJECT_EXECUTABLES)
set(LSTORE_LIBS ${APR_LIBRARY}
                    ${APRUTIL_LIBRARY}
                    ${ZLIB_LIBRARY}
                    ${OPENSSL_LIBRARIES}
                    ${LIBSODIUM_LIBRARY}
    )
set(LSTORE_INCLUDE_SYSTEM ${APR_INCLUDE_DIR}
                    ${APRUTIL_INCLUDE_DIR}
                    ${OPENSSL_INCLUDE_DIR}
                    ${ZLIB_INCLUDE_DIR}
                    ${LIBSODIUM_INCLUDE_DIR}
    )
set(LSTORE_INCLUDE_PUBLIC ${PROJECT_SOURCE_DIR})

check_symbol_exists(statx "sys/stat.h" HAS_STATX)
if (HAS_STATX)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DHAS_STATX")
endif()

# Common functionality is stored here
if(NOT USE_SUPERBUILD)
    include(LStoreCommon)
else()
    include(LStoreFunctions)
    lstore_project_common(toolbox)
endif()
