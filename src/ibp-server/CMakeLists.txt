cmake_minimum_required(VERSION 2.8)

# Name this project
set(LSTORE_PROJECT_NAME "ibpserver")
project(${LSTORE_PROJECT_NAME} C)

message(STATUS "HELLO EXTRA=${ROCKSDB_EXTRA_LIBRARY}")

# Include modules
if(NOT USE_SUPERBUILD)
    set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
endif()
include(CheckIncludeFile)

# Detect compiler flags.
# No-op for ibp

# Find additional dependencies.
if(NOT USE_SUPERBUILD)
    find_package(ToolBox REQUIRED)
    find_package(GOP REQUIRED)
    find_package(IBP REQUIRED)
    find_package(RocksDB REQUIRED)
    include_directories(${TOOLBOX_INCLUDE_DIR} ${GOP_INCLUDE_DIR} ${IBP_INCLUDE_DIR} ${ROCKSDB_INCLUDE_DIR})
    list(APPEND LIBS ${TOOLBOX_LIBRARY} ${GOP_LIBRARY} ${IBP_LIBRARY} ${ROCKSDB_LIBRARY} ${ROCKSDB_EXTRA_LIBRARY})
endif()
set(LSTORE_LIBS toolbox
                gop
                ibp
                ${APR_LIBRARY}
                ${APRUTIL_LIBRARY}
                ${ROCKSDB_LIBRARY}
                ${ROCKSDB_EXTRA_LIBRARY}
    )
set(LSTORE_INCLUDE_SYSTEM ${APR_INCLUDE_DIR} ${APRUTIL_INCLUDE_DIR} ${ROCKSDB_INCLUDE_DIR})
set(LSTORE_INCLUDE_PUBLIC ${PROJECT_SOURCE_DIR})

#Temporary hack until the packaged version of RocksDB catches up with git
include(CheckLibraryExists)
if( "${ROCKSDB_EXTRA_LIBRARY}" STREQUAL "")
    check_library_exists(${ROCKSDB_LIBRARY} rocksdb_cancel_all_background_work ""  HAS_ROCKSDB_CANCEL)
else()
    set(HAS_ROCKSDB_CANCEL "YES")
endif()
if (NOT HAS_ROCKSDB_CANCEL)
    message(STATUS "MISSING RocksDB_cancel!")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_ROCKSDB_CANCEL_MISSING")
    set (ROCKSDB_CANCEL "rocksdb_cancel_all_background_work.cc")
endif()

# Additional Compiler flags.
# No-op for ibp

# common objects
set(LSTORE_PROJECT_OBJS
    activity_log.c
    alloc_history.c
    buffer_transfer.c
    cap_timestamp.c
    cmd_send.c
    commands.c
    db_resource.c
    envelope.c
    envelope_net.c
    global_data.c
    handle_commands.c
    ibpserver_version.c
    install_commands.c
    lock_alloc.c
    osd_fs.c
    parse_commands.c
    phoebus.c
    print_alloc.c
    resource.c
    resource_list.c
    rid.c
    rnd_rid.c
    server.c
    server_lib.c
    subnet.c
    thread_slots.c
    transfer_stats.c
    ${ROCKSDB_CANCEL}
)

set(LSTORE_PROJECT_INCLUDES_OLD)
set(LSTORE_PROJECT_INCLUDES_NAMESPACE ibpserver)
set(LSTORE_PROJECT_INCLUDES
    )

set(LSTORE_PROJECT_EXECUTABLES
    date_spacefree
    expire_list
    get_alloc
    get_config
    get_corrupt
    get_version
    ibp_attach_rid
    ibp_detach_rid
    ibp_rid_mode
    ibp_rescan
    ibp_server
    ibp_undelete
    mkfs_resource
    print_alog
    read_alloc
    repair_history
)
set(LSTORE_INCLUDE_PUBLIC ${PROJECT_SOURCE_DIR})

# Common functionality is stored here

if(NOT USE_SUPERBUILD)
    include(LStoreCommon)
else()
    include(LStoreFunctions)
    lstore_project_common(${LSTORE_PROJECT_NAME})
endif()

foreach(f ${LSTORE_PROJECT_EXECUTABLES})
    set_target_properties(${f} PROPERTIES COMPILE_FLAGS "-DLSTORE_HACK_EXPORT" LINKER_LANGUAGE CXX)
    target_link_libraries(${f} LINK_PUBLIC dl)
endforeach()
