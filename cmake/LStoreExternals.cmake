include(ExternalProject)

#SEe if we need to build JErasure
if(BUILD_JERASURE OR (NOT JERASURE_FOUND) OR (JERASURE_LIBRARY MATCHES "^${EXTERNAL_INSTALL_DIR}"))
    list(APPEND REBUILD_DEPENDENCIES extern-jerasure)
    set(JERASURE_LIBRARY "jerasure")
    set(JERASURE_INCLUDE_DIR ${EXTERNAL_INSTALL_DIR}/include)
    ExternalProject_add(extern-jerasure
            PREFIX "${CMAKE_BINARY_DIR}/state/jerasure/"
            SOURCE_DIR "${CMAKE_SOURCE_DIR}/vendor/jerasure/"
            BINARY_DIR "${CMAKE_BINARY_DIR}/jerasure/"
            INSTALL_DIR "${EXTERNAL_INSTALL_DIR}"
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_DIR}
                        -DWANT_SHARED:BOOL=OFF -DWANT_STATIC:BOOL=ON
                        ${LSTORE_C_COMPILER_WRAPPER_CMAKE}
            TEST_COMMAND ""
            INSTALL_COMMAND $(MAKE) "install"
    )
endif()

# Build outr custom libfuse3 if needed
if (BUILD_FUSE3_LIO)
    #Go ahead and set all the Find handler variables
    list(APPEND REBUILD_DEPENDENCIES libfuse3_lio)
    set(HAS_FUSE3 1)
    set(FUSE3_FOUND 1)
    set(F3_SRC "${CMAKE_SOURCE_DIR}/vendor/libfuse3_lio")
    set(F3_BUILD "${CMAKE_SOURCE_DIR}/build/src/libfuse3_lio")
    set(F3_INSTALL "${CMAKE_INSTALL_PREFIX}")

    set(FUSE_LIBRARIES ${F3_INSTALL}/${CMAKE_INSTALL_LIBDIR}/libfuse3-lio.so)
    set(FUSE_INCLUDE_DIR ${F3_INSTALL}/include)
    set(FUSE_FOUND "true")
    set(FUSE_LIBRARIES_FOUND "true")
    set(FUSE_INCLUDE_DIR_FOUND "true" )

    #Declare the libfuse3 as an external project
    ExternalProject_add(libfuse3_lio
            SOURCE_DIR "${F3_SRC}"
            BINARY_DIR "${F3_BUILD}"
            INSTALL_DIR "${F3_INSTALL}"
            GIT_REPOSITORY "https://github.com/libfuse/libfuse.git"
            GIT_TAG "master"
            USES_TERMINAL_DOWNLOAD true
            USES_TERMINAL_UPDATE true
            USES_TERMINAL_BUILD true
            USES_TERMINAL_INSTALL true
            USES_TERMINAL_TEST true
            UPDATE_COMMAND ""
            PATCH_COMMAND ""
            CONFIGURE_COMMAND meson configure -D prefix=${F3_INSTALL} -D libdir=${CMAKE_INSTALL_LIBDIR}
            BUILD_COMMAND ninja -v
            INSTALL_COMMAND ninja install
    )

    #Add the name munging step
    ExternalProject_add_step(libfuse3_lio munge_step
        DEPENDEES download
        COMMAND echo "This is a download test"
        COMMAND mkdir -p ${F3_BUILD} || echo "Build dir already exists"
        COMMAND mkdir ${F3_INSTALL}   || echo "Install dir already exists"
    )

    ExternalProject_add_step(libfuse3_lio patch_step
        DEPENDEES patch
        DEPENDERS configure
        COMMAND echo "This is a patch test"
        COMMAND mkdir -p ${F3_BUILD} || echo "Build dir already exists"
        COMMAND mkdir ${F3_INSTALL}   || echo "Install dir already exists"
        COMMAND sed -i.bak -e "s/project('libfuse3',/project('libfuse3-lio',/" ${F3_SRC}/meson.build
        COMMAND cp ${F3_SRC}/lib/meson.build ${F3_SRC}/lib/meson.build.bak
        COMMAND cat ${F3_SRC}/lib/meson.build.bak | sed -e "s/library('fuse3',/library('fuse3-lio',/" | sed -e "s/name: 'fuse3',/name: 'fuse3-lio',/" > ${F3_SRC}/lib/meson.build
        COMMAND meson setup ${F3_BUILD} ${F3_SRC}
    )
endif()
