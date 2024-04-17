# Find the FUSE3 includes and library
#
#  FUSE_INCLUDE_DIR - where to find fuse.h, etc.
#  FUSE_LIBRARIES   - List of libraries when using FUSE.
#  FUSE3_FOUND      - True if FUSE3 header and libs are found used by CMake 
#  HAS_FUSE3        - True if FUSE3 found and intended for use by the compiler

# check if already in cache, be silent
IF (FUSE_INCLUDE_DIR)
        SET (FUSE_FIND_QUIETLY TRUE)
ENDIF (FUSE_INCLUDE_DIR)

# find includes
FIND_PATH (FUSE3_LIO_INCLUDE_DIR fuse3/fuse.h
  HINTS /usr/local/include
        /usr/local/include
        /usr/include
)


FIND_LIBRARY(FUSE3_LIO_LIBRARY
        NAMES fuse3-lio
        HINTS ${CMAKE_INSTALL_PREFIX}/lib
              ${CMAKE_INSTALL_PREFIX}/lib/x86_64-linux-gnu
              ${CMAKE_INSTALL_PREFIX}/lib64
)

SET(FUSE_LIBRARY ${FUSE3_LIO_LIBRARY})

if (FUSE3_LIO_LIBRARY AND FUSE3_LIO_INCLUDE_DIR)
    SET(FUSE3_FOUND YES)
    SET(FUSE3_LIO_FOUND YES)
    SET(HAS_FUSE3 1)
    SET(FUSE_LIBRARIES ${FUSE3_LIO_LIBRARY})
    SET(FUSE_INCLUDE_DIR ${FUSE3_LIO_INCLUDE_DIR})
endif()

if (FUSE3_LIO_FOUND)
   message(STATUS "Found FUSE3_LIO: ${FUSE_LIBRARY} ${FUSE_INCLUDE_DIR}")
else()
   message(STATUS "Could not find FUSE3_LIO library: LIB=${FUSE_LIBRARY} INC=${FUSE_INCLUDE_DIR}")
endif()

include ("FindPackageHandleStandardArgs")
find_package_handle_standard_args ("FUSE3_LIO" DEFAULT_MSG
    FUSE_INCLUDE_DIR FUSE_LIBRARIES)

mark_as_advanced (FUSE_INCLUDE_DIR FUSE_LIBRARIES HAS_FUSE3 FUSE3_FOUND FUSE3_LIO_FOUND)
