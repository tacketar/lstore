# FindISAL.cmake
# Finds the Intel Storage Acceleration Library (isa-l)
#
# This module sets:
#   ISAL_FOUND         - True if isa-l is found
#   ISAL_LIBRARY       - Path to the isa-l library
#   ISAL_INCLUDE_DIR   - Path to isa-l include directory
#   ISAL_LIBRARIES     - List of libraries (for compatibility)

# Hints for finding isa-l
set(ISAL_SEARCH_PATHS
    /usr
    /usr/local
    /opt
    ${CMAKE_SOURCE_DIR}/vendor/isa-l  # Vendored location
)

# Find the include directory
find_path(ISAL_INCLUDE_DIR
    NAMES isa-l/erasure_code.h
    PATHS ${ISAL_SEARCH_PATHS}
    PATH_SUFFIXES include
)

# Find the library
find_library(ISAL_LIBRARY
    NAMES isal libisal
    PATHS ${ISAL_SEARCH_PATHS}
    PATH_SUFFIXES lib lib64
)

# Handle the QUIETLY and REQUIRED arguments and set ISAL_FOUND
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ISAL
    REQUIRED_VARS ISAL_LIBRARY ISAL_INCLUDE_DIR
)

# Set additional variables for convenience
if(ISAL_FOUND)
    set(ISAL_LIBRARIES ${ISAL_LIBRARY})
    mark_as_advanced(ISAL_INCLUDE_DIR ISAL_LIBRARY)
else()
    message(WARNING "isa-l not found. Please install libisal-dev or vendor isa-l in ${CMAKE_SOURCE_DIR}/vendor/isa-l")
endif()
