# cmake/FindISAL.cmake
# - Find ISA-L (replaces Jerasure)
# This module defines
#  ISAL_INCLUDE_DIR   - where to find isa-l/erasure_code.h etc.
#  ISAL_LIBRARIES     - the libraries needed to use ISA-L
#  ISAL_FOUND         - If false, do not try to use ISA-L


FIND_PATH(ISAL_INCLUDE_DIR
            NAMES   isa-l/erasure_code.h
                    isa-l/raid.h
                    isa-l/crc64.h
                    isa-l/gf_vect_mul.h
            HINTS ${CMAKE_SOURCE_DIR}/vendor/isa-l)

SET(ISAL_NAMES isal)
FIND_LIBRARY(ISAL_LIBRARY NAMES ${ISAL_NAMES})

IF (ISAL_LIBRARY AND ISAL_INCLUDE_DIR)
    SET(ISAL_LIBRARIES ${ISAL_LIBRARY})
    SET(ISAL_FOUND "YES")
ELSE ()
    SET(ISAL_FOUND "NO")
ENDIF ()

IF (ISAL_FOUND)
   IF (NOT ISAL_FIND_QUIETLY)
      MESSAGE(STATUS "Found ISA-L: ${ISAL_LIBRARIES} ${ISAL_INCLUDE_DIR}")
   ENDIF ()
ELSE ()
   MESSAGE(STATUS "Could not find ISA-L: ${ISAL_LIBRARIES} ${ISAL_INCLUDE_DIR}")
ENDIF ()

MARK_AS_ADVANCED(ISAL_LIBRARY ISAL_INCLUDE_DIR)