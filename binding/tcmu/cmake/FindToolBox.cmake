# -*- cmake -*-

# - Find toolbox libraries and includes
#
# This module defines
#    TOOLBOX_INCLUDE_DIR - where to find liblsl_client.h
#    TOOLBOX_LIBRARIES - the libraries needed to use Phoebus.
#    TOOLBOX_FOUND - If false didn't find phoebus

find_path(TOOLBOX_INCLUDE_DIR tbx/packer.h)

find_library(TOOLBOX_LIBRARY NAMES toolbox)

if (TOOLBOX_LIBRARY AND TOOLBOX_INCLUDE_DIR)
    SET(TOOLBOX_FOUND "YES")
endif (TOOLBOX_LIBRARY AND TOOLBOX_INCLUDE_DIR)


if (TOOLBOX_FOUND)
   message(STATUS "Found Toolbox: ${TOOLBOX_LIBRARY} ${TOOLBOX_INCLUDE_DIR}")
else (TOOLBOX_FOUND)
   message(STATUS "Could not find toolbox library")
endif (TOOLBOX_FOUND)


MARK_AS_ADVANCED(
  TOOLBOX_LIBRARY
  TOOLBOX_INCLUDE_DIR
  TOOLBOX_FOUND
)

