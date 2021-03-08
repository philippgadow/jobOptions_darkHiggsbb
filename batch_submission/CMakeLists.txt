# Copyright (C) 2002-2021 CERN for the benefit of the ATLAS collaboration

cmake_minimum_required( VERSION 3.10 )
project( batch_submission LANGUAGES CXX )

# Find the base release.
if($ENV{AtlasVersion} MATCHES "^22")
  find_package( AthAnalysis 22.2 )
else()
  find_package( AthAnalysis REQUIRED )
endif()

set( ATLAS_PROJECT AthAnalysis )

# Set up CTest. It's necessary for just some technical reasons.
atlas_ctest_setup()

# in case the project is built on top of an athena checkout, atlas_project was already called, otherwise:
if(NOT DEFINED ATLAS_BASE_PROJECTS)
  # Set up the ATLAS project.
  if($ENV{AtlasVersion} MATCHES "^22")
    atlas_project( USE AthAnalysis 22.2 )
  else()
    atlas_project( WorkDir ${${ATLAS_PROJECT}_VERSION}
    USE ${ATLAS_PROJECT} ${${ATLAS_PROJECT}_VERSION}
    PROJECT_ROOT ${CMAKE_SOURCE_DIR}/ )
  endif()
  # Generate an environment setup script. This call makes sure that whatever
  # "external" you use in your analysis project, is set up correctly in the
  # runtime environment.
  lcg_generate_env( SH_FILE ${CMAKE_BINARY_DIR}/${ATLAS_PLATFORM}/env_setup.sh )
  install( FILES ${CMAKE_BINARY_DIR}/${ATLAS_PLATFORM}/env_setup.sh DESTINATION . )
endif()

# Set up CPack. This is necessary to be able to build TGZ files correctly from
# the compiled code. Making it possible to use the code on the grid.
atlas_cpack_setup()

# Remind to set up the environment
message( STATUS "")
message( STATUS "            In order to test your updates, please don't forget to" )
message( STATUS "            set up the environment with e.g.:" )
message( STATUS "            --->   source ${ATLAS_PLATFORM}/setup.sh" )
message( STATUS "")


