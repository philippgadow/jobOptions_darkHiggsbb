# Declare the package name:
atlas_subdir( ClusterSubmission )

# Declare the package's dependencies:
if($ENV{AtlasVersion} MATCHES "^21")
atlas_depends_on_subdirs(
    PUBLIC
    PhysicsAnalysis/POOLRootAccess
    Control/xAODRootAccess 
    Tools/PathResolver )
endif()

# External dependencies:
find_package( ROOT COMPONENTS Core Tree RIO Hist Physics )

# Install files from the package:
atlas_install_data( python/*.py)
atlas_install_data( scripts/*.sh)
atlas_install_data( data/*)

atlas_install_python_modules( python/*.py )

atlas_install_scripts( scripts/*.sh ) 

atlas_add_executable( CreateBatchJobSplit
   util/CreateBatchJobSplit.cxx
   INCLUDE_DIRS ${ROOT_INCLUDE_DIRS}
   LINK_LIBRARIES ${ROOT_LIBRARIES} POOLRootAccessLib xAODRootAccess )
