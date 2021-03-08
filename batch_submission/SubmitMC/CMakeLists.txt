# Declare the package name:
atlas_subdir( SubmitMC )

# Declare the package's dependencies:
atlas_depends_on_subdirs(
    PUBLIC
    Tools/PathResolver )

# External dependencies:
find_package( ROOT COMPONENTS Core Tree RIO Hist Physics )

# Install files from the package:
atlas_install_data( python/*.py)
atlas_install_data( scripts/*.sh)

atlas_install_python_modules( python/*.py )

atlas_install_scripts( scripts/*.sh ) 
