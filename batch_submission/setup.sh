#!/bin/bash

MYRELEASE="21.2.160,AthAnalysis"

# set up ATLAS asetup
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
source /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/user/atlasLocalSetup.sh

# check if build was already performed
if ls ../batch_submission_build/*/python/ 1> /dev/null 2>&1; then
    echo "Build already performed."

    # change to build directory
    cd ../batch_submission_build

    # set up release
    asetup ${MYRELEASE}
else
    echo "Need to build project, please wait..."

    # create build directory and change to it
    mkdir -p ../batch_submission_build
    cd ../batch_submission_build

    # set up release
    asetup ${MYRELEASE}

    # build project and set paths
    cmake ../batch_submission
    make -j4
fi

source */setup.sh

# back to original directory
cd -
