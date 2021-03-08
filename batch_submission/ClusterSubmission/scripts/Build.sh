#!/bin/bash
if [ -f "${ClusterControlModule}" ]; then
    source ${ClusterControlModule}
fi
# some initial output
echo "###############################################################################################"
echo "                    BUILD JOB of batch JOB ${Name}"
echo "###############################################################################################"
export
echo "###############################################################################################"
echo "                    Setting up the environment"
echo "###############################################################################################"
echo "cd ${TMPDIR}"
cd ${TMPDIR}
echo "Setting Up the ATLAS Enviroment:"
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
echo "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh" 
source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh

echo "###############################################################################################"
echo "                        Remove obsolete files from previous jobs"
echo "###############################################################################################"
### The old area should be always removed in case 
### the build job is retried we want to have a clean directory
if [ -d "${COPYAREA}" ];then
    echo "Remove the old build area"
    rm -rf "${COPYAREA}"
fi
### Same for the old OUTPUT. Although that should realistically no longer
### happen
if [ -d "${CleanOut}" ];then
    echo "Remove also the old Output area"
    rm -rf  "${CleanOut}"
fi
if [ -d "${CleanTmp}" ];then
    echo "Remove also the old Temp area "${CleanTmp/%\//}
    Files=`ls ${CleanTmp}`
    for F in ${Files};do
        if [ "${F/%.root/}" != "${F}" ];then
            rm ${CleanTmp}/${F}
        fi
    done
fi

echo "mkdir -p ${CleanOut}"
mkdir -p ${CleanOut}
echo "mkdir -p ${CleanTmp}"
mkdir -p ${CleanTmp}
echo "###############################################################################################"
echo "                        Look for the athena area located at ${TestArea}"
echo "###############################################################################################"
echo "mkdir -p ${COPYAREA}/build"
mkdir -p ${COPYAREA}/build
#   Assume the following structure possibilities
#   
#       source                                    build
#       (mostly testarea in there)            (always there)
########################################################################
#               source                    build
#     (testarea in topdirectory)     (always there)
#
if [ ! -d "${OriginalArea}" ]; then
   echo "ERROR: the area ${OriginalArea} does not exist"
   send_to_failure
   exit 100
fi
if [ ! -d "${OriginalArea}/source" ] && [ ! -d "${OriginalArea}/build" ];then #Case a
    echo "mkdir -p ${COPYAREA}/source"
    mkdir -p ${COPYAREA}/source
    echo "cd ${COPYAREA}/source"
    cd ${COPYAREA}/source

    echo " "
    echo " "
    echo " "
    echo "###############################################################################################"
    ls -lh ${OriginalArea}
    echo "###############################################################################################"
    Packages=`ls ${OriginalArea}`
    for P in ${Packages};do        
        if [ ! -d ${OriginalArea}/${P} ] && [ "${P}" != "package_filters.txt" ];then
            continue
        fi
        ####################################################
        #           Do not copy the Install and workarea
        ####################################################
        if [ "${P}" == "InstallArea" ];then
            continue
        fi
        if [ "${P}" == "WorkArea" ];then
            continue
        fi    
        if [ "${P}" == "TarBall.tgz" ];then
            continue
        fi
        if [ "${P}" == "CMakeFiles" ];then
            continue
        fi
        if [ "${P}" == "x86_64-slc6-gcc49-opt" ];then
            continue
        fi
        if [ "${P}" == "x86_64-centos7-gcc62-opt" ];then
            continue
        fi
        if [ "${P}" == "build" ];then
                continue
        fi
        echo "Copy ${P} from ${OriginalArea} to ${COPYAREA} ..."
        rsync -a --copy-links --exclude=.git ${OriginalArea}/${P} ${COPYAREA}/source
        echo "Copying done."
    done
    echo "###############################################################################################"
    ls -lh ${COPYAREA}/source/
    echo "###############################################################################################"
    echo " "
else
    echo " "
    echo " "
    echo " "
    echo "###############################################################################################"
    ls -lh ${OriginalArea}
    echo "###############################################################################################"
    echo "Copy ${OriginalArea}/source to ${COPYAREA}"
    rsync -a --copy-links  ${OriginalArea}/source ${COPYAREA}
    cd ${COPYAREA}
fi
echo "###############################################################################################"
echo "                        Setup the Athena release"
echo "###############################################################################################"
echo "asetup ${OriginalProject},${OriginalPatch},here"
source $AtlasSetup/scripts/asetup.sh ${OriginalProject},${OriginalPatch},${OriginalVersion},here

#Compile in release 21
#
if [ -z "${CMTBIN}" ];then
    echo "cd ${COPYAREA}/build"
    cd ${COPYAREA}/build
    cmake ../source/
    if [ $? -ne 0 ];then
        echo "###############################################################################################"
        echo "                        BuildJob has ended with an error... Analysis jobs will be stopped"
        echo "###############################################################################################"
        echo "Will delete the current working area"
        if [ -d ${COPYAREA} ]; then
            rm -rf ${COPYAREA}
        fi
        send_to_failure
        exit 100        
    fi
    make -j${nCoresToUse} 
    if [ $? -ne 0 ];then
        echo "###############################################################################################"
        echo "                        BuildJob has ended with an error... Analysis jobs will be stopped"
        echo "###############################################################################################"
        echo "Will delete the current working area"
        if [ -d ${COPYAREA} ]; then
            rm -rf ${COPYAREA}
        fi
        send_to_failure           
        exit 100        
    fi    
else
    #Compile in release 20.7
    echo "setupWorkArea.py"
    setupWorkArea.py
    echo "cd WorkArea/cmt"
    cd WorkArea/cmt
    echo "cmt br cmt config"
    cmt br cmt config
    if [ $? -ne 0 ];then
        echo "###############################################################################################"
        echo "                        BuildJob has ended with an error... Analysis jobs will be stopped"
        echo "###############################################################################################"
        echo "Will delete the current working area"
        if [ -d ${COPYAREA} ]; then
            rm -rf ${COPYAREA}
        fi
        send_to_failure        
        exit 100        
    fi 
    echo "cmt br cmt make"
    cmt br cmt make -j 1
    if [ $? -ne 0 ];then
        echo "###############################################################################################"
        echo "                        BuildJob has ended with an error... Analysis jobs will be stopped"
        echo "###############################################################################################"
        echo "Will delete the current working area"
        if [ -d ${COPYAREA} ]; then
            rm -rf ${COPYAREA}
        fi
        send_to_failure       
        exit 100        
    fi 
fi
echo "###############################################################################################"
echo "                        BuildJob finished will now proceed with the analysis Jobs"
echo "###############################################################################################"
exit 0


