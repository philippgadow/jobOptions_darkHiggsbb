#!/bin/bash
echo "###############################################################################################"
echo "                     Environment variables"
echo "###############################################################################################"
export
echo "###############################################################################################"
echo " "
if [ -z "${ATLAS_LOCAL_ROOT_BASE}" ];then
    echo "###############################################################################################"
    echo "                    Setting up the environment"
    echo "###############################################################################################"
    echo "cd ${TMPDIR}"
    cd ${TMPDIR}
    export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
    echo "Setting Up the ATLAS Enviroment:"
    echo "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh"
    source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
    echo "asetup ${OriginalProject},${OriginalPatch},here"
    asetup ${OriginalProject},${OriginalPatch},here
else
    echo "Assuming your AthAnalysis environment has been set up"
fi


if [ -f "${ClusterControlModule}" ]; then
    source ${ClusterControlModule}
fi

ID=$(get_task_ID)
echo "Got task ID ${ID}"
MergeList=""
if [ -f "${JobConfigList}" ];then
    echo "MergeList=`sed -n \"${ID}{p;q;}\" ${JobConfigList}`"
    MergeList=`sed -n "${ID}{p;q;}" ${JobConfigList}`
else
    echo "ERROR: List ${JobConfigList} does not exist"
    send_to_failure
fi

if [ ! -f "${MergeList}" ];then
    echo "########################################################################"
    echo " ${MergeList} does not exist"
    echo "########################################################################"
    send_to_failure
    exit 100
fi
To_Merge=""
while read -r M; do
    FindSlash=${M##*/}
    if [ ! -d "${M/$FindSlash/}" ]; then
        echo "The Dataset ${M} is stored on dCache.  Assume that the file is healthy"
    elif [ ! -f "${M}" ];then
        echo "File ${M} does not exist. Merging Failed"
        echo "###############################################################################################"
        echo "                    Merging failed"
        echo "###############################################################################################"
        send_to_failure
        exit 100
    fi
    To_Merge="${To_Merge} ${M}"
done < "${MergeList}"

if [ -f "${OutFileList}" ]; then
    echo "OutFile=`sed -n \"${ID}{p;q;}\" ${OutFileList}`"
    OutFile=`sed -n "${ID}{p;q;}" ${OutFileList}`
fi
if [ -f "${OutFile}" ];then
    echo "Remove the old ROOT file"
    rm -f ${OutFile}
fi
echo "hadd ${TMPDIR}/tmp.root ${To_Merge}"
hadd ${TMPDIR}/tmp.root ${To_Merge}
if [ $? -eq 0 ]; then
    ls -lh
    echo "mv ${TMPDIR}/tmp.root ${OutFile}"
    mv ${TMPDIR}/tmp.root ${OutFile}
    echo "###############################################################################################"
    echo "                        Merging terminated successfully"
    echo "###############################################################################################"
else
    echo "###############################################################################################"
    echo "                    Merging failed"
    echo "###############################################################################################"
    send_to_failure
    exit 100
fi


