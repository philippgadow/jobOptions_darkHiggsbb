#!/bin/bash
if [ -f "${ClusterControlModule}" ]; then
    source ${ClusterControlModule}
fi
#### Argh bash is such cumbersome!!!!
ID=`get_task_ID`
if [ -z ${ID} ]; then
    echo "Someone has stolen my identity. I bail out"
    send_to_failure
fi
echo "###############################################################################################"
echo "                                 Batch JOB ${Name} - ${ID}"
echo "###############################################################################################"
Cmd=""
if [ -f "${ListOfCmds}" ];then
    echo "Cmd=`sed -n \"${ID}{p;q;}\" ${ListOfCmds}`"
    Cmd=`sed -n "${ID}{p;q;}" ${ListOfCmds}`
else
    echo "ERROR: No list of commands"
    send_to_failure 
fi
if [ -z "${Cmd}" ]; then
    echo "ERROR: No list of commands"
    send_to_failure
fi
# some initial output
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
    echo "Setting Up the ATLAS Enviroment:"
    export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
    echo "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh" 
    source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
    echo "Setup athena:"
    echo "asetup ${OriginalProject},${OriginalPatch},${OriginalVersion}"
    asetup ${OriginalProject},${OriginalPatch},${OriginalVersion}
    ### In the case the job relies on the TestArea variable
    echo "export TestArea=${OriginalArea}"
    export TestArea=${OriginalArea}

    #Check whether we're in release 21
    if [ -f ${OriginalArea}/../build/${BINARY_TAG}/setup.sh ]; then
        echo "source ${OriginalArea}/../build/${BINARY_TAG}/setup.sh"
        source ${OriginalArea}/../build/${BINARY_TAG}/setup.sh
        WORKDIR=${OriginalArea}/../build/${BINARY_TAG}/bin/
    elif [ -f ${OriginalArea}/../build/${WorkDir_PLATFORM}/setup.sh ];then
        echo "source ${OriginalArea}/../build/${WorkDir_PLATFORM}/setup.sh"
        source ${OriginalArea}/../build/${WorkDir_PLATFORM}/setup.sh
        source ${OriginalArea}/../build/${WorkDir_PLATFORM}/setup.sh
        WORKDIR=${OriginalArea}/../build/${WorkDir_PLATFORM}/bin/
     elif [ -f ${OriginalArea}/../build/${AthAnalysis_PLATFORM}/setup.sh ];then
            echo "source ${OriginalArea}/../build/${AthAnalysis_PLATFORM}/setup.sh"
            source ${OriginalArea}/../build/${AthAnalysis_PLATFORM}/setup.sh        
            WORKDIR=${OriginalArea}/../build/${AthAnalysis_PLATFORM}/bin/
    elif [ -f ${OriginalArea}/../build/${LCG_PLATFORM}/setup.sh ];then
            echo "source ${OriginalArea}/../build/${LCG_PLATFORM}/setup.sh"
            source ${OriginalArea}/../build/${LCG_PLATFORM}/setup.sh
            WORKDIR=${OriginalArea}/../build/${LCG_PLATFORM}/bin/
    elif [ -z "${CMTBIN}" ];then
        source  ${OriginalArea}/../build/x86_64*/setup.sh
        if [ $? -ne 0 ];then
            echo "Something strange happens?!?!?!"
            export
            echo " ${OriginalArea}/../build/${BINARY_TAG}/setup.sh"
            echo " ${OriginalArea}/../build/${WorkDir_PLATFORM}/setup.sh"            
            send_to_failure        
            exit 100
        fi
    fi    
else 
    echo "Assuming your AthAnalysis has been set up properly"
fi
echo "cd ${TMPDIR}"
cd ${TMPDIR}
echo "${Cmd}"
${Cmd}
if [ $? -eq 0 ]; then
    echo "###############################################################################################"
    echo "                        Command execution terminated successfully"
    echo "###############################################################################################"
else
    echo "###############################################################################################"
    echo "                              Job ${Name} has experienced an error"
    echo "###############################################################################################"
    send_to_failure            
    exit 100
fi
