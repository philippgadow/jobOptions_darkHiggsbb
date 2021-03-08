#!/bin/bash

###############################################
## Set up job for batch system
###############################################
if [ -f "${ClusterControlModule}" ]; then
    source ${ClusterControlModule}
fi

ID=`get_task_ID`
if [ -z ${ID} ]; then
    echo "Error, job ID could not be determined: ID=${ID}. Exiting."
    send_to_failure
fi

###############################################
## Find the seed from the common seed file
###############################################
Seed=""
if [ -f "${SeedFile}" ];then
    echo ${SeedFile}
    Seed=`sed -n "${ID}{p;q;}" ${SeedFile}`
else
    echo "Seed could not be extracted from seed file. Exiting."
    send_to_failure            
    exit 100
fi

################################################
## Find the run number 
################################################
RUN=""
if [ -f "${RunFile}" ];then
    RUN=`sed -n "${ID}{p;q;}" ${RunFile}`
else
    echo "Run number not found. Exiting."
    send_to_failure            
    exit 100
fi


##################################################
## Find the input events
##################################################
EVNT_DIR=""
if [ -f "${InFile}" ];then
    EVNT_DIR=`sed -n "${ID}{p;q;}" ${InFile}`
else
    echo "Input directory not found. Exiting."
    send_to_failure
    exit 100
fi
##################################################
## Find the output directory
##################################################
AOD_DIR=""
if [ -f "${OutFile}" ];then
    AOD_DIR=`sed -n "${ID}{p;q;}" ${OutFile}`
else
    echo "Output directory not found. Exiting"
    send_to_failure
    exit 100
fi


sleeptime=$((${Seed}%30))
echo "Sleeping for ${sleeptime}s to avoid too many parallel processes..."
sleep $sleeptime
echo 'Starting job.'

# check if TMPDIR exists or define it as TMP
[[ -d ${TMPDIR} ]] || export TMPDIR=${TMP}

echo "cd ${TMPDIR}"
cd ${TMPDIR}

echo "###############################################################################################"
echo "                             Job submission"
echo "###############################################################################################"
echo "Job name: ${Name}"
echo "Job ID: ${ID}"
echo "Working directory on batch machine: ${TMPDIR}"
echo "###############################################################################################"
echo " "

# Define input, output and log files
File_EVNT=mc16_13TeV.${RUN}.EVNT.${Seed}.pool.root
File_DAOD=mc16_13TeV.${RUN}.${Seed}.root
File_LOG=mc16_13TeV.${ReductionConf}.${RUN}.${Seed}.log
echo "###############################################################################################"
echo "					 		Configuration"
echo "###############################################################################################"
echo "DerivationRelease: "${DerivationRelease}
echo "ReductionConf: "${ReductionConf}
echo "RunNumber: "${RUN}
echo "Seed: "${Seed} 
echo "EVNTDIR: "${EVNT_DIR}
echo "EVNTFile:"${File_EVNT}
echo "AODDir: "${AOD_DIR}
echo "###############################################################################################"
echo " "
echo "###############################################################################################"

if [ ! -f ${EVNT_DIR}/${File_EVNT} ];then
	 echo "EVNT file has not been found. Exiting."
     send_to_failure
	 exit 100
fi

echo "Copy the EVENT_File to TMPDIR ${TMPDIR} "
cp ${EVNT_DIR}/${File_EVNT} ${TMPDIR}/
ls -lh

# store derivation ReductionConf to a file in the TMPDIR to retrieve it later
echo ${ReductionConf} > ${TMPDIR}/derivation_reduction_conf.txt

echo "###############################################################################################"
echo "                  Setting up the environment"
echo "###############################################################################################"
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
echo "Setting Up the ATLAS Enviroment:"
echo "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh" 
source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
echo "source $AtlasSetup/scripts/asetup.sh ${DerivationCache},${DerivationRelease},here"   
source $AtlasSetup/scripts/asetup.sh ${DerivationCache},${DerivationRelease},here   
echo "###############################################################################################"

# retrieve ReductionConf from file
ReductionConf=`cat derivation_reduction_conf.txt`

AODSucceed=2
echo "Starting Reco_trf.py..."
Reco_tf.py --inputEVNTFile ${File_EVNT} --outputDAODFile ${File_DAOD} --reductionConf ${ReductionConf} ${ExtraArgs}
if [ $? -eq 0 ]; then    
    AODSucceed=1
else
    echo "Reco_tf for derivation ${ReductionConf} failed."
fi

# post processing
ls -lh
mkdir -p  ${AOD_DIR}
if  [ -f ${AOD_DIR}/${File_LOG} ];then
	rm ${AOD_DIR}/${File_LOG}
	echo "Cleaning up: an old log file was removed."
fi

mv log.EVNTtoDAOD ${AOD_DIR}/${File_LOG}
echo "Log file ${AOD_DIR}/${File_LOG} was successfully copied."

# finish job execution
if [ "${AODSucceed}" -ne "1" ];then
    send_to_failure            
    exit 100
else
    if [ -f ${AOD_DIR}/DAOD_${ReductionConf}.${File_DAOD} ]; then
        rm ${AOD_DIR}/DAOD_${ReductionConf}.${File_DAOD}
        echo "Cleaning up: an old DAOD file was removed."
    fi


    mv DAOD_${ReductionConf}.${File_DAOD} ${AOD_DIR}
    echo "File was successfully copied. Terminating job."
fi
