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
## Find the job option file
################################################
JOBOPTION=""
if [ -f "${JobFile}" ];then
    echo "${JobFile}"
    JOBOPTION=`sed -n "${ID}{p;q;}" ${JobFile}`
else
    echo "Job option not found. Exiting."
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
## Set up output location
##################################################
EVNT_DIR=""
if [ -f "${OutFile}" ];then
    EVNT_DIR=`sed -n "${ID}{p;q;}" ${OutFile}`
else
    echo "Output location not found. Exiting."
    send_to_failure            
    exit 100
fi

sleeptime=$((${Seed}%30))
echo "Sleeping for ${sleeptime}s to avoid too many parallel processes..."
sleep $sleeptime
echo 'Starting job.'

# check if TMPDIR exists or define it as TMP
[[ -d "${TMPDIR}" ]] || export TMPDIR="${TMP}" || export TMPDIR="/tmp/"

# store number of events and model directory to a file in the TMPDIR to retrieve it later
echo $NumberOfEvents > ${TMPDIR}/numberOfEvents.txt
echo $ModelsDirectory
if [[ -n "${ModelsDirectory}" ]];then
  echo $ModelsDirectory > ${TMPDIR}/ModelPath.txt
fi

echo "###############################################################################################"
echo "                             Job submission"
echo "###############################################################################################"
echo "Job name: ${Name}"
echo "Job ID: ${ID}"
echo "Working directory on batch machine: ${TMPDIR}"
echo "###############################################################################################"
echo " "

echo "###############################################################################################"
echo "                    Setting up the environment"
echo "###############################################################################################"
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
echo "Setting Up the ATLAS Enviroment:"
echo "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh" 
source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
echo "cd ${TMPDIR}"
cd ${TMPDIR}
echo "Setup ${EvgenRelease} (release ${EvgenCache}):"
echo "asetup ${EvgenRelease},${EvgenCache}"
asetup ${EvgenRelease},${EvgenCache}

# retrieve number of events from file
EVENTS=`cat numberOfEvents.txt`
# retrieve model path from file
if [[ -f "ModelPath.txt" ]]; then
    ModelsDirectory=`cat ModelPath.txt`
    export PYTHONPATH=${ModelsDirectory}:$PYTHONPATH
fi


echo "###############################################################################################"
echo "                             Configuration"
echo "###############################################################################################"
echo "JobOption: "${JOBOPTION}
echo "RunNumber: "${RUN}
echo "NumberOfEvents: "${EVENTS}
echo "Seed: "${Seed} 
echo "Output: "${EVNT_DIR}
echo "###############################################################################################"
echo " "

file_evnt="mc16_13TeV.${RUN}.EVNT.${Seed}.pool.root"
file_log="mc16_13TeV.${RUN}.EVNT.${Seed}.log"
GenSucced=2


echo "Gen_tf.py ${ExtraArgs} --ecmEnergy=13000 --firstEvent=1 --maxEvents=${EVENTS} --randomSeed=${Seed} --jobConfig='${JOBOPTION}' --outputEVNTFile='${file_evnt}'" 
Gen_tf.py \
    --ecmEnergy=13000 \
    --firstEvent=1 \
    --maxEvents="${EVENTS}" \
    --randomSeed=${Seed} \
    --jobConfig=${JOBOPTION} \
    --outputEVNTFile=${file_evnt} ${ExtraArgs}
if [ $? -eq 0 ]; then    
  GenSucced=1
else
  echo "Event generation failed."
fi
ls -lh

# post-processing
if [ ! -d ${EVNT_DIR} ];then 
    echo "Creating output directory: $EVNT_DIR"
    mkdir -p ${EVNT_DIR}
fi

if [ -f ${EVNT_DIR}/${file_log} ]; then
  rm ${EVNT_DIR}/${file_log}
  echo "Cleaning up: an old log file was removed."
fi

if [ -f ${EVNT_DIR}/${file_evnt} ]; then
  rm ${EVNT_DIR}/${file_evnt}
  echo "Cleaning up: an old EVNT file was removed."
fi

mv ${file_evnt} ${EVNT_DIR}
echo "Output: the EVNT file was saved to the output directory ${EVNT_DIR}."
cp log.generate ${EVNT_DIR}/${file_log}
echo "Output: the log file was saved to the output directory ${EVNT_DIR}."

if [ "${Keep}" == "True" ];then
  echo "Begin now to tar the whole directory"
  tar -hczf evtgendir.tar.gz ./*
  ls -lh
  mv evtgendir.tar.gz ${EVNT_DIR}/EVNT_${RUN}.${Seed}.tar.gz
fi  

if [ -f param_card.dat ] &&  [ "${Keep}" != "True" ];then
  cp param_card.dat ${SLHA_DIR}/${file_jo/".py"/".slha"}
  cp param_card.dat ${EVNT_DIR}/param_card.${RUN}.EVNT.${Seed}.dat    
fi

if [ -f proc_card_mg5.dat ]  && [ "${Keep}" != "True" ];then
  cp proc_card_mg5.dat ${EVNT_DIR}/proc_card.${RUN}.EVNT.${Seed}.dat    
fi

# finish job execution
if [ "${GenSucced}" -ne "1" ];then
    send_to_failure            
    exit 100
fi
echo "Event generation was successful. Proceed with the next step."
