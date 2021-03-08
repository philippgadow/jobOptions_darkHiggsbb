#!/bin/bash
echo "#####################################################################"
echo "                 Running singularity on the cluster                  "
echo "#####################################################################"
export
echo "#####################################################################"

export CONTAINER_CACHEDIR="${TMPDIR}/.singularity"
echo "export CONTAINER_CACHEDIR=${TMPDIR}/.singularity"
mkdir -p ${TMPDIR}/.home
echo "mkdir -p ${TMPDIR}/.home"
mkdir -p ${CONTAINER_CACHEDIR}
echo "mkdir -p ${CONTAINER_CACHEDIR}"
if [ -f "${CONTAINER_SHIPING_FILE}" ]; then
    cp ${CONTAINER_SHIPING_FILE} ${TMPDIR}/singularity.sh
    
    echo "Prepare the shiping file:"
    echo "echo \""
    echo "export SLURM_ARRAY_TASK_ID=${SLURM_ARRAY_TASK_ID}" >> ${TMPDIR}/singularity.sh
    echo "export SLURM_ARRAY_JOB_ID=${SLURM_ARRAY_JOB_ID}"   >> ${TMPDIR}/singularity.sh
    echo "export SLURM_JOB_USER=${SLURM_JOB_USER}"           >> ${TMPDIR}/singularity.sh
    echo "export SLURM_JOB_ID=${SLURM_JOB_ID}"               >> ${TMPDIR}/singularity.sh
    echo "export TMPDIR=${TMPDIR}"                           >> ${TMPDIR}/singularity.sh
    echo "export IdOffSet=${IdOffSet}"                       >> ${TMPDIR}/singularity.sh    
    echo "source ${CONTAINER_SCRIPT}"                        >> ${TMPDIR}/singularity.sh   
    cat ${TMPDIR}/singularity.sh 
    echo "\" > ${TMPDIR}/singularity.sh"
    chmod 0755 ${TMPDIR}/singularity.sh
else
  echo "ERROR: Variable shipping file does not exist.... Where is it?"
  exit 100     
fi
echo "#####################################################################" 
echo "#####################################################################"
echo "Start the container"
echo "#####################################################################"
ls -lha ${TMPDIR}
#singularity --version
echo "singularity exec --cleanenv -H ${TMPDIR}/.home:/alrb -B ${PWD}:/srv  ${CONTAINER_IMAGE} ${TMPDIR}/singularity.sh" 
singularity  exec --cleanenv -H ${TMPDIR}/.home:/alrb -B ${PWD}:/srv ${CONTAINER_IMAGE} ${TMPDIR}/singularity.sh
if [ $? -ne 0 ];then
    send_to_failure
fi




