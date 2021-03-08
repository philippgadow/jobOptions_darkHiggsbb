#!/bin/bash
function get_job_ID() {
    local job_id=""
    if [ -z ${SLURM_ARRAY_JOB_ID} ]; then
        job_id="${SLURM_JOB_ID}"
    else
        job_id="${SLURM_ARRAY_JOB_ID}"
    fi
    echo ${job_id}
} 
function get_task_ID() {
    local task_id=$((SLURM_ARRAY_TASK_ID+IdOffSet))
    echo ${task_id}   
}
function send_to_failure(){    
    if [ -z ${SLURM_ARRAY_TASK_ID} ];then        
        scontrol requeuehold ${SLURM_JOB_ID}
    else
        scontrol requeuehold ${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}
    fi 
    exit 1   
}
echo "Loaded the Cluster Control module for SLURM clusters"
