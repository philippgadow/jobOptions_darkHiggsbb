#!/bin/bash
function get_job_ID() {
    echo "WARNING: job ID not yet defined"
    return 1    
} 
function get_task_ID() {  
   echo "${CONDOR_TASK_ID}"   
}
function send_to_failure(){    
    echo "WARNING: Failure send not yet defined for CONDOR"
    exit 1   
}
echo "Loaded the Cluster Control module for HTCondor clusters"
