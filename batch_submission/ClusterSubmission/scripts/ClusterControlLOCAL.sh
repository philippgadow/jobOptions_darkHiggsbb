#!/bin/bash
function get_job_ID() {
    echo "0"
    return 0
} 
function get_task_ID() {
    echo "${LOCAL_TASK_ID}"    
}
function send_to_failure(){   
    exit 1   
}
echo "Loaded the Cluster Control module for LOCAL multi-threading"
