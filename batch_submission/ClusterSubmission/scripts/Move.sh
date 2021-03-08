#!/bin/bash
echo "###############################################################################################"
echo "					 Environment variables"
echo "###############################################################################################"
export
echo "###############################################################################################"
echo " "
if [ -f "${ClusterControlModule}" ]; then
    source ${ClusterControlModule}
fi
mkdir -p "${DestinationDir}"
if [ -d "${FromDir}" ];then
    Content=`ls ${FromDir}`
    for item in ${Content};do
        echo "Move ${item} to ${DestinationDir}"
        mv ${FromDir}/${item} ${DestinationDir}
    done
elif [ -f "${FromDir}" ]; then
    while read -r line; do
        echo "Move ${line} to ${DestinationDir}"
        mv "${line}" "${DestinationDir}"    
    done < "${FromDir}"
else
    echo "###############################################################################################"
    echo "Directory: ${FromDir} does not exist"
    echo "###############################################################################################"
    send_to_failure        
    exit 100
fi
exit 0
