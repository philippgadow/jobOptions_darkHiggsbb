#!/bin/bash
echo "###############################################################################################"
echo "					 Environment variables"
echo "###############################################################################################"
export
echo "###############################################################################################"
if [ -f "${ClusterControlModule}" ]; then
    source ${ClusterControlModule}
fi
echo " "
mkdir -p "${DestinationDir}"
if [ -d "${FromDir}" ];then
    Content=`ls ${FromDir}`
    for item in ${Content};do
        echo "Copy ${item} to ${DestinationDir}"
        cp -r ${FromDir}/${item} ${DestinationDir}
    done
elif [ -f "${FromDir}" ]; then
    while read -r line; do
        echo "Copy ${line} to ${DestinationDir}"
        cp -r "${line}" "${DestinationDir}"    
    done < "${FromDir}"
else
    echo "###############################################################################################"
    echo "Directory: ${FromDir} does not exist"
    echo "###############################################################################################"
    send_to_failure        
    exit 100
fi
exit 0
