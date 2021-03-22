#!bin/bash

# Job submission for DESY HTCondor - copy to project directory and adapt to your needs
JOBNAME="EVGEN_monosbb"
#List of DSIDs in job option directory
DSIDS="110000 110001 110002 110003 110004 \
       110010 110011 110012 110013 110014 \
       111000 111001 111002 111003 111004 111005 111006 111007 111008 111009 \
       111010 111011 111012 111013 111014 111015 111016 111017 111018 111019"
EVENTS=10000               #Events per job
NJOBS=1                    #Number of jobs per DSID
RUNTIME="03:00:00"         #Run time per job HH:MM:SS
MEMORY=2000                #Memory per job in MB
MODELSDIR=$PWD/models

cd batch_submission
COMMAND="python SubmitMC/python/submit.py --jobName ${JOBNAME} --engine HTCONDOR --eventsPerJob ${EVENTS} --nJobs ${NJOBS} -r ${DSIDS} --noBuildJob --modelsDir ${MODELSDIR} --accountinggroup af-atlas --evgen_runtime ${RUNTIME} --evgen_memory ${MEMORY}"
echo $COMMAND
$COMMAND
