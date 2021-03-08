#! /usr/bin/env python
from __future__ import print_function
from ClusterSubmission.ClusterEngine import ClusterEngine
from ClusterSubmission.Utils import CreateDirectory, prettyPrint
import os, time, subprocess
import logging
logging.basicConfig(format='%(levelname)s: % (message)s', level=logging.INFO)


######################################################################
##      SUN-GRID ENGINE (SGE)
#####################################################################
class SGEEngine(ClusterEngine):
    def __jobName_to_jobID(self, jobName, subJobs=[]):
        jobNames = []
        jobIDs = []

        if subJobs == []:
            jobNames = [jobName]
        else:
            jobNames = [jobName + "_%d" % i for i in subJobs] + [jobName + "__%d" % i for i in subJobs]

        for job in jobNames:
            cmd = "qstat -f | grep -B 1 'Job_Name = {}' |  grep -o -E \"[0-9]{{8,}}\\..*.physics.ox.ac.uk\"".format(job)
            try:
                out = subprocess.check_output([cmd], shell=True)
            except:  # In case the grep commands fail, i.e. the job hasn't run or has finished running
                out = ""
            for line in out.split('\n'):
                jobIDs.append(line)

        return jobIDs

    def __schedule_jobs(self, to_hold, sub_job):
        To_Hold = ""
        prettyPrint("", "#############################################################################")
        if len(sub_job) == 0: prettyPrint("Submit cluster job:", self.job_name())
        else: prettyPrint("Submit job: ", "%s in %s" % (self.subjob_name(sub_job), self.job_name()))
        prettyPrint("", "#############################################################################")
        info_written = False

        for H in to_hold:
            if not info_written: prettyPrint("Hold %s until" % (self.subjob_name(sub_job) if len(sub_job) else self.job_name()), "")
            if isinstance(H, str):
                To_Hold = ":".join(["%s" % s for s in ([To_Hold] + self.__jobName_to_jobID(H)) if s != ""])
            elif isinstance(H, tuple):
                To_Hold = ":".join(["%s" % s for s in ([To_Hold] + self.__jobName_to_jobID(H[0], H[1])) if s != ""])
            else:
                logging.error("<_schedule_jobs>: Invalid object")
                logging.error(H)
                exit(1)
            prettyPrint("", H if isinstance(H, str) else H[0])

        if (len(To_Hold)):
            if "Clean" in self.subjob_name(sub_job) or "Copy-LCK" in self.subjob_name(sub_job):
                To_Hold = "-W depend=afterany:" + To_Hold
            else:
                To_Hold = "-W depend=afterok:" + To_Hold

        return To_Hold

    def submit_job(self, script, sub_job="", mem=-1, env_vars=[], hold_jobs=[], run_time="", n_cores=1):
        if not CreateDirectory(self.log_dir(), False): return False
        exec_script = self.link_to_copy_area(script)
        pwd = os.getcwd()
        os.chdir(self.log_dir())

        if not exec_script: return False
        if mem < 0:
            logging.error("Please give a reasonable memory")
            return False

        additionalOptions = "-l cput={run_time} -l walltime={run_time} ".format(run_time=run_time)
        additionalOptions += "-l nodes=1:ppn={} ".format(n_cores)

        env_vars.append(("ATLAS_LOCAL_ROOT_BASE", ""))  # Force scripts re-asetup

        submit_cmd = "qsub -o {log_dir} -j oe {dependencies} -N '{jobName}' {env_vars} {additionalOptions} {exec_script}".format(
            log_dir=self.log_dir(),
            dependencies=self.__schedule_jobs(self.to_hold(hold_jobs), sub_job),
            jobName=self.subjob_name(sub_job),
            env_vars=" -v \"" + ",".join(["%s='%s'" % (var, value) for var, value in (env_vars + self.common_env_vars())]) + "\"",
            additionalOptions=additionalOptions,
            exec_script=exec_script)

        print(submit_cmd)

        if os.system(submit_cmd):
            logging.error("Failed to submit " + submit_cmd)
            return False
        os.chdir(pwd)
        return True

    def submit_array(self, script, sub_job="", mem=-1, env_vars=[], hold_jobs=[], run_time="", n_cores=1, array_size=-1):
        if array_size < 1:
            logging.error("Please give a valid array size")
            return False
        ### Oxford cluster does not support arrays. Cast the jobs into single subjobs
        for job_n in range(array_size):
            if not self.submit_job(script=script,
                                   sub_job="%s_%d" % (sub_job, job_n + 1),
                                   mem=mem,
                                   env_vars=env_vars + [("SGE_TASK_ID", job_n + 1)],
                                   hold_jobs=hold_jobs,
                                   run_time=run_time):
                return False
        return True
