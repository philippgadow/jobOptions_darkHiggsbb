#! /usr/bin/env python
from ClusterSubmission.ClusterEngine import ClusterEngine
from ClusterSubmission.Utils import TimeToSeconds, CreateDirectory, prettyPrint, ResolvePath, getGmdOutput
import os, logging, time
logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)


def get_num_scheduled(user_name):
    cmd = "squeue --format=\"%%i\" -u %s" % (user_name)
    num_jobs = 0
    for i, line in enumerate(getGmdOutput(cmd)):
        if i == 0: continue
        if line.find("_") == -1 or (line.find("[") == -1 and line.find("]") == -1): num_jobs += 1

        else:
            array_size = line[line.find("[") + 1:line.rfind("%") if line.rfind("%") != -1 else line.rfind("]")]
            try:
                num_jobs += int(array_size.split("-")[-1])
            except:
                pass
    return num_jobs


####################################################################
###   Implementation to be used on the SLURM systems               #
####################################################################
class SlurmEngine(ClusterEngine):
    def submit_job(self, script, sub_job="", mem=-1, env_vars=[], hold_jobs=[], run_time="", n_cores=1):
        CreateDirectory(self.log_dir(), False)
        self.set_cluster_control_module("ClusterSubmission/ClusterControlSLURM.sh")
        exec_script = self.pack_environment(env_vars, script)
        pwd = os.getcwd()
        os.chdir(self.config_dir())

        if not exec_script: return False
        if mem < 0:
            logging.error("Please give a reasonable memory")
            return False

        if os.getenv("USER"): logging.info("Currently %d jobs are scheduled" % (get_num_scheduled(os.getenv("USER"))))
        submit_cmd = "sbatch --output=%s/%s.log  --mail-type=FAIL --mail-user='%s' --mem=%iM %s %s --job-name='%s' %s %s" % (
            self.log_dir(), sub_job if len(sub_job) > 0 else self.job_name(), self.mail_user(), mem, self.__partition(run_time),
            self.__schedule_jobs(self.to_hold(hold_jobs), sub_job), self.subjob_name(sub_job),
            "" if len(self.excluded_nodes()) == 0 else "--exclude=" + ",".join(self.excluded_nodes()), exec_script)

        if os.system(submit_cmd): return False
        os.chdir(pwd)
        return True

    def submit_array(self, script, sub_job="", mem=-1, env_vars=[], hold_jobs=[], run_time="", n_cores=1, array_size=-1):
        CreateDirectory(self.log_dir(), False)
        self.set_cluster_control_module("ClusterSubmission/ClusterControlSLURM.sh")
        pwd = os.getcwd()
        os.chdir(self.config_dir())
        if mem < 0:
            logging.error("Please give a reasonable memory")
            return False
        ArrayStart = 0
        ArrayEnd = min(self.max_array_size(), array_size)
        logging.info(" <submit_array>: Submit array %s with size %d" % (self.subjob_name(sub_job), array_size))
        while ArrayEnd > ArrayStart:
            n_jobs_array = min(array_size - ArrayStart, self.max_array_size())
            exec_script = self.pack_environment(env_vars + [("IdOffSet", str(ArrayStart))], script)
            if not exec_script: return False
            if os.getenv("USER"):
                time.sleep(1)
                logging.info("Going to add %d job to the currently %d scheduled ones" %
                             (n_jobs_array, get_num_scheduled(os.getenv("USER"))))
            submit_cmd = "sbatch --output=%s/%s_%%A_%%a.log --array=1-%i%s --mail-type=FAIL --mail-user='%s' --mem=%iM %s %s --job-name='%s' %s %s" % (
                self.log_dir(),
                sub_job if len(sub_job) > 0 else self.job_name(),
                n_jobs_array,
                "" if n_jobs_array < self.max_running_per_array() else "%%%d" % (self.max_running_per_array()),
                self.mail_user(),
                mem,
                self.__partition(run_time),
                ### Shedule the job after the hold jobs and also after previous array blocks
                self.__schedule_jobs(self.to_hold(hold_jobs) + ([] if ArrayStart == 0 else [self.subjob_name(sub_job)]), sub_job),
                self.subjob_name(sub_job),
                "" if len(self.excluded_nodes()) == 0 else "--exclude=" + ",".join(self.excluded_nodes()),
                exec_script,
            )
            if os.system(submit_cmd): return False
            ArrayStart = ArrayEnd
            ArrayEnd = min(ArrayEnd + self.max_array_size(), array_size)
        os.chdir(pwd)
        return True

    #### return the partion of the slurm engine
    def __partition(self, RunTime):
        partition = ""
        OldTime = 1.e25
        part_cmd = "sinfo --format='%%P %%l %%a'"
        for L in getGmdOutput(part_cmd):
            if len(L.split()) < 3: continue

            name = L.split()[0].replace("*", "")
            Time = L.split()[1]
            Mode = L.split()[2]
            t0 = TimeToSeconds(Time)
            if t0 > 0 and t0 > TimeToSeconds(RunTime) and t0 < OldTime and Mode == "up":
                partition = name
                OldTime = TimeToSeconds(Time)
        if len(partition) == 0:
            logging.error("Invalid run-time given %s" % (RunTime))
            exit(1)
        return " --partition %s --time='%s' " % (partition, RunTime)

    ### Convert the job into a slurm job-id
    def __slurm_id(self, job):
        Ids = []
        jobName = ''
        ### Bare subids as requested by the user
        sub_ids = []
        if isinstance(job, str):
            jobName = job
            #### feature to get only sub id's in a certain range
        elif isinstance(job, tuple):
            jobName = job[0]
            #### Users have the possibility to pipe either the string
            #### of job names [ "FirstJobToHold", "SecondJobToHold", "TheCake"]
            #### or to pipe a tuple which can be either of the form
            ####    [ ("MyJobsArray" , [1,2,3,4,5,6,7,8,9,10,11]), "The cake"]
            #### meaning that the job ID's are constructed following the
            #### sequence 1-11. It's important to emphazise that the array
            #### *must* start with a 1. 0's are ignored by the system. There
            #### is also an third option, where the user parses
            ####     ["MyJobArray", -1]
            #### This option is used to indicate a one-by-one dependency of
            #### tasks in 2 consecutive arrays.
            if isinstance(job[1], list):
                sub_ids = sorted([int(i) for i in job[1]])
                if -1 in sub_ids:
                    logging.warning(
                        "<__slurm_id>: -1 found in sub ids. If you want to pipe a 1 by 1 dependence of subjobs in two arrays please add [ (%s, -1) ]"
                        % (jobName))
        else:
            logging.error("Invalid object:")
            logging.error(job)
            exit(1)
        ### Find all associated job-ids
        for J in getGmdOutput("squeue --format=\"%%j %%i\""):
            if len(J.strip()) == 0: continue
            fragments = J.split()
            if len(fragments) == 0: continue
            if fragments[0].strip() == jobName:
                cand_id = fragments[1].strip()
                ### The pending job is an array
                if cand_id.find("_") != -1:
                    main_job = cand_id[:cand_id.find("_")]
                    job_range = cand_id[cand_id.find("_") + 1:]
                    ### We simply do not care about particular subjobs
                    if len(sub_ids) == 0:
                        if main_job not in Ids: Ids += [main_job]
                    elif len(sub_ids) > 0:
                        Ids += ["%s_%d" % (main_job, i) for i in sub_ids if i > 0]

                elif cand_id not in Ids:
                    Ids += [cand_id]
        Ids.sort()
        ### The allowed array size on the cluster is smaller than the
        ### requested size by the user --> jobs got split into multiple arrays
        if len(sub_ids) > 0 and max(sub_ids) > self.max_array_size():
            split_Ids = []
            for sub in sub_ids:
                ## How many times do we exceed the array size
                n = (sub - sub % self.max_array_size()) / self.max_array_size()
                rel_jobs = [int(j.split("_")[0]) for j in Ids if int(j.split("_")[1]) == sub]
                if len(rel_jobs) <= n:
                    logging.warning("<__slurm_id>: Failed to establish dependency on %s." % (jobName))
                    continue
                split_Ids += [
                    "%d_%d" % (rel_jobs[n], sub % self.max_array_size() if sub % self.max_array_size() != 0 else self.max_array_size())
                ]
            return split_Ids
        return Ids

    ### Schedule the job after the following jobs succeeded
    def __schedule_jobs(self, HoldJobs, sub_job="", RequireOk=True):
        prettyPrint("", "#############################################################################")
        if len(sub_job) == 0: prettyPrint("Submit cluster job:", self.job_name())
        else: prettyPrint("Submit job: ", "%s in %s" % (sub_job, self.job_name()))
        prettyPrint("", "#############################################################################")
        info_written = False

        to_hold = []
        dependency_str = ""
        for H in HoldJobs:
            ids = self.__slurm_id(H)
            if len(ids) > 0:
                if not info_written: prettyPrint("Hold %s until" % (sub_job if len(sub_job) else self.job_name()), "")
                info_written = True
                prettyPrint("",
                            H if isinstance(H, str) else
                            ("%s [%s]" % (H[0], ",".join(str(h) for h in H[1])) if isinstance(H[1], list) else "%s [1 by 1]" % (H[0])),
                            width=32,
                            separator='*')
            ### Usual dependency on entire jobs or certain subjobs in an array
            if isinstance(H, str) or isinstance(H[1], list): to_hold += ids
            elif isinstance(H[1], int) and H[1] == -1:
                dependency_str += " --dependency=aftercorr:%s " % (":".join(ids))
            else:
                logging.error("<schedule_jobs> Invalid object ")
                logging.error(H)
                exit(1)
        if len(to_hold) == 0: return ""
        if len(dependency_str) > 0:
            return dependency_str

        return " --dependency=" + ("afterok:" if RequireOk else "after:") + ":".join(to_hold)
