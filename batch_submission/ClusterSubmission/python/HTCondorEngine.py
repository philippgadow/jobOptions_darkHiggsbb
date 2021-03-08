#! /usr/bin/env python
from ClusterSubmission.ClusterEngine import ClusterEngine
from ClusterSubmission.Utils import TimeToSeconds, CreateDirectory, WriteList, id_generator, AppendToList
import os, logging
logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)


####################################################################################################################################
##  Implementation of the Cluster engine using a HTCondor system as backend
##  Details about HTCondor can be found at
##      --- http://information-technology.web.cern.ch/services/fe/lxbatch/howto/quickstart-guide-htcondor
##      --- https://indico.cern.ch/event/611296/contributions/2604376/attachments/1471164/2276521/TannenbaumT_UserTutorial.pdf
##      --- http://research.cs.wisc.edu/htcondor/manual/v8.6/2_10DAGMan_Applications.html
##      --- https://batchdocs.web.cern.ch/local/submit.html
####################################################################################################################################
class HTCondorJob(object):
    ###Helper class for HTCondor job submission: the individual job.
    #    --- job_name: name of the job
    #    --- submission_file: path to job sumission file
    #    --- arraylength: Defines whether the job will be submitted as an array or not
    #    --- abbreviation: One letter abbrevation of the job to keep the DAG file small in cases of large arrays
    #    --- engine: Instance of the submitting HTCondorEngine
    def __init__(self, job_name, submission_file, arraylength=-1, engine=None, abbreviation=''):
        self.__job_name = job_name
        self.__submission_file = submission_file
        self.__children = []
        self.__array_size = arraylength
        self.__engine = engine
        self.__abbreviation = abbreviation

    def abb_letter(self):
        return self.__abbreviation

    def array_size(self):
        return self.__array_size

    def htcondor_engine(self):
        return self.__engine

    def getJobName(self):
        return self.__job_name

    def getSubmissionFile(self):
        return self.__submission_file

    def getChildren(self):
        return self.__children

    def getChild(self, child_name):
        for x in self.getChildren():
            if x[0].getJobName() == child_name:
                return x[0]
        return None

    def addChild(self, child, task_ids=[]):
        # allow dependency to be both single job name or list of job names, in first case promote to single object list
        if not isinstance(child, HTCondorJob):
            logging.warning("Wrong object given to addChild")
            return False
        if self.getChild(child): return True
        if child == self: return True
        self.__children += [(child, task_ids)]
        return True

    def get_job_config_str(self):
        if self.array_size() < 1:
            return ["JOB %s %s" % (self.abb_letter(), self.getSubmissionFile())]
        return ["JOB %s%d %s" % (self.abb_letter(), i + 1, self.getSubmissionFile()) for i in range(self.array_size())
                ] + ["VARS %s%d CONDOR_TASK_ID=\"%d\"" % (self.abb_letter(), i, i) for i in range(1,
                                                                                                  self.array_size() + 1)]

    def get_child_str(self):
        if self.array_size() < 1:
            return self.abb_letter()
        return " ".join(["%s%d" % (self.abb_letter(), i) for i in range(1, self.array_size() + 1)])

    def get_dependency_str(self):
        good_jobs = [
            job_task for job_task in self.__children
            if len([1 for grand_task in self.__children if grand_task[0].getChild(job_task[0].getJobName())]) == 0
        ]

        if len(good_jobs) == 0: return []

        if self.array_size() < 1:
            return ["PARENT %s CHILD %s" % (self.abb_letter(), " ".join([job_task[0].get_child_str() for job_task in good_jobs]))]
        parent_dict = {}

        for child, tasks in good_jobs:
            ### The job is parent for all children
            if len(tasks) == 0:
                for tsk_id in range(1, self.array_size() + 1):
                    p_str = "%s%d" % (self.abb_letter(), tsk_id)
                    try:
                        parent_dict[p_str] += [child.get_child_str()]
                    except:
                        parent_dict[p_str] = [child.get_child_str()]
            ### One by one mapping of the jobs
            elif -1 in tasks:
                for tsk_id in range(1, self.array_size() + 1):
                    p_str = "%s%d" % (self.abb_letter(), tsk_id)
                    ch_str = "%s%d" % (child.abb_letter(), tsk_id)
                    try:
                        parent_dict[p_str] += [ch_str]
                    except:
                        parent_dict[p_str] = [ch_str]
            ### Use the sub id as indicated in the array
            else:
                for tsk_id in tasks:
                    p_str = "%s%d" % (self.abb_letter(), tsk_id)
                    try:
                        parent_dict[p_str] += [child.get_child_str()]
                    except:
                        parent_dict[p_str] = [child.get_child_str()]
        return sorted(["PARENT %s CHILD %s" % (parent, " ".join(children)) for parent, children in parent_dict.iteritems()])


class HTCondorEngine(ClusterEngine):
    ### Make sure to always copy the constructor from the basic cluster engine
    def __init__(self,
                 jobName,
                 baseDir,
                 buildTime="01:59:59",
                 mergeTime="01:59:59",
                 buildCores=2,
                 buildMem=1400,
                 mergeMem=100,
                 maxArraySize=40000,
                 maxCurrentJobs=-1,
                 mail_user="",
                 accountinggroup="",
                 singularity_image="",
                 run_in_container=True,
                 hold_build=[],
                 exclude_nodes=[],
                 submit_build=True):
        ClusterEngine.__init__(self,
                               jobName=jobName,
                               baseDir=baseDir,
                               buildTime=buildTime,
                               mergeTime=mergeTime,
                               buildCores=buildCores,
                               buildMem=buildMem,
                               mergeMem=mergeMem,
                               maxArraySize=maxArraySize,
                               maxCurrentJobs=maxCurrentJobs,
                               mail_user=mail_user,
                               accountinggroup=accountinggroup,
                               singularity_image=singularity_image,
                               run_in_container=run_in_container,
                               hold_build=hold_build,
                               exclude_nodes=exclude_nodes,
                               submit_build=submit_build)

        ### Save the job dependencies in a dictionary to be
        ### processed after the submission files have been written
        self.__job_dependency_dict = []
        self.__submitted_jobs = 0
        self.__abbreviation = 'A'

    def _write_submission_file(self, sub_job, exec_script, env_vars=[], mem=1, run_time='00:00:01', nproc=1, arraylength=-1):
        self.set_cluster_control_module("ClusterSubmission/ClusterControlHTCONDOR.sh")
        if not exec_script:
            logging.error("<_write_submission_file> No exec_script was given!")
            return False
        if mem < 0:
            logging.error("<_write_submission_file> No memory requirement for the job was specified.")
            return False
        job_name = self.subjob_name(sub_job)
        if len([x for x in self.__job_dependency_dict if x.getJobName() == job_name]):
            logging.error("The job %s has already been defined. Please ensure unique job names" % (job_name))
            return False

        log_string = "%s/%s%s" % (self.log_dir(), sub_job if len(sub_job) else job_name, "_$(CONDOR_TASK_ID)" if arraylength > 0 else "")

        exec_script = self.pack_environment(env_vars, exec_script)
        submision_content = []

        submision_content += [
            "universe                = vanilla",
            "executable              = %s" % (exec_script),
            "output                  = %s.out" % (log_string),
            "error                   = %s.err" % (log_string),
            "log                     = %s.log" % (log_string),
            #"transfer_executable     = True",
            "notification            = Error",
            "notify_user             = %s" % (self.mail_user()),
            "request_memory          = %d" % (mem),
            "on_exit_remove          = (ExitBySignal == False) && (ExitCode == 0)",
            "request_cpus            = %d" % (nproc),
            #### Extra attributes
            "+MaxRuntime             = %d" % (TimeToSeconds(run_time)),  ### CERN cloud
            "+RequestRuntime         = %d" % (TimeToSeconds(run_time)),  ### DESY cloud
            "+MyProject              = %s" % (self.accountinggroup()) if self.accountinggroup() else "",
        ]

        if arraylength > 0:
            submision_content += ["environment = CONDOR_TASK_ID=$(CONDOR_TASK_ID)"]
        submision_content += [
            "queue",
        ]
        self.__job_dependency_dict += [
            HTCondorJob(job_name=job_name,
                        submission_file=WriteList(submision_content, "%s/%s.sub" % (self.config_dir(), id_generator(25))),
                        arraylength=arraylength,
                        engine=self,
                        abbreviation=self.__assign_abb_letter())
        ]
        self.__submitted_jobs += 1 if arraylength <= 1 else arraylength
        return True

    def _get_condor_job(self, job_name):
        for job in self.__job_dependency_dict:
            if job.getJobName() == job_name:
                return job
        return None

    def subjob_name(self, sub_job):
        if len(sub_job): return sub_job
        return self.job_name()

    def __process_dependencies(self, sub_job, hold_jobs=[]):
        """Users have the possibility to pipe either the string
           of job names [ "FirstJobToHold", "SecondJobToHold", "TheCake"]
           or to pipe a tuple which can be either of the form
              [ ("MyJobsArray" , [1,2,3,4,5,6,7,8,9,10,11]), "The cake"]
           meaning that the job ID's are constructed following the
           sequence 1-11. It's important to emphazise that the array
           *must* start with a 1. 0's are ignored by the system. There
           a third option, where the user parses
               ["MyJobArray", -1]
           that is supported by the slurm engine is currently not supported for htcondor."""

        child = self._get_condor_job(self.subjob_name(sub_job))
        for parent in self.to_hold(hold_jobs):
            parent_name = parent if isinstance(parent, str) else parent[0]
            parent_job = self._get_condor_job(parent_name)
            if not parent_job:
                logging.warning("Could not establish dependency towards %s" % (parent_name))
                continue
            parent_job.addChild(child, [] if isinstance(parent, str) else parent[1])

    def submit_job(self, script, sub_job="", mem=-1, env_vars=[], hold_jobs=[], run_time="", n_cores=1):
        if not self._write_submission_file(
                sub_job=sub_job, exec_script=script, env_vars=env_vars, mem=mem, run_time=run_time, nproc=n_cores):
            return False

        self.__process_dependencies(sub_job=sub_job, hold_jobs=hold_jobs)
        return True

    ### The resolvement of the particular letters generates tremendous large
    ### DAG files. To prolong the moment of potential suffocation. The jobs are
    ### abbreviated by a single letter in the DAG file
    def __assign_abb_letter(self):
        letter = self.__abbreviation
        inc_letter = True
        new_str = ""
        for pos in reversed(range(len(self.__abbreviation))):
            if inc_letter and letter[pos] != 'Z':
                new_str = chr(ord(letter[pos]) + 1) + new_str
                inc_letter = False
            elif not inc_letter:
                new_str = letter[pos] + new_str
            else:
                new_str = 'A' + new_str

        if inc_letter: new_str = 'A' + new_str
        self.__abbreviation = new_str
        return letter

    def submit_array(self, script, sub_job="", mem=-1, env_vars=[], hold_jobs=[], run_time="", n_cores=1, array_size=-1):
        if not self._write_submission_file(
                sub_job=sub_job, exec_script=script, env_vars=env_vars, mem=mem, run_time=run_time, nproc=n_cores, arraylength=array_size):
            return False
        self.__process_dependencies(sub_job=sub_job, hold_jobs=hold_jobs)
        return True

    def finish(self):
        if len(self.__job_dependency_dict) == 0:
            logging.debug("Nothing has been scheduled")
            return False

        dag_content = []
        for job in self.__job_dependency_dict:
            dag_content += job.get_job_config_str()
        dag_content += ["\n\n\n"]

        for job in self.__job_dependency_dict:
            dag_content += job.get_dependency_str()

        dag_dir = self.log_dir() + "/DAG/"
        dag_location = WriteList(dag_content, "%s/%s.dag" % (dag_dir, self.job_name()))
        os.chdir(dag_dir)
        cmd = "condor_submit_dag -verbose -maxidle %d %s %s.dag" % (self.max_running_per_array(),
                                                                    ("-append '+MyProject = \"%s\"'" % self.accountinggroup()
                                                                     if self.accountinggroup() else ""), self.job_name())
        return not os.system(cmd)
