#! /usr/bin/env python
from __future__ import print_function
from ClusterSubmission.Utils import CreateDirectory, WriteList, ResolvePath, prettyPrint, id_generator
import os, time, logging
from random import shuffle
logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)

#######################################################################################
#                         Environment variables                                       #
#######################################################################################
ATLASPROJECT = os.getenv("AtlasProject") if not os.getenv("AtlasPatch") else os.getenv("AtlasPatch")
ATLASVERSION = os.getenv("AtlasVersion") if not os.getenv("AtlasPatchVersion") else os.getenv("AtlasPatchVersion")
ATLASRELEASETYPE = os.getenv("AtlasReleaseType")
ATLASBUILDBRANCH = os.getenv("AtlasBuildBranch")
ATLASBUILDSTAMP = os.getenv("AtlasBuildStamp")
TESTAREA = os.getenv("TestArea")
ROOTSYS = os.getenv("ROOTSYS")
ROOTVERSION = "%s-%s" % (ROOTSYS.split("/")[-2], ROOTSYS.split("/")[-1])
SINGULARITY_DIR = "/cvmfs/atlas.cern.ch/repo/containers/images/singularity/"


#######################################################################
#           Basic class for cluster submission.                       #
#           It manages the structure of how jobs should be scheduled  #
#######################################################################
class ClusterEngine(object):
    ### Standard constructor
    ###  --- jobName: Name of the job on the cluster
    ###  --- baseDir: Most basic directory to build the directory structure for logs/tmps/outputs per day
    ###  --- buildTIme: <HH:MM:SS> How long is the run time of the build job
    ###  --- mergeTime: <HH:MM:SS> How long is the run time of each merge job
    ###  --- buildCores: How many CPUS can be used for the build job. i.e. make -j<buildCores>
    ###  --- buildMem: Virtual memory reserved for the build job
    ###  --- mergeVMem: Virual memory reserved for the merge job
    ###  --- maxArraySize: What is the maximum size of job arrays such that the underlying cluster-engine can handle it. Jobs exceeding this size will be split
    ###  --- maxCurrentJobs: How many jobs of each array can be run simultaneously. (Useful for system protection against overload)
    ###  --- mail_user: E-Mail address to which the status mails are sent (Engine dependent)
    ###  --- accountinggroup: What is the budget code of the group paying the money for the jobs (Not needed on all clusters)
    ###  --- singularity_image: Location of a docker image for the container (TO be refined for general usage)
    ###  --- run_in_container: Switch whether a singularity container shall be emplaced
    ###  --- exclude_nodes: The job is not sent to certain nodes in the system (Cluster dependent)
    ###  --- submit_build: Compile the source code again on the cluster beforehand (Recommended)
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

        self.__jobName = jobName
        self.__baseFolder = baseDir
        self.__Today = time.strftime("%Y-%m-%d")
        self.__buildTime = buildTime

        self.__submit_build = submit_build
        self.__buildMem = buildMem
        self.__mergeMem = mergeMem
        self.__buildCores = buildCores
        self.__buildTime = buildTime
        self.__holdBuild = hold_build
        self.__submitted_build = False
        self.__max_current = maxCurrentJobs
        self.__max_array_size = maxArraySize
        self.__mail_user = mail_user
        self.__accountinggroup = accountinggroup
        self.__merge_time = mergeTime
        self.__sgl_container = "%s/%s" % (SINGULARITY_DIR, singularity_image)
        self.__run_in_container = run_in_container
        self.__exclude_nodes = exclude_nodes

        self.__cluster_control_file = ""
        #######################################################
        #   Folder structure
        #     BASEFOLDER
        #      --- LOGS/<Date>/<JobName>
        #      --- TMP/<Date>/<JobName>
        #           --- BUILD
        #           --- CONFIG
        #      --- OUTPUT/<Date>/<JobName>
        ########################################################

    ### Exclude certain nodes from the job submission
    def excluded_nodes(self):
        return self.__exclude_nodes

    ### Overall job name
    def job_name(self):
        return self.__jobName

    ### Maximum size of a single job array.
    ### If the number of jobs exceeds the array size, the job must be split
    def max_array_size(self):
        return self.__max_array_size

    ### How many jobs from the same array are allowed to run simultanneously
    ###     Useful to protect the system against overload
    def max_running_per_array(self):
        return self.__max_current

    ### Allowed time for the merge jobs
    def merge_time(self):
        return self.__merge_time

    ### Required memory (MB) for the merge jobs
    def merge_mem(self):
        return self.__mergeMem

    ### Very basic directory to establish the folder structure mentioned above
    def base_dir(self):
        return self.__baseFolder

    ### Location of the LOGS
    def log_dir(self):
        return "%s/LOGS/%s/%s" % (self.base_dir(), self.__Today, self.job_name())

    ### Temporary directory. Used for job configuration files, temporary builds
    ### and also intermediate job files
    def tmp_dir(self):
        return "%s/TMP/%s/%s" % (self.base_dir(), self.__Today, self.job_name())

    ### Location of the directory where the TestArea is copied to and then compiled by
    ### the build job
    def build_dir(self):
        return "%s/BUILD" % (self.tmp_dir())

    ### Location of the directory where the configuration files are saved
    def config_dir(self):
        return "%s/CONFIG" % (self.tmp_dir())

    ### Location of the directory containing the final analysis files
    def out_dir(self):
        return "%s/OUTPUT/%s/%s" % (self.base_dir(), self.__Today, self.job_name())

    ### Specified E_mail address to send mails to the user on specific progress actions (Usually failures)
    def mail_user(self):
        return self.__mail_user

    ### Name of the group paying the budget for the job (Used at DESY sites)
    def accountinggroup(self):
        return self.__accountinggroup

    ### Copy a given file to the config directory unde a random hash. If the same file
    ### is given twice to the method it is also copied twice as no link-connection between the files
    ### is stored
    def link_to_copy_area(self, config_file):
        config_path = ResolvePath(config_file)
        if not config_path: return None
        ### Create the directory
        CreateDirectory(self.config_dir(), False)
        ### Keep the ending of the file but rename it to a random thing
        final_path = "%s/%s.%s" % (self.config_dir(), id_generator(99), config_file[config_file.rfind(".") + 1:])
        os.system("cp %s %s" % (config_path, final_path))
        os.system("chmod 0700 %s " % final_path)
        return final_path

    ### Check if a job has already been submitted under the same name
    def submit_hook(self):
        if os.path.exists("%s/.job.lck" % (self.tmp_dir())):
            logging.error("<submit_build_job>: The build job cannot be submitted since there is already a job running %s on the cluster" %
                          (self.job_name()))
            return False
        #### Do not overwrite existing output
        if os.path.exists("%s/.job.lck" % (self.out_dir())):
            logging.error("<submit_build_job>: The job has been finished %s and cannot be resubmitted again" % (self.job_name()))
            return False
        return True

    ### Check if the build job is planned to be submitted or not
    def send_build_job(self):
        return self.__submit_build

    ### What is the name of the singularity container
    def singularity_container(self):
        return self.__sgl_container

    #### write a file to be sourced inside the singularity container to
    #### pass the environment variables of the job
    def write_ship_file(self, env_vars):
        ship_file_name = WriteList(["#!/bin/bash"] + ["export %s='%s'" % (var, val) for var, val in env_vars + self.common_env_vars()],
                                   "%s/%s.sh" % (self.config_dir(), id_generator(74)))
        os.system("chmod 0700 %s" % (ship_file_name))
        return ship_file_name

    ### Will the job run inside a singularity container?
    def run_singularity(self):
        return self.__run_in_container

    ### Assemble all required environment variables to a shell script
    ### The primary script is copied to the cluster and connected with the variable export
    ###    --- env_vars: List of variables encoded in a tuple [ (var_1, val_1), (var2, vale_2)]
    ###    --- script: Location of the script to be used for the job.
    def pack_environment(self, env_vars, script):
        exec_script = self.link_to_copy_area(script)
        if not exec_script: return False
        ship_file = self.write_ship_file(env_vars)
        if self.run_singularity():
            ship_file = self.write_ship_file([
                ("CONTAINER_SCRIPT", exec_script),
                ("CONTAINER_IMAGE", self.singularity_container()),
                ("CONTAINER_SHIPING_FILE", ship_file),
            ])
            exec_script = self.link_to_copy_area(ResolvePath("ClusterSubmission/Singularity.sh"))

        env_script = WriteList(["#!/bin/bash", "source %s" %
                                (ship_file), "source %s" % (exec_script)], "%s/EnvScript_%s.sh" % (self.config_dir(), id_generator(50)))
        os.system("chmod 0700 %s" % (env_script))
        return env_script

    ### How many cores can be used to compile the environment on the cluster
    def get_build_cores(self):
        return self.__buildCores

    ### Submits the build job in which the test area is copied to the cluster and recompiled there
    ### to avoid any clashes between current developments and running jobs
    def submit_build_job(self):
        if self.check_submitted_build():
            logging.warning("<submit_build_job>: Build job is already submitted")
            return True
        if not self.submit_hook(): return False
        ### Few cluster engines go crazy if the log files of the own jobs are deleted
        ### Make sure that the build job deletes the log dir before submission
        if not CreateDirectory(self.log_dir(), True): return False
        if self.send_build_job() and not self.submit_job(script="ClusterSubmission/Build.sh",
                                                         sub_job="Build",
                                                         mem=self.get_build_mem(),
                                                         n_cores=self.get_build_cores(),
                                                         env_vars=[("CleanOut", self.out_dir()), ("CleanTmp", self.tmp_dir()),
                                                                   ("nCoresToUse", self.get_build_cores()), ("COPYAREA", self.build_dir())],
                                                         run_time=self.get_build_time(),
                                                         hold_jobs=self.get_build_hold_jobs()):
            return False
        elif not self.send_build_job():
            if not CreateDirectory(self.out_dir(), False): return False
            Dummy_Job = WriteList(
                ["#!/bin/bash", "echo \"I'm a dummy build job. Will wait 15 seconds until everything is scheduled\"", "sleep 15"],
                "%s/%s.sh" % (self.config_dir(), id_generator(35)))
            if not self.submit_job(script=Dummy_Job, sub_job="Build", mem=100, run_time="00:05:00", hold_jobs=self.__holdBuild):
                return False

        self.__submitted_build = True
        self.lock_area()
        return True

    ### Is the build job submitted?
    def check_submitted_build(self):
        return self.__submitted_build

    ### Set the internal flag that the build job has been submitted
    def submitted_build(self, submitted=False):
        self.__submitted_build = submitted

    ### How much memory to allocate for the build job
    def get_build_mem(self):
        return self.__buildMem

    ### How long is the build job allowed to last?
    def get_build_time(self):
        return self.__buildTime

    ### Which are the dependencies of the build job
    def get_build_hold_jobs(self):
        return self.__holdBuild

    ### Write a file which blocks the submission of following jobs with the same
    ### name at the same day
    def lock_area(self):
        WriteList(["###Hook file to prevent double submission of the same job"], "%s/.job.lck" % (self.tmp_dir()))

    ### Send a job which removes (temporary) files from the system
    ###  --- hold_jobs: Job names on which the job shall wait
    ###  --- to_clean: list of files to be removed
    ###  --- sub_job: Optional sub_job name to identify the job later
    def submit_clean_job(self, hold_jobs=[], to_clean=[], sub_job=""):
        clean_cfg = "%s/Clean_%s.txt" % (self.config_dir(), id_generator(35))
        WriteList(to_clean, clean_cfg)
        return self.submit_job(script="ClusterSubmission/Clean.sh",
                               mem=100,
                               env_vars=[("ToClean", clean_cfg)],
                               hold_jobs=hold_jobs,
                               sub_job="Clean%s%s" % ("" if len(sub_job) == 0 else "-", sub_job),
                               run_time="01:00:00")

    ### Send a job which copies files to certain destination
    ###    --- hold_jobs: Job names on which the job must wait
    ###    --- to_copy: List of files to copy
    ###    --- destination: TO which folder the files are copied to
    ###    --- source_dir (Needed if to_copy is empty): Simply copy a directory to another
    def submit_copy_job(self, hold_jobs=[], to_copy=[], destination="", source_dir="", sub_job=""):
        copy_cfg = ""
        if len(to_copy) > 0:
            copy_cfg = "%s/Copy_%s.txt" % (self.config_dir(), id_generator(35))
            WriteList(to_copy, copy_cfg)
        elif len(source_dir) > 0:
            copy_cfg = source_dir
        else:
            logging.error("<submit_copy_job> Nothing to copy")
            return False
        if len(destination) == 0:
            logging.error("<submit_copy_job> No destination where to copy provided")
            return False
        return self.submit_job(script="ClusterSubmission/Copy.sh",
                               mem=100,
                               env_vars=[
                                   ("DestinationDir", destination),
                                   ("FromDir", copy_cfg),
                               ],
                               hold_jobs=hold_jobs,
                               sub_job="Copy%s%s" % ("" if len(sub_job) == 0 else "-", sub_job),
                               run_time="01:00:00")

    ### Send a job which moves files o certain destination
    ###    --- hold_jobs: Job names on which the job must wait
    ###    --- to_move: List of files to move
    ###    --- destination: TO which folder the files are copied to
    ###    --- source_dir (Needed if to_move is empty): Simply move a directory into another
    def submit_move_job(self, hold_jobs=[], to_move=[], destination="", source_dir="", sub_job=""):
        move_cfg = ""
        if len(to_move) > 0:
            move_cfg = "%s/Move_%s.txt" % (self.config_dir(), id_generator(35))
            WriteList(to_move, move_cfg)
        elif len(source_dir) > 0:
            move_cfg = source_dir
        else:
            logging.error("<submit_move_job> Nothing to move")
            return False
        if len(destination) == 0:
            logging.error("<submit_move_job> No destination where to move provided")
            return False
        return self.submit_job(script="ClusterSubmission/Move.sh",
                               mem=100,
                               env_vars=[
                                   ("DestinationDir", destination),
                                   ("FromDir", move_cfg),
                               ],
                               hold_jobs=hold_jobs,
                               sub_job="Move%s%s" % ("" if len(sub_job) == 0 else "-", sub_job),
                               run_time="01:00:00")

    ### Sends the final clean job together with the copy job to maintain the overwrite protection
    ### for other jobs with the same name
    ###     --- hold_jobs: List of jobs on which the job shall hold
    def submit_clean_all(self, hold_jobs=[]):
        if not self.submit_copy_job(
                hold_jobs=hold_jobs, to_copy=["%s/.job.lck" % (self.tmp_dir())], destination=self.out_dir(), sub_job="LCK"):
            return False
        return self.submit_clean_job(hold_jobs=[self.subjob_name("Copy-LCK")], to_clean=[self.tmp_dir()])

    #### Basic method to submit a job
    ####  --- script: bash_script to execute by the cluster engine
    ####  --- sub_job: Sub-name of the particular job
    ####  --- mem: Required memory in MB
    ####  --- env_vars: Enrionmental variables required to configure each task
    ####  --- hold_jobs: List of jobs on which this job shall wait before start
    ####  --- run_time <HH:MM:SS>: How long can the job run at maximum
    ####  --- n_cores: Number of cores in use
    def submit_job(self, script, sub_job="", mem=-1, env_vars=[], hold_jobs=[], run_time="", n_cores=1):
        logging.error("<submit_job>: This is a dummy function. Please replace this by the proper engine command")
        return False

    #### Basic method to submit a job array
    ####  --- script: bash_script to execute by the cluster engine
    ####  --- sub_job: Sub-name of the particular job
    ####  --- mem: Required memory in MB
    ####  --- env_vars: Enrionmental variables required to configure each task
    ####  --- hold_jobs: List of jobs on which this job shall wait before start
    ####  --- run_time <HH:MM:SS>: How long can the job run at maximum
    ####  --- n_cores: Number of cores in use
    ####  --- array_size: How many tasks will be spawned
    def submit_array(self, script, sub_job="", mem=-1, env_vars=[], hold_jobs=[], run_time="", n_cores=1, array_size=-1):
        logging.error("<submit_array>: This is a dummy function. Please replace this by the proper engine command")
        return False

    ### Convert the sub_job names to a global job name
    def subjob_name(self, sub_job=""):
        return self.job_name() + ("" if len(sub_job) == 0 else "_") + sub_job

    ### Returns the environment variables common to all jobs such as AtlasRelease and BuildArea
    def common_env_vars(self):
        ath_version = ATLASVERSION if ATLASRELEASETYPE == "stable" else ATLASBUILDSTAMP

        common_vars = [("OriginalProject", ATLASPROJECT), ("OriginalVersion", ath_version), ("OriginalPatch", ATLASBUILDBRANCH),
                       ("Name", self.job_name())]
        ### submit_build_job not yet called
        if not self.__submitted_build:
            common_vars += [("OriginalArea", TESTAREA)]
            ### Build submitted
        elif self.send_build_job():
            common_vars += [("OriginalArea", os.path.join(self.build_dir(), "source"))]
            ### Testarea is setup in a source dir
        elif TESTAREA.endswith("source"):
            common_vars += [("OriginalArea", TESTAREA)]
            ### Testarea is setup in topdirectory
        else:
            common_vars += [("OriginalArea", os.path.join(TESTAREA, "source"))]

        if len(self.__cluster_control_file) > 0:
            common_vars += [("ClusterControlModule", self.__cluster_control_file)]
        return common_vars

    ### Helper method to parse a shell script sourced at
    ###   the beginning of the  Cluster jobs to interface the
    ###   --- job_id via get_job_ID()
    ###   --- task_id via get_task_ID()
    ###   --- send_to_failure via send_to_failure()
    def set_cluster_control_module(self, control_location):
        if len(self.__cluster_control_file):
            logging.debug("Cluster control module has already been set")
            return
        file_loc = ResolvePath(control_location)
        if not file_loc:
            logging.error("Could not set the cluster control module")
            return
        logging.info("Set the Cluster control module to %s" % (control_location))
        self.__cluster_control_file = self.link_to_copy_area(file_loc)

    ### Ensures that the jobs always wait on the build job
    def to_hold(self, hold_jobs):
        return [h for h in hold_jobs] + ([] if not self.__submitted_build else [self.subjob_name("Build")])

    ### Creates an instance to merge temporary analysis jobs
    ###     --- out_name: Final name of the merged file
    ###     --- files_to_merge: List of files to be merged together
    ###     --- files_per_job: How many files shall be handled in the first merge job:
    ###     --- hold_jobs: Names of the jobs on which the merge job shall wait
    ###     --- final_split: How many files shall remain at maximum after the merge
    def create_merge_interface(self, out_name="", files_to_merge=[], files_per_job=10, hold_jobs=[], final_split=1, shuffle_files=True):
        return MergeSubmit(outFileName=out_name,
                           files_to_merge=files_to_merge,
                           hold_jobs=hold_jobs,
                           cluster_engine=self,
                           files_per_job=files_per_job,
                           shuffle_files=shuffle_files,
                           final_split=final_split)

    ### Method sending the final clean-up and in the case of the local enginge
    ### starting the scheduling of the jobs
    def finish(self):
        return True

    def print_banner(self):
        print("#####################################################################################################")
        print("                        ClusterEngine for job %s " % self.__jobName)
        print("#####################################################################################################")
        prettyPrint("JobName", self.job_name())
        prettyPrint("LogIdr", self.log_dir())
        prettyPrint("BuildDir", self.build_dir())
        prettyPrint("TmpDir", self.tmp_dir())
        prettyPrint("outputDir", self.out_dir())


###########################################################################
### Class to handle the iterative merging of                             ##
### files via the hadd command on the cluster                            ##
###########################################################################
###  --- outFileName: Name of the final file
###  --- files_to_merge: List of root files to use for the merging
###  --- cluster_engine: Instance of the cluster engine object in charge
###  --- files_per_job: How many files shall be merged in a single task
###  --- final_split: Merge the files into N output files
###  --- shuffle_files: Shuffle the list before merging
###########################################################################
class MergeSubmit(object):
    def __init__(self,
                 outFileName="",
                 files_to_merge=[],
                 hold_jobs=[],
                 cluster_engine=None,
                 files_per_job=5,
                 final_split=1,
                 shuffle_files=True):
        self.__out_name = outFileName
        self.__shuffle_files = shuffle_files
        self.__cluster_engine = cluster_engine
        self.__hold_jobs = [h for h in hold_jobs]
        self.__files_per_job = files_per_job if files_per_job > 1 else 2
        self.__merge_lists = self.__assemble_merge_list(files_to_merge)
        self.__tmp_out_files = []
        self.__child_job = None
        self.__parent_job = None
        self.__submitted = False

        if len(self.__merge_lists) > final_split:
            self.__tmp_out_files = ["%s/%s.root" % (self.engine().tmp_dir(), id_generator(100)) for d in range(len(self.__merge_lists))]
            self.__child_job = self.create_merge_interface(final_split=final_split)
            self.__child_job.set_parent(self)
        elif final_split == 1 or len(self.__merge_lists) == 1:
            CreateDirectory(self.engine().out_dir(), False)
            self.__tmp_out_files = ["%s/%s.root" % (self.engine().out_dir(), self.outFileName())]
        else:
            CreateDirectory(self.engine().out_dir(), False)
            self.__tmp_out_files = [
                "%s/%s_%d.root" % (self.engine().out_dir(), self.outFileName(), i + 1)
                for i in range(min(final_split, len(self.__merge_lists)))
            ]

    # How many files per merge task
    def files_per_job(self):
        return self.__files_per_job

    ### In cases more than onf merge step is required what are the file names of the intermediate jobs
    def temporary_files(self):
        return self.__tmp_out_files

    ### Locations of the config files assigining the input files per task
    def merge_lists(self):
        return self.__merge_lists

    ### On which task shall the particular merge job wait
    def hold_jobs(self):
        return self.__hold_jobs if not self.parent() else [self.engine().subjob_name(self.parent().job_name())]

    ### Create another layer of merge jobs to execute the next stage of merging
    def create_merge_interface(self, final_split=1):
        return self.engine().create_merge_interface(out_name=self.outFileName(),
                                                    files_to_merge=self.temporary_files(),
                                                    files_per_job=int(self.files_per_job() / 2),
                                                    final_split=final_split,
                                                    shuffle_files=self.__shuffle_files)

    ### Instance of the holding cluster engine
    def engine(self):
        return self.__cluster_engine

    ### Name of the final file
    def outFileName(self):
        return self.__out_name

    ### How many layers of merge jobs are defined
    def childs_in_chain(self):
        if not self.__child_job: return 0
        return 1 + self.__child_job.childs_in_chain()

    ### Get the instance of the next merge layer
    def child(self):
        return self.__child_job

    ### Get the instance of the previous merge layer
    def parent(self):
        return self.__parent_job

    ### Set the previous merge layer
    def set_parent(self, parent):
        self.__parent_job = parent

    ### Sub-job name of the merge job
    def job_name(self):
        if self.__child_job:
            return "MergeLvl_%d-%s" % (self.childs_in_chain(), self.outFileName())
        return "merge-%s" % (self.outFileName())

    def __assemble_merge_list(self, files_to_merge):
        copied_in = [x for x in files_to_merge]
        if self.__shuffle_files: shuffle(copied_in)
        merge_lists = []
        merge_in = []
        for i, fi in enumerate(copied_in):
            if i > 0 and i % self.__files_per_job == 0:
                merge_name = "%s/%s.txt" % (self.engine().config_dir(), id_generator(85))
                WriteList(merge_in, merge_name)
                merge_lists += [merge_name]
                merge_in = []
            merge_in += [fi]

        ### Pack the last remenants into a last merge job
        if len(merge_in) > 0:
            merge_name = "%s/%s.txt" % (self.engine().config_dir(), id_generator(85))
            WriteList(merge_in, merge_name)
            merge_lists += [merge_name]
        return merge_lists

    ### Schedule the job on the cluster
    def submit_job(self):
        if self.__submitted: return False
        job_array = WriteList(self.merge_lists(), "%s/%s.txt" % (self.engine().config_dir(), id_generator(31)))
        final_merge_name = WriteList(self.temporary_files(), "%s/%s.txt" % (self.engine().config_dir(), id_generator(30)))
        if not self.engine().submit_array(script="ClusterSubmission/Merge.sh",
                                          sub_job=self.job_name(),
                                          mem=self.engine().merge_mem(),
                                          env_vars=[
                                              ("JobConfigList", job_array),
                                              ("OutFileList", final_merge_name),
                                              ("ALRB_rootVersion", ROOTVERSION),
                                          ],
                                          hold_jobs=self.hold_jobs(),
                                          run_time=self.engine().merge_time(),
                                          array_size=len(self.merge_lists())):
            return False
        self.__submitted = True
        if not self.child(): return True
        if not self.child().submit_job(): return False
        return self.engine().submit_clean_job(hold_jobs=[self.engine().subjob_name(self.child().job_name())],
                                              to_clean=self.temporary_files(),
                                              sub_job=self.job_name())
