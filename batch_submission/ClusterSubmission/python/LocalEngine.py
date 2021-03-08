#! /usr/bin/env python
from ClusterSubmission.ClusterEngine import ClusterEngine
from ClusterSubmission.Utils import CreateDirectory, ResolvePath, id_generator, getRunningThreads, ReadListFromFile, WriteList, AppendToList
import os, time, threading, logging
logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)


###############################################################
##              LocalEngine
## the local engine manages the local submission of the jobs
## i.e. many threads are started in parallel
###############################################################
class LocalEngine(ClusterEngine):
    def __init__(self, jobName="", baseDir="", maxCurrentJobs=-1, singularity_image="", run_in_container=False):
        ClusterEngine.__init__(self,
                               jobName=jobName,
                               baseDir=baseDir,
                               maxCurrentJobs=maxCurrentJobs,
                               submit_build=run_in_container,
                               singularity_image=singularity_image,
                               run_in_container=run_in_container)
        self.__threads = []
        self.__runned_jobs = 0

    def get_threads(self):
        return self.__threads

    def n_threads(self):
        return len(self.get_threads())

    def get_array_size(self, task_name=""):
        return len([x for x in self.get_threads() if x.name() == task_name])

    #### Basic method to submit a job
    def submit_job(self, script, sub_job="", mem=-1, env_vars=[], hold_jobs=[], run_time=""):
        ### Memory is senseless in this setup. Do not pipe it further
        pending_threads, direct_pending = self.get_holding_threads(hold_jobs=hold_jobs)
        exec_script = self.pack_environment(env_vars, script)
        if not exec_script: return False
        self.set_cluster_control_module("ClusterSubmission/ClusterControlLOCAL.sh")
        self.__threads += [
            LocalClusterThread(thread_name=self.subjob_name(sub_job),
                               subthread=-1,
                               thread_engine=self,
                               dependencies=pending_threads + [th for th in direct_pending if th.thread_number() == i + 1],
                               script_exec=exec_script)
        ]
        return True

    def get_holding_threads(self, hold_jobs=[]):
        pending_threads = []
        direct_pending = []
        for th in self.get_threads():
            for hold in self.to_hold(hold_jobs):
                ### Simple name matching
                if isinstance(hold, str):
                    if hold == th.name():
                        pending_threads += [th]
                        break
                elif isinstance(hold, tuple):
                    if hold[0] == th.name() and th.thread_number() > 0:
                        if isinstance(hold[1], list):
                            if th.thread_number() in hold[1]:
                                pending_threads += [th]
                                break
                            elif -1 in hold[1]:
                                direct_pending += [th]
                        elif isinstance(hold[1], int) and -1 == hold[1]:
                            direct_pending += [th]
        return pending_threads, direct_pending

    def submit_array(self, script, sub_job="", mem=-1, env_vars=[], hold_jobs=[], run_time="", n_cores=1, array_size=-1):
        if array_size < 1:
            logging.error("<submit_array>: Please give a valid array size")
            return False
        pending_threads, direct_pending = self.get_holding_threads(hold_jobs=hold_jobs)
        exec_script = self.pack_environment(env_vars, script)
        if not exec_script: return False
        self.set_cluster_control_module("ClusterSubmission/ClusterControlLOCAL.sh")
        for i in range(array_size):
            self.__threads += [
                LocalClusterThread(thread_name=self.subjob_name(sub_job),
                                   subthread=i + 1 if array_size > 0 else -1,
                                   thread_engine=self,
                                   dependencies=pending_threads + [th for th in direct_pending if th.thread_number() == i + 1],
                                   script_exec=exec_script)
            ]
        return True

    def print_status(self, running):
        logging.info("<LocalEngine>: Executed %d/%d jobs at the moment %d jobs are running" %
                     (self.__runned_jobs, self.n_threads(), len(running)))
        for th in running:
            if not th.isAlive(): return
            th.print_log_file()
            time.sleep(0.2)

    def finish(self):
        CreateDirectory(self.log_dir(), False)
        CreateDirectory(self.tmp_dir(), False)
        executable = [th for th in self.get_threads() if th.is_launchable()]
        running = []
        dead_jobs = []

        cycles = 0
        ### There are still some jobs to execute
        while self.__runned_jobs + len(dead_jobs) < self.n_threads():
            cycles += 1
            running = [th for th in running if th.isAlive()]
            if len(running) < self.max_running_per_array():
                for th in executable:
                    if len(running) >= self.max_running_per_array(): break
                    th.start()
                    self.__runned_jobs += 1
                    running += [th]
                executable = [th for th in self.get_threads() if th.is_launchable()]
            else:
                dead_jobs += [th for th in self.get_threads() if th.is_dead() or th.in_dead_chain() and not th in dead_jobs]
            time.sleep(1)
            if cycles % 120 == 0:
                self.print_status(running)

        while getRunningThreads(running) > 0:
            time.sleep(0.5)
            cycles += 1
            if cycles % 120 == 0:
                self.print_status(running)

        ### Need to do something with the dead jobs. At least inform the user


class LocalClusterThread(threading.Thread):
    def __init__(self, thread_name="", subthread=-1, thread_engine=None, dependencies=[], script_exec=""):
        threading.Thread.__init__(self)
        self.__engine = thread_engine
        self.__name = thread_name
        self.__sub_num = subthread

        self.__isSuccess = False
        self.__started = False
        self.__dependencies = [d for d in dependencies]
        self.__script_to_exe = script_exec
        self.__tmp_dir = "%s/%s" % (thread_engine.tmp_dir(), id_generator(50))
        CreateDirectory(self.__tmp_dir, True)
        self.__env_vars = [("LOCAL_TASK_ID", "%d" % (self.thread_number())), ("TMPDIR", self.__tmp_dir)]

    def __del__(self):
        logging.info("<LocalClusterThread>: Clean up %s" % (self.__tmp_dir))
        os.system("rm -rf %s" % (self.__tmp_dir))

    def dependencies(self):
        return self.__dependencies

    def thread_engine(self):
        return self.__engine

    def thread_number(self):
        return self.__sub_num

    def name(self):
        return self.__name

    def is_launchable(self):
        if self.isAlive() or self.__started or self.in_dead_chain(): return False
        self.__dependencies = [th for th in self.__dependencies if th.isAlive() or not th.is_started() or not th.is_success()]
        return len(self.__dependencies) == 0

    def in_dead_chain(self):
        return len([th for th in self.__dependencies if th.is_dead() or th.in_dead_chain()]) > 0

    def is_dead(self):
        return not self.isAlive() and self.is_started() and not self.is_success()

    def is_success(self):
        return self.__isSuccess

    def is_started(self):
        return self.__started

    def run(self):
        self.__started = True
        ###################
        self.__isSuccess = self._cmd_exec()

    def log_file(self):
        return "%s/%s%s.log" % (self.thread_engine().log_dir(), self.name(), "" if self.thread_number() < 1 else "_%d" %
                                (self.thread_number()))

    def print_log_file(self, last_lines=10):
        if not os.path.exists(self.log_file()): return
        log_content = ReadListFromFile(self.log_file())
        n_lines = len(log_content)
        for i in range(max(0, n_lines - last_lines), n_lines):
            if self.thread_number() == -1:
                logging.info("<%s> %s" % (self.name(), log_content[i]))
            else:
                logging.info(
                    "<%s - %d/%d> %s" %
                    (self.name(), self.thread_number(), self.thread_engine().get_array_size(task_name=self.name()), log_content[i]))

    def _cmd_exec(self):
        if not os.path.exists(self.__script_to_exe):
            logging.error("<_cmd_exec>: Could not find %s" % (self.__script_to_exe))
            return False
        ### Threads can set their own enviroment variables without affecting the others
        os.system("chmod 0700 %s" % (self.__script_to_exe))
        if self.thread_number() == -1:
            logging.info("<_cmd_exec> Start job %s" % (self.name()))
        else:
            logging.info("<_cmd_exec> Start task %d/%d in job %s" %
                         (self.thread_number(), self.thread_engine().get_array_size(task_name=self.name()), self.name()))
        cmd_file = self.thread_engine().pack_environment(env_vars=self.__env_vars, script=self.__script_to_exe)

        return os.system("python %s --Cmd %s > %s  2>&1" % (ResolvePath("ClusterSubmission/exeScript.py"), cmd_file, self.log_file())) == 0
