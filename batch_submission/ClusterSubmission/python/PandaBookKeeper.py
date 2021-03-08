#! /usr/bin/env python
from ClusterSubmission.Utils import CheckPandaSetup, FillWhiteSpaces, WriteList, ClearFromDuplicates, cmp_to_key
from pandatools.PBookCore import PBookCore
from pandatools.queryPandaMonUtils import query_tasks
from pandatools.localSpecs import task_active_superstatus_list, task_final_superstatus_list, LocalTaskSpec

from ClusterSubmission.RucioListBuilder import GetScopes
from ClusterSubmission.ListDisk import RUCIO_ACCOUNT
import os, argparse, time, logging
from pprint import pprint
logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)


### Pbook 1.4. comes along with many new features. The drawback
### it's been erased w.r.t to previous releases. This helper class
### restores backwards compabilitiy
class PandaJobInfo(LocalTaskSpec):
    @property
    def jobName(self):
        return self.taskname

    @property
    def JobID(self):
        return self.reqid

    @property
    def taskStatus(self):
        return self.status

    @property
    def jediTaskID(self):
        return self.jeditaskid

    @property
    def inDS(self):
        return ",".join(sorted(ClearFromDuplicates([x['containername'] for x in self._fulldict['datasets'] if x['type'] == 'input'])))

    @property
    def outDS(self):
        return ",".join(sorted(ClearFromDuplicates([x['containername'] for x in self._fulldict['datasets'] if x['type'] != 'input'])))

    def scope(self):
        return self.jobName[:self.jobName.find(".", self.jobName.find(".") + 1)]

    def job_name(self):
        return self.jobName[0:self.jobName.find("/")]


def getArgumentParser():
    parser = argparse.ArgumentParser(description='This script looks up the pbook database and prints the jobs according to the status.',
                                     prog='ListDisk',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("--select_status",
                        help="Only consider jobs tin the following super status",
                        nargs="+",
                        default=[],
                        choices=task_active_superstatus_list + task_final_superstatus_list + ["exhausted", "scouting"])
    parser.add_argument("--select_scopes",
                        help="Only show jobs submitted from a certain group account",
                        nargs="+",
                        default=[],
                        choices=GetScopes(select_user=False, select_group=True) + ["user." + RUCIO_ACCOUNT])
    parser.add_argument("--job_name_pattern", help="String paterns which must be part of the jobname.", nargs="+", default=[])
    parser.add_argument("--exclude_pattern", help="String patterns which must not be in the job name", default=[], nargs="+")
    parser.add_argument("--exclude_jobids", help="JobIDs or parent jobIDs to be excluded", default=[], nargs="+", type=int)

    parser.add_argument("--sync_db", help="Synchronize the pbook database in the very begining", default=False, action='store_true')
    parser.add_argument("--clean_period", help="Erase all jobs from  the database older than n days", default=30, type=int)
    parser.add_argument("--automatic_retry", help="Retry all jobs which are not done  automatically", default=False, action="store_true")
    parser.add_argument("--broken_log_file", help="Location of the logfile to be written", default="%s/panda_failures.log" % (os.getcwd()))
    parser.add_argument("--log_file", help="Location of the logfile to be written", default="%s/panda_progress.log" % (os.getcwd()))
    parser.add_argument("--duplicated_log",
                        help="Location of all jobs which are task duplications",
                        default="%s/panda_duplication.log" % (os.getcwd()))

    parser.add_argument("--rucio", help="With this option you can set the rucio_account", default=RUCIO_ACCOUNT)
    parser.add_argument("--askapproval", help="Asks for approval of the request", default=False, action="store_true")
    parser.add_argument("--comment", help="Comment for the rucio approval", default="")
    parser.add_argument("--kill", help="Kill the jobs. Only possible if patterns are selected", default=False, action="store_true")
    parser.add_argument("--change_files_per_job", help="Change the files per job in a grid job", default=-1, type=int)
    parser.add_argument("--change_ngb_per_job", help="Change number of GB per job in a grid job", default=-1, type=int)
    return parser


def get_progress(job):
    done_jobs = int(job.nfilesfinished)
    all_jobs = int(job.nfiles)
    if all_jobs == 0: return -1, 0, 0
    proc_frac = 100. * done_jobs / all_jobs
    return proc_frac, done_jobs, all_jobs


def make_progess(job):
    proc_frac, done_jobs, all_jobs = get_progress(job)
    return "%d/%d" % (done_jobs, all_jobs)


def jedi_sorter(a, b):
    if a.jediTaskID != b.jediTaskID: return a.jediTaskID - b.jediTaskID
    bad_stat = ["broken", "aborted", "failed"]
    sub_optimal = ["scouting"]
    if a.taskStatus not in bad_stat and b.taskStatus not in bad_stat:
        return int(get_progress(b)[0] - get_progress(a)[0])
    elif a.taskStatus in bad_stat and b.taskStatus in bad_stat:
        return 0
    elif a.taskStatus in bad_stat:
        return 1
    elif b.taskStatus in bad_stat:
        return -1
    return 0


def interesting_grid_jobs(options):
    pbook_db = PBookCore()
    CheckPandaSetup()

    to_consider = []

    ts, url, data = query_tasks(username=pbook_db.username,
                                limit=2000,
                                reqid=None,
                                status=None,
                                superstatus=None,
                                taskname=None,
                                days=options.clean_period,
                                jeditaskid=None,
                                metadata=False,
                                sync=options.sync_db,
                                verbose=False)
    for task in data:
        job = PandaJobInfo(task, source_url=url, timestamp=ts)
        scope = job.scope()
        job_name = job.job_name()
        ### The last tag is added pbook to specify the container name
        if len(options.select_scopes) > 0 and not scope in options.select_scopes: continue

        if len(options.select_status) > 0 and not job.taskStatus in options.select_status: continue

        if len(options.job_name_pattern) > 0 and len([P for P in options.job_name_pattern if job_name.find(P) != -1]) != len(
                options.job_name_pattern):
            continue
        if len(options.exclude_pattern) > 0 and len([P for P in options.exclude_pattern if job_name.find(P) != -1]) > 0: continue
        if len([x for x in options.exclude_jobids if x == int(job.reqid) or x == int(job.jediTaskID)]) > 0: continue

        ### Kill productions which had some failures
        ### or misconfigurations. Be aware that the patterns
        ### must be specific otherwise everything on the grid
        ### is killed.
        if len(options.job_name_pattern) > 0 and options.kill:
            if job.taskStatus not in ["done", "broken", "failed", "aborted", "exhausted", "finished", "aborting"]:
                pbook_db.kill(job.JobID)
                logging.info("Going to kill %s." % (job_name))
        to_consider += [job]

    tmp_consider = [
        job for job in to_consider if job.taskStatus not in ["broken", "aborted"] or len([
            x for x in to_consider
            if x.jobName == job.jobName and x.jediTaskID != job.jediTaskID and x.taskStatus not in ["broken", "aborted"]
        ]) == 0
    ]
    to_consider = []

    tmp_consider.sort(key=cmp_to_key(jedi_sorter))
    ### Remove duplicates pointing to the same taskid
    for job in tmp_consider:
        if len([x for x in to_consider if x.jediTaskID == job.jediTaskID]) > 0: continue
        to_consider += [job]
        if job.taskStatus in ["running", "finished", "exhausted", "scouting", "submitting", "throttled", "aborted", "aborting"]:
            if not options.kill and options.automatic_retry and (
                    int(job.nfilesfailed) > 0 or job.taskStatus in ["aborted", "exhausted"] or options.change_files_per_job > 0
                    or options.change_ngb_per_job > 0) and job.taskStatus not in ["aborting", "scouting", "throttled"]:
                logging.info("Retry %s (%d) since it's in the %s state. %s sub jobs are done" %
                             (job.jobName, job.jediTaskID, job.taskStatus, make_progess(job)))
                new_opts = {}
                if options.change_files_per_job > 0: new_opts["nFilesPerJob"] = options.change_files_per_job
                if options.change_ngb_per_job > 0: new_opts["nGBPerJob"] = options.change_ngb_per_job
                pbook_db.retry(job.JobID, newOpts=new_opts)
            else:
                logging.info("%s subjobs of %s have already been succeeded. The job is still in the %s (%d) state." % (
                    make_progess(job),
                    job.jobName,
                    job.taskStatus,
                    job.jediTaskID,
                ))

    to_consider.sort(key=lambda x: x.jobName)
    logging.info("Found %d jobs to have a closer look at" % (len(to_consider)))
    write_status_log(options.log_file, to_consider)
    duplicated = get_duplicated_jobs(to_consider)
    if len(duplicated) > 0:
        logging.warning("Found jobs writing to the same container but having different jediTaskID")
        write_status_log(options.duplicated_log, duplicated)
    return to_consider


def get_broken_jobs(job_list):
    broken_jobs = []
    for job in job_list:
        if job.taskStatus in ["broken", "failed"] and len([
                cross for cross in job_list
                if job.jediTaskID != cross.jediTaskID and job.jobName == cross.jobName and cross.taskStatus not in ["broken", "failed"]
        ]) == 0 and len([cross for cross in broken_jobs if job.jobName == cross.jobName]) == 0:
            broken_jobs += [job]
    return broken_jobs


def write_broken_log(options, broken_jobs):
    if len(broken_jobs) > 0:
        logging.info("Found %d unhealthy jobs will prompt them below" % (len(broken_jobs)))
        max_task_letters = max([len(str(job.jediTaskID)) for job in broken_jobs])
        max_status_letters = max([len(str(job.taskStatus)) for job in broken_jobs])
        max_job_name_letters = max([len(job.jobName) for job in broken_jobs])
        log_file = []
        for job in sorted(broken_jobs, key=lambda x: x.jobName):
            log_file += [
                "https://bigpanda.cern.ch/task/%s/ %s %s %s %s %s %s" %
                (job.jediTaskID, FillWhiteSpaces(max_task_letters - len(str(job.jediTaskID))), job.taskStatus,
                 FillWhiteSpaces(max_status_letters - len(job.taskStatus)), job.jobName,
                 FillWhiteSpaces(max_job_name_letters - len(job.jobName)), job.inDS)
            ]
            logging.info(log_file[-1])
        WriteList(log_file, options.broken_log_file)


def write_status_log(file_location, grid_jobs):
    if len(grid_jobs) > 0:
        max_task_letters = max([len(str(job.jediTaskID)) for job in grid_jobs])
        max_status_letters = max([len(str(job.taskStatus)) for job in grid_jobs])
        max_job_name_letters = max([len(job.jobName) for job in grid_jobs])
        max_progress_letters = max([len(make_progess(job)) for job in grid_jobs])
        log_file = []
        for job in sorted(grid_jobs, key=lambda x: get_progress(x)[0], reverse=True):
            proc_frac, done_jobs, all_jobs = get_progress(job)
            log_file += [
                "https://bigpanda.cern.ch/task/%s/ %s %s %s %s %s %s %s(%.2f%%) %s" %
                (job.jediTaskID, FillWhiteSpaces(max_task_letters - len(str(job.jediTaskID))), job.jobName,
                 FillWhiteSpaces(max_job_name_letters - len(job.jobName)), job.taskStatus,
                 FillWhiteSpaces(max_status_letters - len(job.taskStatus)), make_progess(job),
                 FillWhiteSpaces(max_progress_letters - len(make_progess(job))), proc_frac, job.inDS)
            ]
        WriteList(log_file, file_location)


def get_duplicated_jobs(grid_jobs):
    duplicated = []
    for i in range(len(grid_jobs)):
        is_twice = len([x for x in duplicated if x.jobName == grid_jobs[i]]) > 0
        for j in range(i + 1, len(grid_jobs)):
            if grid_jobs[i].jobName == grid_jobs[j].jobName: is_twice = True
            if is_twice: break
        if is_twice: duplicated += [grid_jobs[i]]
    return duplicated


if __name__ == '__main__':
    options = getArgumentParser().parse_args()

    to_consider = interesting_grid_jobs(options)
    broken_jobs = get_broken_jobs(to_consider)
    write_broken_log(options, broken_jobs)
