#! /usr/bin/env python
from ClusterSubmission.Utils import ReadListFromFile, setup_engine, setupBatchSubmitArgParser
import argparse
import logging
logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)


def setupScriptSubmitParser():
    parser = setupBatchSubmitArgParser()
    parser.add_argument("--ListOfCmds", help="Text file where all commands are listed", required=True)
    parser.add_argument('--HoldJob', default=[], nargs="+", help='Specfiy job names which should be finished before your job is starting. ')
    parser.add_argument('--RunTime', help='Changes the RunTime of the analysis Jobs', default='07:59:59')
    parser.add_argument('--vmem', help='Changes the virtual memory needed by each jobs', type=int, default=2000)
    return parser


def main():
    Options = setupScriptSubmitParser().parse_args()
    submit_engine = setup_engine(Options)

    list_of_cmds = submit_engine.link_to_copy_area(Options.ListOfCmds)

    if not list_of_cmds:
        logging.error("Please give a valid file with list of commands to execute")
        exit(1)

    if not submit_engine.submit_build_job():
        logging.error("Submission failed")
        exit(1)
    submit_engine.submit_array(script="ClusterSubmission/Run.sh",
                               mem=Options.vmem,
                               env_vars=[("ListOfCmds", list_of_cmds)],
                               hold_jobs=Options.HoldJob,
                               run_time=Options.RunTime,
                               array_size=len(ReadListFromFile(list_of_cmds)))
    submit_engine.submit_clean_all(hold_jobs=[submit_engine.job_name()])
    submit_engine.finish()


if __name__ == '__main__':
    main()
