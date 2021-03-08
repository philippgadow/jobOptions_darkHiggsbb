#! /usr/bin/env python
from ClusterSubmission.Utils import ReadListFromFile, setup_engine, setupBatchSubmitArgParser, CheckRucioSetup, CheckRemainingProxyTime, IsROOTFile
from ClusterSubmission.RucioListBuilder import GetDataSetFiles, downloadDataSets
from ClusterSubmission.DatasetDownloader import DataSetFileHandler
import sys, os, argparse, commands, time, logging


def getArgumentParser():
    """Get arguments from command line."""
    parser = setupBatchSubmitArgParser()
    parser.add_argument("--fileLists", help="Specify the file lists to submit", default=[], nargs="+")
    parser.add_argument("--fileListsFolder", help="Specify a folder containing file lists", type=str, default="")
    parser.add_argument("--remainingSplit", help="Specify a remaining split of the files", default=1, type=int)
    ### Arguments to read a list of dataset files
    parser.add_argument("--RucioDSList", help="Specify a file containing a list of rucio datasets to be merged", default="")
    parser.add_argument("--RucioRSE",
                        help="Specify the RSE on which the datasets are stored. Required if download is not activated",
                        default="")
    parser.add_argument("--download", help="Download the files in advance", default=False, action='store_true')
    parser.add_argument("--batch_size", help="What is the final size of each file in the end", default=-1, type=int)

    parser.add_argument("--nFilesPerJob", help="Specify number of files per merge job", default=10, type=int)
    parser.add_argument("--HoldJob", help="Specify a list of jobs to hold on", default=[])
    parser.set_defaults(Merge_vmem=4000)
    return parser


def main():
    """Merge files from a list using the MergeClass in ClusterEngine."""
    RunOptions = getArgumentParser().parse_args()
    if RunOptions.fileListsFolder != "":
        if len(RunOptions.fileLists) > 0:
            logging.warning('You gave both a folder containing filelists and separate filelists, will merge both!')
        if not os.path.isdir(RunOptions.fileListsFolder):
            logging.error(' %s is not a directory, exiting...' % RunOptions.fileListsFolder)
            sys.exit(1)
        for l in os.listdir(RunOptions.fileListsFolder):
            if not os.path.isdir('%s/%s' % (RunOptions.fileListsFolder, l)):
                RunOptions.fileLists.append('%s/%s' % (RunOptions.fileListsFolder, l))
    submit_engine = setup_engine(RunOptions)
    merging = [
        submit_engine.create_merge_interface(out_name=L[L.rfind("/") + 1:L.rfind(".")],
                                             files_to_merge=ReadListFromFile(L),
                                             files_per_job=RunOptions.nFilesPerJob,
                                             hold_jobs=RunOptions.HoldJob,
                                             final_split=RunOptions.remainingSplit) for L in RunOptions.fileLists
    ]
    ### Rucio lists
    if len(RunOptions.RucioDSList) > 0:
        CheckRucioSetup()
        CheckRemainingProxyTime()
        #### Check that we can actually obtain the datasets
        if len(RunOptions.RucioRSE) == 0 and not RunOptions.download:
            logging.error("Please specifiy either the RSE on which the datasets are stored via --RucioRSE or activate the download option")
            exit(1)

        ds_to_merge = ReadListFromFile(RunOptions.RucioDSList)
        download_dir = submit_engine.tmp_dir() + "TMP_DOWNLOAD/"
        if RunOptions.download:
            downloadDataSets(InputDatasets=ds_to_merge, Destination=download_dir, RSE=RunOptions.RucioRSE, use_singularity=False)

        to_wait = []
        hold_jobs = []
        for ds in ds_to_merge:
            ds_name = ds[ds.find(":") + 1:]
            if RunOptions.batch_size <= 0:
                merging += [
                    submit_engine.create_merge_interface(
                        out_name=ds_name,
                        files_to_merge=GetDataSetFiles(dsname=ds, RSE=RunOptions.RucioRSE, protocols="root")
                        if not RunOptions.download else [download_dir + ds_name + "/" + x for x in os.listdir(download_dir + ds_name)],
                        files_per_job=RunOptions.nFilesPerJob,
                        hold_jobs=RunOptions.HoldJob + hold_jobs,
                        final_split=RunOptions.remainingSplit)
                ]

            else:
                merging += [
                    DataSetFileHandler(rucio_container=ds,
                                       dest_rse=RunOptions.RucioRSE,
                                       download=RunOptions.download,
                                       merge=True,
                                       download_dir=download_dir,
                                       destination_dir=submit_engine.out_dir(),
                                       cluster_engine=submit_engine,
                                       max_merged_size=RunOptions.batch_size * 1024 * 1024 * 1024,
                                       hold_jobs=RunOptions.HoldJob + hold_jobs,
                                       files_per_merge_job=2)
                ]
            to_wait += [submit_engine.subjob_name(merging[-1].job_name())]
            if len(to_wait) % 5 == 0:
                hold_jobs = [w for w in to_wait]
                to_wait = []
    for merge in merging:
        merge.submit_job()

    clean_hold = [submit_engine.subjob_name(merge.job_name()) for merge in merging]

    submit_engine.submit_clean_all(clean_hold)
    submit_engine.finish()


if __name__ == '__main__':
    main()
