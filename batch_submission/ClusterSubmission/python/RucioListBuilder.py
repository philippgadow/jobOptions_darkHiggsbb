#! /usr/bin/env python
from ClusterSubmission.Utils import WriteList, ClearFromDuplicates, CreateDirectory, ExecuteCommands, get_action_from_parser, setupBatchSubmitArgParser, id_generator, CheckRucioSetup, CheckRemainingProxyTime, getGmdOutput
from ClusterSubmission.ListDisk import RUCIO_ACCOUNT, RUCIO_RSE, ListDisk
from ClusterSubmission.ClusterEngine import SINGULARITY_DIR
import os, logging, argparse

logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)


def GetDataSetFiles(dsname, RSE, protocols):
    logging.info("Get the files of the dataset %s at %s" % (dsname, RSE))
    logging.info("Issuing command: rucio list-file-replicas --protocols %s --rse %s %s " % (protocols, RSE, dsname))
    DSReplicas = getGmdOutput("rucio list-file-replicas --protocols %s --rse %s %s " % (protocols, RSE, dsname))
    DS = []
    for line in DSReplicas:
        Entry = None
        LineInfo = line.split()
        for i, column in enumerate(LineInfo):
            if RSE in column:
                try:
                    Entry = LineInfo[i + 1]
                    break
                except:
                    logging.warning("There was some strange noise here ", column)
                    pass
        if Entry:
            logging.info("Entry: " + Entry)
            ReplacePath = os.getenv("CLSUB_RUCIOREPLACEPATH")
            LocalPath = os.getenv("CLSUB_RUCIOLOCALPATH")
            if ReplacePath and LocalPath:
                Entry = Entry.replace(ReplacePath, LocalPath)
            DS.append(Entry)
    return DS


def downloadDataSets(InputDatasets, Destination, RSE="", use_singularity=False):
    ### Apparently rucio does no longer work in combination with AthAnalysis. So let's
    ### execute it from a singulartity container
    Cmds = []
    image_to_choose = setupBatchSubmitArgParser().get_default("SingularityImage")
    home_dir = setupBatchSubmitArgParser().get_default("BaseFolder") + "/TMP/.singularity/"
    CreateDirectory(Destination, False)
    if use_singularity:
        CreateDirectory(home_dir, False)
    to_clean = []
    for DS in InputDatasets:
        if not use_singularity:
            Cmds += ["rucio download %s --ndownloader 32 %s --dir %s" % (DS, "" if len(RSE) == 0 else "--rse %s" % (RSE), Destination)]
        else:
            singularity_dir = home_dir + "/" + id_generator(21)
            to_clean += [singularity_dir]
            singularity_script = WriteList([
                "#!/bin/bash",
                "export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase",
                "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh",
                "lsetup rucio",
                "echo 'rucio download %s --ndownloader 32 %s --dir %s'" % (DS, "" if len(RSE) == 0 else "--rse %s" % (RSE), Destination),
                "rucio download %s --ndownloader 32 %s --dir %s" % (DS, "" if len(RSE) == 0 else "--rse %s" % (RSE), Destination),
            ], "%s/to_exec.sh" % (singularity_dir))
            os.system("chmod 0777 " + singularity_script)
            Cmds += [
                "singularity exec --cleanenv -H %s:/alrb -B %s:/srv  %s/%s %s" %
                (singularity_dir, Destination, SINGULARITY_DIR, image_to_choose, singularity_script)
            ]
    ExecuteCommands(ListOfCmds=Cmds, MaxCurrent=8)

    for c in to_clean:
        os.system("rm -rf %s" % (c))


def GetScopes(select_user=False, select_group=False, select_official=False):
    logging.info("Reading in the scopes:")
    Scopes = getGmdOutput("rucio list-scopes")
    ScopeList = ClearFromDuplicates([
        Entry for Entry in Scopes
        if (select_user == True and Entry.find("user") != -1) or (select_group == True and Entry.find("group") != -1) or (
            select_official == True and Entry.find("user") == -1 and Entry.find("group") == -1)
    ])
    logging.info("Done found %d scopes" % (len(ScopeList)))
    return ScopeList


def createFileList(dsname, options):
    logging.info('Creating file list for ' + dsname)
    DS = GetDataSetFiles(dsname, options.RSE, options.protocols)
    if len(DS) == 0:
        logging.error("No datasets found")
        return
    if dsname.find(":") > -1:
        dsname = dsname[dsname.find(":") + 1:len(dsname)]
    CreateDirectory(options.OutDir, False)
    filelistname = options.OutDir + "/" + dsname.rstrip('/') + ".txt"
    if os.path.exists(filelistname) == True:
        logging.info("Remove the old FileList")
        os.system("rm " + filelistname)
    WriteList(DS, filelistname)


def getArgumentParser():
    """Get arguments from command line."""
    OutDir = os.getcwd()
    parser = argparse.ArgumentParser(
        description=
        'This script creates lists with datasets located at a RSE location. Futher patterns to find or exclude can be specified.',
        prog='RucioListBuilder',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--dataset',
                        '-d',
                        '-l',
                        '-D',
                        '-L',
                        help='Dataset to collect data for, or filename containing list of datasets',
                        default='')
    parser.add_argument('-o', '-O', '--OutDir', help='specify output directory to put file list(s) into', default=OutDir)
    parser.add_argument('--single_out_file', help='Pipe all paths into a single file', default=False, action='store_true')
    parser.add_argument('--out_file_name', help='Specify the file-name', default="")

    parser.add_argument('-r', '-R', '--RSE', '--rse', help='specify a RSE', default=RUCIO_RSE)
    parser.add_argument('-p',
                        '-P',
                        '--protocols',
                        help="Specify the protocols you want to use for the file list creation. Default: 'dcap'",
                        default="root")
    return parser


def main():
    CheckRucioSetup()
    CheckRemainingProxyTime()
    """"""
    RunOptions = getArgumentParser().parse_args()

    all_files = []
    if RunOptions.single_out_file and len(RunOptions.out_file_name) == 0:
        logging.error("Please provide a file name if you run with --single-out_file")
        exit(1)
    # Do we have one dataset, or a file with a list of them?
    if os.path.exists(RunOptions.dataset):
        with open(RunOptions.dataset) as dsfile:
            for line in dsfile:
                # Ignore comment lines and empty lines
                if line.startswith('#'): continue
                realline = line.strip()
                if realline.find("_tid") > -1: realline = realline[0:realline.find("_tid")]
                if not realline: continue  # Ignore whitespace

                if not RunOptions.single_out_file:
                    createFileList(realline, RunOptions)
                else:
                    all_files += GetDataSetFiles(realline, RunOptions.RSE, RunOptions.protocols)

    else:
        createFileList(RunOptions.dataset, RunOptions)

    if len(all_files) > 0:
        WriteList(all_files, options.out_file)


if __name__ == '__main__':
    main()
