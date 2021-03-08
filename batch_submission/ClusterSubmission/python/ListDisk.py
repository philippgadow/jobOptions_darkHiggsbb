#! /usr/bin/env python
from ClusterSubmission.Utils import CheckRemainingProxyTime, CheckRucioSetup, CreateDirectory, WriteList, getGmdOutput
import os, sys, argparse, time, logging
logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)

RUCIO_ACCOUNT = os.getenv("RUCIO_ACCOUNT")
RUCIO_RSE = os.getenv("RUCIO_RSE") if os.getenv("RUCIO_RSE") else ""


def ListDisk(RSE):
    if len(RSE) == 0:
        logging.error("No disk is given")
        return []

    logging.info("Read content of " + RSE)
    OnDisk = getGmdOutput("rucio list-datasets-rse " + RSE)
    MyDataSets = []
    for Candidates in OnDisk:
        if Candidates.startswith('-'): continue
        elif Candidates.startswith('SCOPE'): continue
        MyDataSets.append(Candidates)
    return MyDataSets


def ListDiskWithSize(RSE):
    if len(RSE) == 0:
        logging.error("No disk is given")
        return []
    logging.info("Read content of %s and also save the size of each dataset" % (RSE))
    OnDisk = getGmdOutput("rucio list-datasets-rse %s --long" % (RSE))
    MyDS = []
    for Candidates in OnDisk:
        try:
            DS = Candidates.split("|")[1].strip()
            Size = Candidates.split("|")[3].strip()
            Stored = float(Size[:Size.find("/")]) / 1024 / 1024 / 1024
            TotalSize = float(Size[Size.find("/") + 1:]) / 1024 / 1024 / 1024
        except:
            continue
        logging.info("%s   %s   %.2f GB" % (DS, Stored, TotalSize))
        MyDS.append((DS, TotalSize))
    return sorted(MyDS, key=lambda size: size[1], reverse=True)


def GetDataSetInfo(DS, RSE, Subscriber=None):
    Cmd = "rucio list-rules %s --csv" % (DS)
    logging.debug("Executing " + Cmd)
    Rules = getGmdOutput(Cmd)
    for i in range(len(Rules)):
        Rule = Rules[i]
        try:
            ID = Rule.split(",")[0].strip()
            Owner = Rule.split(",")[1].strip()
            RuleRSE = Rule.split(",")[4].strip()
            if RuleRSE == RSE and (Subscriber == None or Subscriber == Owner): return ID, Owner
        except:
            continue
    return None, None


def ListDataFilesWithSize(DS):
    file_list = []
    Cmd = "rucio list-files %s --csv" % (DS)
    for file_in_ds in getGmdOutput(Cmd):
        #  group.perf-muons:group.perf-muons.17916903.EXT0._000027.NTUP_MCPTP.root,6DD081BE-7CAE-4AAE-8F9C-E1E3612AA09C,8b139e60,3.893 GB,None
        #
        file_name = file_in_ds.split(",")[0]
        ## Skim away the scope
        file_name = file_name[file_name.find(":") + 1:]

        file_size_str = file_in_ds.split(",")[3]
        unit = file_size_str.split(" ")[1].replace("i", "").upper()
        file_size = float(file_size_str.split(" ")[0])
        if unit == "GB": file_size *= 1024 * 1024 * 1024
        elif unit == "MB": file_size *= 1024 * 1024
        elif unit == "KB": file_size *= 1024
        else:
            logging.error("Unkown file-size %s" % (file_size_str))
            exit(1)
        file_list += [(file_name, file_size)]
    return file_list


def GetUserRules(user):
    Cmd = "rucio list-rules --account %s --csv" % (user)
    logging.debug("Executing " + Cmd)
    OnDisk = getGmdOutput(Cmd)
    MyRules = []
    for Rule in OnDisk:
        try:
            ID = Rule.split(",")[0].strip()
            DataSet = Rule.split(",")[2].strip()
            Rule_RSE = Rule.split(",")[4].strip()
        except:
            continue
        MyRules.append((ID, DataSet, Rule_RSE))
    return MyRules


def ListUserRequests(RSE, user):
    if len(RSE) == 0:
        logging.error("No disk is given")
        return []
    logging.info("List requests of user %s at %s" % (user, RSE))
    AllRules = GetUserRules(user)
    MyDataSets = []
    for Candidates in AllRules:
        ID = Candidates[0]
        DataSet = Candidates[1]
        Rule_RSE = Candidates[2]
        if not RSE == Rule_RSE: continue
        MyDataSets.append(DataSet)
    return MyDataSets


def GetDataSetReplicas(DS):
    Cmd = "rucio list-rules %s --csv" % (DS)
    logging.debug("Executing " + Cmd)
    Replicas = []
    all_rses = getRSEs()
    for line in getGmdOutput(Cmd):
        try:
            ds_rse = line.split(",")[4].strip()
        except:
            continue
        if ds_rse in all_rses: Replicas.append(ds_rse)
    return Replicas


def getRSEs():
    Cmd = "rucio list-rses"
    logging.debug("Executing " + Cmd)
    return sorted(getGmdOutput(Cmd))


def getArgumentParser():
    """Get arguments from command line."""
    parser = argparse.ArgumentParser(
        description='This script lists datasets located at a RSE location. Further patterns to find or exclude can be specified.',
        prog='ListDisk',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-p', '-P', '--pattern', help='specify a pattern which is part of dataset name', nargs='+', default=[])
    parser.add_argument('-e', '-E', '--exclude', help='specify a pattern which must not be part of dataset name', nargs='+', default=[])
    parser.add_argument('-o', '-O', '--OutDir', help='specify output directory', default=os.getcwd())
    parser.add_argument('-r', '-R', '--RSE', '--rse', help='specify a RSE', default=RUCIO_RSE, choices=getRSEs())
    parser.add_argument('--MyRequests', help='list datasets which you requested yourself', action='store_true', default=False)
    parser.add_argument('--rucio', help='Which rucio account shall be used for MyRequests', default=RUCIO_ACCOUNT)
    return parser


def main():
    """List datasets located at a RSE location."""
    CheckRucioSetup()
    CheckRemainingProxyTime()

    RunOptions = getArgumentParser().parse_args()

    Today = time.strftime("%Y-%m-%d")
    Patterns = RunOptions.pattern
    OutDir = RunOptions.OutDir
    RSE = RunOptions.RSE
    if ',' in RSE: RSE = RSE.split(',')[0]  # in case people have more than one RSE in their environment variable for grid submits

    Prefix = ''
    if RunOptions.MyRequests:
        Prefix = 'MyRequestTo_'
        DS = ListUserRequests(RSE, RunOptions.rucio)
    else:
        DS = ListDisk(RSE)


###    MetaFile = open("Content_%s.txt"%(RSE), 'w')
###    for DataSet, Size in ListDiskWithSize(RSE):
###           Owner, ID = GetDataSetInfo(DataSet,RSE)
###           line = "%s  |   %s   | %s  | %.2f GB"%(ID, Owner,DataSet, Size)
###           MetaFile.write("%s\n"%(line))
###           print line
###    MetaFile.close()
###    exit(0)

    if len(DS) == 0:
        logging.warning("Disk is empty.")
        exit(0)
    CreateDirectory(OutDir, False)

    ###########
    #   Define the file list name
    ###########
    FileList = "%s%s_%s" % (Prefix, RSE, Today)
    if len(Patterns) > 0: FileList += "_%s" % ('_'.join(Patterns))
    if len(RunOptions.exclude) > 0: FileList += "_exl_%s" % ('_'.join(RunOptions.exclude))
    FileList += '.txt'
    Write = []
    for d in sorted(DS):
        allPatternsFound = True
        for Pattern in Patterns:
            if not Pattern in d:
                allPatternsFound = False
                break
        for Pattern in RunOptions.exclude:
            if Pattern in d:
                allPatternsFound = False
                break
        if allPatternsFound:
            IsInWrite = False
            if d.split(".")[-1].isdigit(): d = d[:d.rfind(".")]
            if d.find("_tid") != -1: d = d[0:d.rfind("_tid")]
            if len([w for w in Write if w.find(d) != -1]) > 0: continue
            logging.info("Write dataset %s" % (d))
            Write.append(d)
    if len(Write) == 0:
        logging.error("No datasets containing given pattern(s) found!")
        exit(0)

    WriteList(Write, "%s/%s" % (OutDir, FileList))
    logging.info("Datasets written to file %s/%s" % (OutDir, FileList))

if __name__ == '__main__':
    main()
