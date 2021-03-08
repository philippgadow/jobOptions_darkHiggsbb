#! /usr/bin/env python
import math, os, sys, time, threading, string, random, argparse, logging, subprocess
logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)
_has_commands = True
try:
    import commands
except:
    _has_commands = False
    pass
import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True


def setupBatchSubmitArgParser():
    USERNAME = os.getenv("USER") if os.getenv("USER") else "none"
    """The name of the "BASEFOLDER" environment variable has historical reasons. The batch submission system in place during XAMPP development was the
    SUN grid engine system. However, it was replaced by SLURM at the MPPMU site and XAMPP has opened to other batch schedulers like HTCondor in the meantime.
    For the so-called "historic reasons", the developers have decided to stay with "SGE_BASEFOLDER", although it is not *S*un *G*rid *E*ngine anymore that
    does the heavy lifting but some other system."""
    BASEFOLDER = os.getenv("SGE_BASEFOLDER")
    STANDARD_ENGINE = os.getenv("SGE_CLUSTERENGINE")
    TESTAREA = os.getenv("TestArea")
    MYEMAIL = USERNAME + "@rzg.mpg.de" if not os.getenv("JOB_MAIL") else os.getenv("JOB_MAIL")
    SINGULARITY_DIR = "/cvmfs/atlas.cern.ch/repo/containers/images/singularity/"
    BUDGETCODE = ""

    #######################################################################
    #               Basic checks for TestArea and BASEFOLDER              #
    #######################################################################
    if STANDARD_ENGINE:
        logging.info("Default setup for the cluster engine to use has been found. Namely, %s." % (STANDARD_ENGINE))
    if BASEFOLDER == None:
        logging.info("Could not find the environment variable 'SGE_BASEFOLDER'. This variable defines the directory of your output & logs.")

        # RZG Garching
        ## Set garching to default enviroment
        BASEFOLDER = "/ptmp/mpp/%s/Cluster/" % (USERNAME)

        # try to auto detect settings based on hostname
        try:
            hostname = os.getenv("HOSTNAME")
        except NameError:
            hostname = ""

        if not hostname:
            logging.warning(
                "Could not auto-detect your system. Not possible to auto-assign SGE_BASEFOLDER to recommended path. Manual action required:"
            )
            logging.warning(
                "Environment variable SGE_BASEFOLDER must be defined in order to setup batch submit. Consider adding to your ~/.bashrc the line"
            )
            logging.warning("export SGE_BASEFOLDER=<path to storage space for batch logs, temp data and batch output>")
            BASEFOLDER = "/"
            hostname = "none"
        # DESY NAF Hamburg
        if "desy.de" in hostname and os.path.isdir("/nfs/dust/"):
            BASEFOLDER = "/nfs/dust/atlas/user/%s/Cluster/" % (USERNAME)
            BUDGETCODE = 'af-atlas'

        if "ox.ac.uk" in hostname:
            BASEFOLDER = "/data/atlas/atlasdata/{}".format(USERNAME)
            logging.info("Setting basefolder to {}".format(BASEFOLDER))
            logging.info(
                "Please set the environment variable 'SGE_BASEFOLDER' appropriately to avoid dumping unorganized stuff into your data directory."
            )

        # CERN lxplus Geneva
        if "lxplus" in hostname:
            BASEFOLDER = "/afs/cern.ch/work/%s/%s/Cluster/" % (USERNAME[0], USERNAME)
            logging.info("Setting the basefolder to the AFS work directory. Please make sure that this exists.")
            logging.info("If it does not exist please follow the instructions in this link to request it:")
            logging.info("https://resources.web.cern.ch/resources/Help/?kbid=067040")
            logging.info("Note that eventually AFS will become deprecated by the CERN IT service.")
            logging.info("It will be replaced by EOS. Look here for details and for the initial EOS space request: http://cernbox.cern.ch/")
            logging.info("If you want to use a different path already now, e.g. on EOS, consider adding to your ~/.bashrc the line")
            logging.info("export SGE_BASEFOLDER=/eos/user/%s/%s/Cluster" % (USERNAME[0], USERNAME))
        logging.info("Will set its default value to " + BASEFOLDER)

    if not TESTAREA:
        logging.error("Please set up AthAnalysis")
        exit(1)

    parser = argparse.ArgumentParser(
        prog='BatchSubmitParser',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="This script provides the basic arguments to setup the cluster engine",
    )
    parser.add_argument('--jobName', '-J', help='Specify the JobName', required=True)
    parser.add_argument('--HoldBuildJob',
                        help='Specifiy job names which should be finished before the build job starts',
                        default=[],
                        nargs="+")
    parser.add_argument('--BaseFolder', help='Changes the BaseFolder where the OutputFiles are saved.', default=BASEFOLDER)
    parser.add_argument('--BuildTime', help='Changes the RunTime of the BuildJob', default='01:59:59')
    parser.add_argument('--MergeTime', help='Changes the RunTime of the merge Jobs', default='01:59:59')
    parser.add_argument("--jobArraySize", help="The maximum size of the slurm job-array", type=int, default=7500)
    parser.add_argument("--maxCurrentJobs", help="The maximum size of the slurm job-array", type=int, default=400)
    parser.add_argument('--Build_vmem', help='Changes the virtual memory needed by the build job', type=int, default=8000)
    parser.add_argument('--Merge_vmem', help='Changes the virtual memory needed by the merge job', type=int, default=500)
    parser.add_argument('--nBuild_Cores', help="How many cores shall be used for the build job", type=int, default=2)
    parser.add_argument("--mailTo", help="Specify a notification E-mail address", default=MYEMAIL)
    parser.add_argument("--accountinggroup", help="If there is a special accounting group / project specify it here", default=BUDGETCODE)
    parser.add_argument("--engine",
                        help="What is the grid engine to use",
                        choices=["SLURM", "HTCONDOR", "LOCAL", "SGE"],
                        required=STANDARD_ENGINE is None,
                        default=STANDARD_ENGINE)
    parser.add_argument('--noBuildJob', help='Do not submit the build job', default=True, action="store_false")
    parser.add_argument('--ContainerShipping',
                        help='Setup the jobs inside a special singularity container',
                        default=False,
                        action="store_true")
    parser.add_argument("--SingularityImage",
                        help="The migration to centos 7 rises the need of using singularity",
                        choices=[item for item in os.listdir(SINGULARITY_DIR)
                                 if item.startswith("x86_64")] if os.path.exists(SINGULARITY_DIR) else [],
                        default="x86_64-centos6.img")
    parser.add_argument("--exclude_nodes", help="Specify nodes to be excluded from the submission", default=["zt02", "zt01"], nargs="+")
    return parser


def get_action_from_parser(my_parser, action_name):
    for act in my_parser._actions:
        if act.dest == action_name: return act
    logging.warning("Unkoown action %s in parser" % (action_name))
    return None


##
#### Helper method to create the proper ClusterSubmit engine
#### in your batch_submission script
####  --- RunOptions: Argument parser containing the arguments provided by setupBatchSubmitArgParser()
def setup_engine(RunOptions):
    from ClusterSubmission.LocalEngine import LocalEngine
    from ClusterSubmission.SGEEngine import SGEEngine
    from ClusterSubmission.SlurmEngine import SlurmEngine
    from ClusterSubmission.HTCondorEngine import HTCondorEngine

    if not RunOptions.noBuildJob:
        logging.warning("You are submitting without any schedule of an build job. This is not really recommended")

    if RunOptions.engine == "SLURM":
        return SlurmEngine(jobName=RunOptions.jobName,
                           baseDir=RunOptions.BaseFolder,
                           buildTime=RunOptions.BuildTime,
                           mergeTime=RunOptions.MergeTime,
                           buildCores=RunOptions.nBuild_Cores,
                           buildMem=RunOptions.Build_vmem,
                           mergeMem=RunOptions.Merge_vmem,
                           maxArraySize=RunOptions.jobArraySize,
                           maxCurrentJobs=RunOptions.maxCurrentJobs,
                           mail_user=RunOptions.mailTo,
                           hold_build=RunOptions.HoldBuildJob,
                           accountinggroup=RunOptions.accountinggroup,
                           singularity_image=RunOptions.SingularityImage,
                           run_in_container=RunOptions.ContainerShipping,
                           submit_build=RunOptions.noBuildJob,
                           exclude_nodes=RunOptions.exclude_nodes)
    elif RunOptions.engine == "HTCONDOR":
        return HTCondorEngine(jobName=RunOptions.jobName,
                              baseDir=RunOptions.BaseFolder,
                              buildTime=RunOptions.BuildTime,
                              mergeTime=RunOptions.MergeTime,
                              buildCores=RunOptions.nBuild_Cores,
                              buildMem=RunOptions.Build_vmem,
                              mergeMem=RunOptions.Merge_vmem,
                              maxArraySize=RunOptions.jobArraySize,
                              maxCurrentJobs=RunOptions.maxCurrentJobs,
                              mail_user=RunOptions.mailTo,
                              accountinggroup=RunOptions.accountinggroup,
                              singularity_image=RunOptions.SingularityImage,
                              run_in_container=RunOptions.ContainerShipping,
                              hold_build=RunOptions.HoldBuildJob,
                              submit_build=RunOptions.noBuildJob)
    elif RunOptions.engine == "SGE":
        return SGEEngine(jobName=RunOptions.jobName,
                         baseDir=RunOptions.BaseFolder,
                         buildTime=RunOptions.BuildTime,
                         mergeTime=RunOptions.MergeTime,
                         buildCores=RunOptions.nBuild_Cores,
                         buildMem=RunOptions.Build_vmem,
                         mergeMem=RunOptions.Merge_vmem,
                         accountinggroup=RunOptions.accountinggroup,
                         singularity_image=RunOptions.SingularityImage,
                         maxArraySize=RunOptions.jobArraySize,
                         maxCurrentJobs=RunOptions.maxCurrentJobs,
                         mail_user=RunOptions.mailTo,
                         run_in_container=RunOptions.ContainerShipping,
                         hold_build=RunOptions.HoldBuildJob,
                         submit_build=RunOptions.noBuildJob)
    elif RunOptions.engine == "LOCAL":
        return LocalEngine(jobName=RunOptions.jobName,
                           baseDir=RunOptions.BaseFolder,
                           maxCurrentJobs=min(max(1, RunOptions.maxCurrentJobs), 16),
                           singularity_image=RunOptions.SingularityImage,
                           run_in_container=RunOptions.ContainerShipping)

    ### seemingly impossible to arrive here - catch the error in any case
    logging.error("<setup_engine> : How could you parse %s? It's not part of the choices." % (RunOptions.engine))
    exit(1)


class CmdLineThread(threading.Thread):
    def __init__(self, cmd, args=""):
        threading.Thread.__init__(self)
        self.__cmd = cmd
        self.__args = args
        self.__statusCode = -1

    def run(self):
        self.start_info()
        self.__statusCode = os.system("%s %s" % (self.__cmd, self.__args))

    def start_info(self):
        logging.info("Execute command %s %s" % (self.__cmd, self.__args))

    def isSuccess():
        if self.__statusCode == -1:
            logging.warning('Thread not executed')
        return (self.__statusCode == 0)


###    Writes a python list to an output file
###    Directories are resolved and created on the fly
###         --Files: List containing strings representing each a line in the final file
###         --OutLocation:  Final destination of the file
def WriteList(Files, OutLocation):
    """Write list of files to output location. If output location does not exist, create directory."""
    if OutLocation.find("/") != -1:
        CreateDirectory(OutLocation[:OutLocation.rfind("/")], CleanUpOld=False)
    with open(OutLocation, "w") as Out:
        if Out is None:
            logging.error('Could not create the file ' + OutLocation)
            return None
        for F in Files:
            Out.write(F + "\n")
        Out.close()
    return OutLocation


###    If a txt file exists the Files are appended to the existing one without
###    reading the full file content before. In case that the list does not exist WriteList
###    is invoked
###         --- Files: List containing strings representing each a line in the final file
###         --- OutLocation:  Final destination of the file
def AppendToList(Files, OutLocation):
    if not os.path.isfile(OutLocation): return WriteList(Files, OutLocation)
    with open(OutLocation, "a") as Out:
        if Out is None:
            logging.error("Could not write file " + OutLocation)
            return None
        for F in Files:
            Out.write(F + "\n")
        Out.close()
    return OutLocation


###    Returns a string of random characters
###        --- size: Length of the final string
def id_generator(size=45, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


###     Pipes the cmd-line output of a command into a python list
###     where each element represents a line from the output. Trailing
###     and beginning white spaces are removed
###         --- command: Full command inclusive its arguments
def getGmdOutput(command):
    std_out = []
    if not _has_commands:
        std_out = subprocess.getoutput(command)
    else:
        std_out = commands.getoutput(command)
    return [x.strip() for x in std_out.split("\n")]


def CheckRemainingProxyTime():
    """Check if the GRID proxy lifetime if larger than 0. Otherwise generate new GRID proxy."""
    RemainingTime = 0
    try:
        RemainingTime = int(getGmdOutput("voms-proxy-info --timeleft")[-1])
    except ValueError:
        pass
    if not RemainingTime > 0:
        logging.info("No valid VOMS-PROXY, creating one...")
        os.system("voms-proxy-init --voms atlas")
        CheckRemainingProxyTime()
    return RemainingTime


def CheckRucioSetup():
    try:
        import rucio
    except ImportError:
        logging.error('No RUCIO setup is found please SETUP rucio using "lsetup rucio"')
        sys.exit(1)

    if not os.getenv("RUCIO_ACCOUNT"):
        logging.error("No RUCIO ACCOUNT is available.. please define a rucio Account")
        exit(1)
    logging.info("Rucio is set up properly.")


def CheckAMISetup():
    try:
        import pyAMI.client
    except ImportError:
        logging.error('No AMI setup is found please SETUP pyami using "lsetup pyami"')
        sys.exit(1)
    logging.info("AMI is set up properly.")


def CheckPandaSetup():
    PANDASYS = os.getenv("PANDA_SYS")
    if PANDASYS == None:
        logging.error('Please setup Panda using "lsetup panda"')
        exit(1)
    CheckRucioSetup()
    CheckRemainingProxyTime()


def IsROOTFile(FileName):
    if FileName.endswith(".root"): return True
    if FileName.split(".")[-1].isdigit():
        return FileName.split(".")[-2] == "root"
    return False


def CreateDirectory(Path, CleanUpOld=True):
    """Create new directory if possible. Option to delete existing directory of same name."""
    if len(Path) == 0 or Path == os.getenv("HOME"):
        logging.error("Could not create " + Path)
        return False
    if os.path.exists(Path) and CleanUpOld:
        logging.info("Found old copy of the folder " + Path)
        logging.info("Will delete it.")
        os.system("rm -rf " + Path)
    if not os.path.exists(Path):
        logging.info("Create directory " + Path)
    os.system("mkdir -p " + Path)
    return os.path.exists(Path)


###  Reads in a txt file and converts its content
###  to a python list. Each line is represented by one
###  list item. Lines starting with # or which are empty are skipped.
###  Trailing and leading white spaces are removed from the text
###     --- File: Path to the text file to be read
def ReadListFromFile(File):
    List = []
    In_Path = ResolvePath(File)
    if In_Path and len(In_Path) > 0:
        with open(In_Path) as myfile:
            for line in myfile:
                if line.startswith('#'):
                    continue
                realline = line.strip()
                if not realline:
                    continue  # Ignore whitespace
                List.append(realline)
    else:
        logging.warning("Could not find list file %s" % (File))
    return List


def prettyPrint(preamble, data, width=30, separator=":"):
    """Prints uniformly-formatted lines of the type "preamble : data"."""
    preamble = preamble.ljust(width)
    print('{preamble}{separator} {data}'.format(preamble=preamble, separator=separator, data=data))


def TimeToSeconds(Time):
    """Convert time in format DD:MM:SS to seconds."""
    S = 0
    for i, E in enumerate(Time.split(":")):
        try:
            S += int(E) * math.pow(60, 2 - i)
        except Exception:
            if i == 0:
                try:
                    S += (24 * int(E.split("-")[0]) + int(E.split("-")[1])) * math.pow(60, 2 - i)
                except Exception:
                    pass
    return S


def ResolvePath(In):
    try:
        from PathResolver import PathResolver
    except Exception:
        try:
            PathResolver = ROOT.PathResolver()
        except:
            logging.error("I've no idea what to do next")
            exit(1)
    ### Make the path resolver being totally quiet
    try:
        from AthenaCommon.Constants import FATAL, ERROR
        PathResolver.SetOutputLevel(FATAL)
    except:
        try:
            PathResolver.setOutputLevel(6)
        except:
            pass
    if os.path.exists(In):
        return os.path.abspath(In)
    # Remove the 'data/' in the file name
    if "data/" in In:
        In = In.replace("data/", "")

    ResIn = PathResolver.FindCalibFile(In)
    if not ResIn:
        ResIn = PathResolver.FindCalibDirectory(In)
    if len(ResIn) > 0 and os.path.exists(ResIn):
        return ResIn

    PkgDir = PathResolver.FindCalibDirectory(In.split("/")[0])
    if PkgDir and os.path.exists("%s/%s" % (PkgDir, In)):
        return "%s/%s" % (PkgDir, In)
    try:
        Ele = os.listdir(PkgDir)[0]
        PkgDir = os.path.realpath(PkgDir + "/" + Ele + "/../../")
        if os.path.exists("%s/%s" % (PkgDir, In.split("/", 1)[-1])):
            return "%s/%s" % (PkgDir, In.split("/", 1)[-1])
    except Exception:
        pass
    logging.error("No such file or directory " + In)
    return None


def MakePathResolvable(file_location):
    if len([x for x in ["/", "./", "../"] if file_location.startswith(x) > 0]):
        logging.error("Please avoig giving absolute paths")
        return ""
    if file_location.find("data/") == file_location.find("/") + 1:
        file_location = file_location[:file_location.find("/") + 1] + file_location[file_location.find("data/") + 5:]
        if not ResolvePath(file_location):
            logging.error("The file is somehow not part of a package?")
            return ""

    return file_location


def CheckConfigPaths(Configs=[], filetype="conf"):
    Files = []
    for C in Configs:
        TempConf = ResolvePath(C)
        if not TempConf:
            continue
        if os.path.isdir(TempConf):
            logging.info("The Config " + C + " is a directory read-out all config files")
            Files += [
                "%s/%s" % (TempConf, Cfg) for Cfg in os.listdir(TempConf)
                if Cfg.endswith(filetype) and not os.path.isdir("%s/%s" % (TempConf, Cfg))
            ]
        elif os.path.isfile(TempConf) and C.endswith(filetype):
            Files.append(TempConf)
    return Files


def ExecuteCommands(ListOfCmds, MaxCurrent=16, MaxExec=-1, verbose=True, sleep_time=0.01):
    Threads = []
    for Cmd in ListOfCmds:
        Threads.append(CmdLineThread(Cmd))
    ExecuteThreads(Threads=Threads, MaxCurrent=MaxCurrent, MaxExec=MaxExec, verbose=verbose, sleep_time=sleep_time)


def getRunningThreads(Threads):
    Running = 0
    for Th in Threads:
        if Th.isAlive():
            Running += 1
    return Running


def ExecuteThreads(Threads, MaxCurrent=16, MaxExec=-1, verbose=True, sleep_time=0.01):
    Num_Executed = 0
    Num_Threads = len(Threads)
    N_Prompt = min([100, int(Num_Threads / 100)])
    if N_Prompt == 0:
        N_Prompt = int(Num_Threads / 10) if int(Num_Threads / 10) > 0 else 100
    while Num_Executed != Num_Threads:
        if Num_Executed == MaxExec:
            break
        while getRunningThreads(Threads) < MaxCurrent and Num_Threads != Num_Executed:
            Threads[Num_Executed].start()
            Num_Executed += 1
            time.sleep(sleep_time)
            if Num_Executed == MaxExec:
                break
            if verbose and Num_Executed % N_Prompt == 0:
                logging.info("Executed %d out of %d threads" % (Num_Executed - getRunningThreads(Threads), Num_Threads))
        WaitCounter = 0
        while getRunningThreads(Threads) >= MaxCurrent:
            if verbose:
                WaitCounter += 1
            if WaitCounter == 5000:
                logging.info("At the moment %d threads are active. Executed %d out of %d Threads" %
                             (getRunningThreads(Threads), Num_Executed - getRunningThreads(Threads), len(Threads)))
                WaitCounter = 0
            time.sleep(sleep_time)
    WaitCounter = 0
    while getRunningThreads(Threads) > 0:
        if WaitCounter == 5000:
            logging.info("Wait until the last %d threads are going to finish " % (getRunningThreads(Threads)))
            WaitCounter = 0
        time.sleep(0.01)
        if verbose:
            WaitCounter += 1


def RecursiveLS(Dir, data_types=[]):
    n_data_types = len(data_types)
    if not os.path.isdir(Dir):
        logging.warning("Not a directory %s" % (Dir))
        return []
    LS = []
    for item in os.listdir(Dir):
        full_path = "%s/%s" % (Dir, item)
        full_path = full_path.replace("//", "/")
        if os.path.isdir(full_path): LS += RecursiveLS(full_path, data_types)
        if n_data_types == 0 or len([d for d in data_types if item.endswith(".%s" % (d))]) > 0:
            LS += [full_path]
    return LS


def ClearFromDuplicates(In=[]):
    TmpIn = []
    for I in In:
        if I not in TmpIn: TmpIn.append(I)
    return TmpIn


def FillWhiteSpaces(N, Space=" "):
    str = ""
    for i in range(0, N):
        str += Space
    return str


def convertStdVector(str_list=[]):
    vec = ROOT.std.vector(str)()
    for entry in str_list:
        vec.push_back(entry)
    return vec


def convertFloatVector(double_list=[]):
    vec = ROOT.std.vector(float)()
    for entry in double_list:
        vec.push_back(entry)
    return vec


def convertDoubleVector(double_list=[]):
    vec = ROOT.vector('double')()
    for entry in double_list:
        vec.push_back(entry)
    return vec


def IsListIn(List1=[], List2=[]):
    for L in List1:
        EntryIn = False
        if len(List2) == 0:
            logging.warning("Reference list not found")
            return False
        for C in List2:
            if C.find(L.strip()) > -1:
                EntryIn = True
                break
        if EntryIn == False:
            logging.info(L + " not found in reference list")
            return False
    return True


def cmp_to_key(mycmp):
    'Convert a cmp= function into a key= function'

    ### Taken from https://docs.python.org/3/howto/sorting.html
    class K:
        def __init__(self, obj, *args):
            self.obj = obj

        def __lt__(self, other):
            return mycmp(self.obj, other.obj) < 0

        def __gt__(self, other):
            return mycmp(self.obj, other.obj) > 0

        def __eq__(self, other):
            return mycmp(self.obj, other.obj) == 0

        def __le__(self, other):
            return mycmp(self.obj, other.obj) <= 0

        def __ge__(self, other):
            return mycmp(self.obj, other.obj) >= 0

        def __ne__(self, other):
            return mycmp(self.obj, other.obj) != 0

    return K
