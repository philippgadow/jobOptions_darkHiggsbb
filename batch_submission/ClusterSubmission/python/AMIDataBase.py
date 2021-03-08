#! /usr/bin/env python
from ClusterSubmission.Utils import CheckRemainingProxyTime, ClearFromDuplicates
from ClusterSubmission.PeriodRunConverter import GetPeriodRunConverter
from ClusterSubmission.ListDisk import RUCIO_ACCOUNT
import sys, argparse, os, logging
logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)

m_AMIClient = None


def getAmiClient():
    global m_AMIClient
    if m_AMIClient: return m_AMIClient
    try:
        import pyAMI.client
        import pyAMI.atlas.api as AtlasAPI

    except ImportError:
        logging.error('No AMI setup is found please SETUP AMI using "localSetupPyAMI"')
        sys.exit(1)

    if not RUCIO_ACCOUNT:
        logging.error("No RUCIO ACCOUNT is available.. please define a rucio Account")
        exit(1)

    while CheckRemainingProxyTime() < 600:
        logging.info("VOMS-PROXY is running out, renewing...")

    m_AMIClient = pyAMI.client.Client('atlas')
    AtlasAPI.init()
    return m_AMIClient


class AMI_Entry(object):
    def __init__(self):
        self.__datasets = {}

    def addDataset(self, data_type="AOD", tag="", events=-1, status=""):
        if data_type not in self.__datasets.iterkeys():
            self.__datasets[data_type] = {}
        if tag not in self.__datasets[data_type].iterkeys() and events > 0:
            self.__datasets[data_type][tag] = {'events': events, 'status': status}

    def dataTypes(self):
        return self.__datasets.iterkeys()

    def hasDataType(self, data_type):
        return data_type in self.dataTypes()

    def getEvents(self, tag, data_type="AOD"):
        if not self.hasDataType(data_type):
            logging.warning("Unkown data format %s " % (data_type))
            return 0
        try:
            return self.__datasets[data_type][tag]['events']
        except:
            return -1

    def getStatus(self, tag, data_type="AOD"):
        if not self.hasDataType(data_type):
            logging.warning("Unkown data format %s " % (data_type))
            return 0
        try:
            return self.__datasets[data_type][tag]['status']
        except:
            return "unkown"

    def tags(self, data_type="AOD"):
        if not self.hasDataType(data_type):
            logging.warning("Unknown data type %s" % (data_type))
            return []
        return sorted([T for T in self.__datasets[data_type].iterkeys()], key=lambda x: x[x.rfind("_") + 1:], reverse=True)


class AMImcEntry(AMI_Entry):
    def __init__(self, dsid=1, xsec=-1., filtereff=1., physics_name="", campaign="mc16_13TeV"):
        AMI_Entry.__init__(self)
        self.__dsid = dsid
        self.__xsec = xsec
        self.__filtereff = filtereff
        self.__physics = physics_name
        self.__campaign = campaign

    def dsid(self):
        return self.__dsid

    def xSection(self):
        return self.__xsec

    def filtereff(self):
        return self.__filtereff

    def name(self):
        return self.__physics

    def campaign(self):
        return self.__campaign

    def nEvents(self, tag, data_type="AOD", isAFII=False):
        N = self.getEvents(tag=tag, data_type=data_type)
        if N > 0: return N
        for known in self.getTags(data_type=data_type, filter_full=isAFII, filter_fast=not isAFII):
            if known.endswith(tag):
                return self.getEvents(data_type=data_type, tag=known)
        logging.warning("Unknown tag %s for sample %s (%d)" % (tag, self.name(), self.dsid()))
        return 0

    def getTags(self, data_type="AOD", filter_full=False, filter_fast=False):
        """filter means to drop the sim flavour from the list"""
        if not self.hasDataType(data_type):
            logging.warning("Unkown data format %s for sample %s (%d)" % (data_type, self.name(), self.dsid()))
            return []
        List = []
        for key in self.tags(data_type):
            if not filter_full and key.find("_s") != -1: List.append(key)
            elif not filter_fast and key.find("_a") != -1: List.append(key)
            elif not filter_full and not filter_fast: List.append(key)
            elif key.find("_s") == -1 and key.find("_a") == -1: List.append(key)
        return List


class AMIdataEntry(AMI_Entry):
    def __init__(self, runNumber=1):
        ### Constructor of the  mother class
        AMI_Entry.__init__(self)

        self.__runNumber = runNumber
        self.__period = GetPeriodRunConverter().GetPeriodElement(self.runNumber())

    def runNumber(self):
        return self.__runNumber

    def year(self):
        return self.__period.year()

    def topPeriod(self):
        return self.__period.period()[0]

    def subPeriod(self):
        return self.__period.period()


class AMIDataBase(object):
    def __init__(self):
        self.__mc_channels = []
        self.__runs = []

    def getMCDataSets(self, channels=[], campaign="mc16_13TeV", derivations=[]):
        getAmiClient()
        import pyAMI.client
        import pyAMI.atlas.api as AtlasAPI

        data_type = ClearFromDuplicates(["AOD"] + derivations)
        channels_to_use = []
        # Check only the dsid which are non-existent or not complete
        for mc in channels:
            ami_channel = self.getMCchannel(dsid=mc, campaign=campaign)
            if not ami_channel:
                channels_to_use.append(mc)
                continue
            # Check if the dsid is already complete w.r.t all data-formats
            to_append = False
            for data in data_type:
                if not ami_channel.hasDataType(data):
                    to_append = True
                if to_append:
                    break
            if to_append:
                channels_to_use.append(mc)

        Blocks = []
        # Try to block the queries in DSIDS of thousands
        for mc in channels_to_use:
            FirstDigits = int(str(mc)[0:3])
            if FirstDigits not in Blocks:
                Blocks.append(FirstDigits)
        # Summarizing into blocks leads to a huge reduction of queries
        if len(Blocks) < len(channels_to_use):
            channels_to_use = Blocks
        logging.info("<AMIDataBase>: going to ask AMI about %d different things" % (len(channels_to_use)))
        prompt = max(int(len(channels_to_use) / 10), 2)
        for i, mc in enumerate(channels_to_use):
            if i % prompt == 0:
                logging.info("<AMIDataBase>: %d/%d stubbed AMI :-P" % (i, len(channels_to_use)))
            # AMI query
            DSIDS = AtlasAPI.list_datasets(
                getAmiClient(),
                patterns=["%s.%i%%.%%" % (campaign, mc)],
                fields=[
                    'type',
                    'events',
                    'ami_status',
                    "physics_short",
                    "dataset_number",
                    "cross_section",
                    #"generator_filter_efficienty",  # if someone of the AMI guys fixes the name to generator_filter_efficiency this will break
                    "prodsys_status",
                ],
                ### Maximum 1000 datasets and foreach one
                limit=[1, 1000 * 75],
                type=data_type)

            for amiDS in DSIDS:
                DS = int(amiDS["dataset_number"])
                ami_entry = self.getMCchannel(dsid=DS, campaign=campaign)
                # a fresh AMImcEntry needs to be created
                if not ami_entry:
                    physics_name = amiDS["physics_short"]
                    try:
                        xS = float(amiDS["cross_section"])
                    except Exception:
                        logging.warning("<AMIDataBase>: No x-section found for %s (%i) in AMI" % (physics_name, DS))
                        xS = 1.
                    try:
                        filterEfficiency = float(
                            amiDS["generator_filter_efficienty"]
                        )  # if someone of the AMI guys fixes the name to generator_filter_efficiency this will break
                    except Exception:
                        logging.warning("<AMIDataBase>: No filter efficiency found for %s (%i) in AMI" % (physics_name, DS))
                        filterEfficiency = 1.
                    ami_entry = AMImcEntry(dsid=DS, xsec=xS, filtereff=filterEfficiency, physics_name=physics_name, campaign=campaign)
                    self.__mc_channels.append(ami_entry)
                ds_type = amiDS["type"]
                tag = self.__getDSTag(amiDS['ldn'], ds_type)
                nevents = int(amiDS['events'])
                ami_entry.addDataset(data_type=ds_type, tag=tag, events=nevents, status=amiDS["prodsys_status"])
        return True

    def getMCchannel(self, dsid, campaign="mc16_13TeV"):
        for ch in self.__mc_channels:
            if ch.dsid() == dsid and ch.campaign() == campaign:
                return ch
        return None

    def __getDSTag(self, DSName, type):
        return DSName[DSName.find(type) + len(type) + 1:len(DSName)]

    def getRunElement(self, runNumber):
        for run in self.__runs:
            if run.runNumber() == runNumber: return run
        return None

    ## The project is defined as data<Year>_<Project>.
    def loadRuns(self, Y, derivations=[], project="13TeV"):
        ### import AMI
        getAmiClient()
        import pyAMI.client
        import pyAMI.atlas.api as AtlasAPI

        periods = GetPeriodRunConverter().GetSubPeriods(Y, project=project)

        ### I'm not happy about this pattern line. If we change project to cos or hi
        ### then the patter might differ what AMI needs
        Pattern = "data%i_%s.%%physics_Main.%%" % (Y, project)

        DSIDS = AtlasAPI.list_datasets(getAmiClient(),
                                       patterns=[Pattern],
                                       fields=[
                                           'run_number',
                                           "period",
                                           'type',
                                           'events',
                                           'ami_status',
                                       ],
                                       period=",".join(GetPeriodRunConverter().GetSubPeriods(Y, project=project)),
                                       type=ClearFromDuplicates(["AOD"] + derivations))

        ### Read out the AMI query
        for entry in DSIDS:
            R = int(entry["run_number"])
            if not self.getRunElement(R): self.__runs += [AMIdataEntry(R)]
            runElement = self.getRunElement(R)
            flavour = entry["type"]
            tag = self.__getDSTag(entry['ldn'], flavour)
            nevents = int(entry['events'])
            runElement.addDataset(data_type=flavour, tag=tag, events=nevents, status="")


m_ami_db = None


def getAMIDataBase():
    global m_ami_db
    if not m_ami_db:
        m_ami_db = AMIDataBase()
    return m_ami_db
