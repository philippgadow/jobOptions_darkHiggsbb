from ClusterSubmission.Utils import CreateDirectory, ExecuteCommands, ReadListFromFile, ClearFromDuplicates, WriteList, ResolvePath
from ClusterSubmission.AMIDataBase import getAMIDataBase
import os, argparse, logging


def GetAMITagsMC(DS, SkimPTag=False, SkimETag=True, SkimSTag=True):
    Tag = DS[DS.rfind(".") + 1:]
    if SkimPTag:
        while SkimETag and Tag.find("e") != -1:
            Tag = Tag[Tag.find("_") + 1:]
        while SkimSTag and Tag.find("s") != -1:
            Tag = Tag[Tag.find("_") + 1:]
        while SkimSTag and Tag.find("a") != -1:
            Tag = Tag[Tag.find("_") + 1:]
        while Tag.rfind("_p") != -1:
            Tag = Tag[:Tag.rfind("_p")]
        return Tag
    else:
        if Tag.rfind("_p") != -1:
            Tag = Tag[Tag.find("_p"):]
            while Tag.rfind("_p") != Tag.rfind("_"):
                Tag = Tag[:Tag.rfind("_")]
            if Tag.startswith("_"): Tag = Tag[1:]
            return GetAMITagsMC(DS, SkimPTag=True, SkimETag=SkimETag, SkimSTag=SkimSTag) + "_" + Tag
        else:
            return GetAMITagsMC(DS, SkimPTag=True, SkimETag=SkimETag, SkimSTag=SkimSTag)
    return Tag


def GetPRW_datasetID(DS):
    try:
        dsid = int(DS.split('.')[1])
        return dsid
    except:
        pass
    ds_copy = DS
    if ds_copy.find(":") != -1: ds_copy = ds_copy[ds_copy.find(":") + 1:]
    for Sub in getUsersSubmittedPRW():
        if ds_copy.startswith(Sub):
            ds_copy = ds_copy[len(Sub) + 1:]
            return GetPRW_datasetID(ds_copy)
    logging.warning("Could not extract the DSID from %s" % (DS))
    return 0


def setupArgParser():
    parser = argparse.ArgumentParser(prog='UpdateSampleLists',
                                     description='This script updates sample lists in a given directory to the latest derivation cache',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--ListDir', '-o', '-O', help='Where are the file-lists located', type=str, required=True)
    # r9838 special mc16a tag with lowered ditau pt threshold
    parser.add_argument('--mc16aTag', help='Which rtag should be used for the mc16a campaign', default=['r9364', 'r9838'], nargs="+")
    # r10261 special mc16d tag with lowered ditau pt threshold
    parser.add_argument('--mc16dTag', help='Which rtag should be used for the mc16c campaign', default=['r10201', 'r10261'], nargs="+")
    # r11262 special mc16d tag with lowered ditau pt threshold
    parser.add_argument('--mc16eTag', help='Which rtag should be used for the mc16c campaign', default=['r10724', 'r11262'], nargs="+")
    parser.add_argument('--derivation', help='Which derivation should be written to the file lists ', default='SUSY2')
    parser.add_argument("--min_ptag", help="Which ptag should the derivation at least have", default=-1, type=int)
    parser.add_argument("--max_ptag", help="Which ptag should the derivation at least have", default=-1, type=int)
    return parser


if __name__ == "__main__":
    RunOptions = setupArgParser().parse_args()
    Sample_Dir = ResolvePath(RunOptions.ListDir)
    No_AOD = []
    TO_REQUEST = []

    if not Sample_Dir:
        logging.error("ERROR: Please give a valid  directory")
        exit(1)

    for File in os.listdir(Sample_Dir):
        if os.path.isdir("%s/%s" % (Sample_Dir, File)): continue
        logging.info("Update file list %s" % (File))

        DataSets = sorted(
            ClearFromDuplicates([GetPRW_datasetID(DS) for DS in ReadListFromFile("%s/%s" % (Sample_Dir, File)) if DS.find("data") == -1]))
        if len(DataSets) == 0: continue
        logging.info("Call the AMI database")

        DERIVATIONS = []
        NO_DERIVARTION = []
        AODs = []
        getAMIDataBase().getMCDataSets(channels=DataSets, derivations=["DAOD_%s" % (RunOptions.derivation)])
        #### Find the AODs for each DSID first
        for DSID in DataSets:
            Found_MC16a = False
            Found_MC16d = False
            Found_MC16e = False
            pyami_ch = getAMIDataBase().getMCchannel(dsid=DSID)
            tags_to_check = pyami_ch.getTags(data_type="AOD", filter_fast=True) if len(pyami_ch.getTags(
                data_type="AOD", filter_fast=True)) else pyami_ch.getTags(data_type="AOD", filter_full=True)
            for aod_tag in tags_to_check:
                ### reject duplicate tags
                if aod_tag.find("_e") != -1 or len([x for x in ["r", "s", "a"] if aod_tag.rfind("_%s" % (x)) != aod_tag.find("_%s" %
                                                                                                                             (x))]) > 0:
                    continue
                if len([x for x in RunOptions.mc16aTag + RunOptions.mc16dTag + RunOptions.mc16eTag if aod_tag.endswith(x)]) == 0: continue
                if len([x for x in RunOptions.mc16aTag if aod_tag.endswith(x)]) == 1:
                    Found_MC16a = True
                elif len([x for x in RunOptions.mc16dTag if aod_tag.endswith(x)]) == 1:
                    Found_MC16d = True
                elif len([x for x in RunOptions.mc16eTag if aod_tag.endswith(x)]) == 1:
                    Found_MC16e = True
                AODs.append("%s.%d.%s.recon.AOD.%s" % (pyami_ch.campaign(), pyami_ch.dsid(), pyami_ch.name(), aod_tag))
                dAOD_Tags = sorted([
                    t for t in pyami_ch.getTags(data_type="DAOD_%s" % (RunOptions.derivation))
                    if t.startswith("%s_p" % (aod_tag)) and int(t[t.rfind("p") + 1:]) >= RunOptions.min_ptag and (
                        RunOptions.max_ptag < 0 or int(t[t.rfind("p") + 1:]) <= RunOptions.max_ptag)
                ],
                                   key=lambda x: x.split("_")[-1],
                                   reverse=True)
                if len(dAOD_Tags) == 0:
                    logging.warning("No %s derivation found for %s" % (RunOptions.derivation, AODs[-1]))
                    NO_DERIVARTION += [AODs[-1]]
                else:
                    DERIVATIONS += [
                        "%s.%d.%s.deriv.DAOD_%s.%s" %
                        (pyami_ch.campaign(), pyami_ch.dsid(), pyami_ch.name(), RunOptions.derivation, dAOD_Tags[0])
                    ]

            if not (Found_MC16a or Found_MC16d or Found_MC16e):
                logging.warning(" No AOD could be found at all for DSID %d" % (DSID))
                No_AOD.append(str(DSID))
            if not Found_MC16a:
                logging.warning("No mc16a found for %d (%s)" % (DSID, pyami_ch.name()))
                No_AOD.append("%s.%d.%s  -- mc16a" % (pyami_ch.campaign(), DSID, pyami_ch.name()))
            if not Found_MC16d:
                logging.warning("No mc16d found for %d (%s)" % (DSID, pyami_ch.name()))
                No_AOD.append("%s.%d.%s  -- mc16d" % (pyami_ch.campaign(), DSID, pyami_ch.name()))
            if not Found_MC16e:
                logging.warning("No mc16e found for %d (%s)" % (DSID, pyami_ch.name()))
                No_AOD.append("%s.%d.%s  -- mc16e" % (pyami_ch.campaign(), DSID, pyami_ch.name()))

        WriteList(sorted(DERIVATIONS + NO_DERIVARTION, key=lambda x: GetPRW_datasetID(x)), "%s/%s" % (Sample_Dir, File))
        TO_REQUEST += NO_DERIVARTION

    if len(TO_REQUEST) > 0: WriteList(ClearFromDuplicates(TO_REQUEST), "ToRequestOnJIRA.txt")
    if len(No_AOD) > 0: WriteList(No_AOD, "NoAOD.txt")
