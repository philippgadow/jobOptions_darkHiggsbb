import sys, os, argparse, logging
from ClusterSubmission.Utils import ReadListFromFile, WriteList


def convertToAOD(DAOD):
    AOD = DAOD
    if AOD.find(":") > -1: AOD = AOD[AOD.find(":") + 1:]
    ### replace any .deriv in the dataset name
    ### mc16_13TeV:mc16_13TeV.404292.MGPy8EG_A14N23LO_GG_1400_100_LLEi33.deriv.DAOD_SUSY2.e5651_s3126_r9364_r9315_p3260
    AOD = AOD.replace(".deriv", "")

    ### usual DAOD string
    daod_pos = AOD.find("DAOD")
    if daod_pos != -1:
        merge_pos = AOD.find(".merge")
        AOD = AOD[:daod_pos] + ("recon.AOD" if merge_pos == -1 else "AOD") + AOD[AOD.find(".", daod_pos):]
    ### replace the NTUP_PILEUP string
    ntup_pileup = AOD.find("NTUP_PILEUP")
    if ntup_pileup != -1:
        merge_pos = AOD.find(".merge")
        AOD = AOD[:ntup_pileup] + ("recon.AOD" if merge_pos == -1 else "recon.AOD") + AOD[AOD.find(".", ntup_pileup):]
    # remove the ptags
    while AOD.rfind("_p") > AOD.find("AOD"):
        AOD = AOD[:AOD.rfind("_p")]
    ## Remove the double r-tag
    while AOD.rfind("_r") != AOD.find("_r"):
        AOD = AOD[:AOD.rfind("_r")]
    ### Remote the double e -tag
    while AOD.rfind(".e") < AOD.rfind("_e"):
        uscore_pos = AOD.find("_", AOD.rfind(".e"))
        AOD = AOD[:uscore_pos] + AOD[AOD.find("_", uscore_pos + 1):]
    return AOD


if __name__ == '__main__':

    OutDir = os.getcwd()

    parser = argparse.ArgumentParser(
        description='This script converts DAOD filelists to AOD filelists which then can be used for creating pileup reweighting files.',
        prog='CreateAODFromDAODList',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--datasets', '-d', '-D', help='DAOD filelist to be converted into AOD', required=True)
    parser.add_argument('--outFile', help="pipe the output into a script into a file", default='')
    RunOptions = parser.parse_args()

    logging.info('The following DAODs are converted into ADOs:\n')
    DAODsToConvert = [convertToAOD(daod) for daod in ReadListFromFile(RunOptions.datasets)]

    logging.info('\nThe ADOs are:\n')

    for daod in DAODsToConvert:
        logging.info("   --- %s" % (daod))

    if len(RunOptions.outFile) > 0: WriteList(DAODsToConvert, RunOptions.outFile)
