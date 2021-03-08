#! /usr/bin/env python
from ClusterSubmission.ListDisk import getRSEs, RUCIO_RSE, RUCIO_ACCOUNT, ListUserRequests, ListDisk, GetDataSetInfo
from ClusterSubmission.Utils import CheckRemainingProxyTime, CheckRucioSetup, ReadListFromFile
import logging, argparse, os
logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)


def getArgumentParser():
    """Read arguments from command line"""
    parser = argparse.ArgumentParser(
        description='This script deletes datasets located at a RSE location. Futher patterns to find or exclude can be specified.',
        prog='DeleteFromGroupDisk',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-r', '-R', '--RSE', "--rse", help='specify RSE storage element', default=RUCIO_RSE, choices=getRSEs())
    parser.add_argument('-d', '-l', '-D', '-L', '--list', help='specify a list containing the datasets to be deleted')
    parser.add_argument("--rucio", help="With this option you can set the rucio_account", default=RUCIO_ACCOUNT)
    return parser


def deleteFromDisk(datasets=[], rucio_account="", rse=""):
    os.environ["RUCIO_ACCOUNT"] = rucio_account
    MyRequests = ListUserRequests(rse, rucio_account)
    to_del = []
    for Item in datasets:
        if len(Item) <= 1: continue
        ## Put in the scope
        if Item.find(":") == -1: Item = Item[:Item.find(".", Item.find(".") + 1)] + ":" + Item
        for DS in MyRequests:
            if Item == DS or (len(Item) < len(DS) and DS.find(Item) == 0 and DS[len(Item)] in [".", "_"]):
                logging.info("Found rucio rule " + DS + " matching " + Item)
                to_del += [DS]
    n_to_del = len(to_del)
    for i, Item in enumerate(to_del, 1):
        logging.info("Delete %d/%d rule for %s at storage element %s subscribed by %s" % (i, n_to_del, Item, rse, rucio_account))
        ID, Owner = GetDataSetInfo(Item, rse, rucio_account)
        if not ID: continue
        cmd = "rucio delete-rule %s --account %s" % (ID, rucio_account)
        logging.debug("Executing " + cmd)
        os.system(cmd)


if __name__ == '__main__':
    CheckRucioSetup()
    CheckRemainingProxyTime()
    RunOptions = getArgumentParser().parse_args()
    deleteFromDisk(datasets=ReadListFromFile(RunOptions.list), rucio_account=RunOptions.rucio, rse=RunOptions.RSE)
