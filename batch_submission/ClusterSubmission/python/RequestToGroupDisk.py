#! /usr/bin/env python
from ClusterSubmission.ListDisk import RUCIO_ACCOUNT, RUCIO_RSE, ListUserRequests, getRSEs
from ClusterSubmission.Utils import ReadListFromFile, CheckRemainingProxyTime, CheckRucioSetup, CreateDirectory, ClearFromDuplicates
import logging, argparse, os
logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)


def initiateReplication(ListOfDataSets=[], Rucio="", RSE="", lifeTime=-1, approve=False, comment=""):
    Arguments = []
    ### Life time is interpreted in seconds from
    ### the client. Let's transform it into days
    if lifeTime > -1: Arguments.append(" --lifetime %d " % (lifeTime * 60 * 60 * 24))
    if approve: Arguments.append(" --ask-approval ")
    if len(comment) > 0: Arguments.append(" --comment \"%s\" " % (comment))
    if len(RSE) == 0:
        logging.error("No rucio RSE has been given")
        return
    if len(ListOfDataSets) == 0:
        logging.warning("No datasets given")
        return
    os.environ['RUCIO_ACCOUNT'] = Rucio
    requested_ds = ListUserRequests(RSE, Rucio)
    requested_ds += [ds[ds.find(":") + 1:] for ds in requested_ds if ds.find(":") != -1]
    for Item in ListOfDataSets:
        if Item.startswith("#") or len(Item) == 0 or Item in requested_ds:
            continue
        logging.info("Request new rule for %s to %s" % (Item, RSE))
        Cmd = "rucio add-rule --account %s %s %s 1 %s" % (Rucio, " ".join(Arguments), Item, RSE)
        logging.debug("Executing " + Cmd)
        os.system(Cmd)


def getArgumentParser():
    """Get arguments from command line."""
    parser = argparse.ArgumentParser(description='This script requests datasets to a RSE location. Further options can be specified.',
                                     prog='RequestToGroupDisk',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-r', '-R', '--RSE', '--rse', help='specify RSE storage element', default=RUCIO_RSE, choices=getRSEs())
    parser.add_argument('-l', '--list', help='specify a list containing the datasets to be requested', required=True)
    parser.add_argument("--rucio", help="With this option you can set the rucio_account", default=RUCIO_ACCOUNT)
    parser.add_argument("--lifetime", help="Defines a lifetime after which the rules are automatically deleted", type=int, default=-1)
    parser.add_argument("--askapproval", help="Asks for approval of the request", default=False, action="store_true")
    parser.add_argument("--comment", help="Comment", default="")
    return parser


def main():
    """Request datasets to RSE location."""
    CheckRucioSetup()
    CheckRemainingProxyTime()

    RunOptions = getArgumentParser().parse_args()
    List = ClearFromDuplicates(ReadListFromFile(RunOptions.list))

    ### Start replication of the datasets
    initiateReplication(ListOfDataSets=List,
                        Rucio=RunOptions.rucio,
                        RSE=RunOptions.RSE,
                        lifeTime=RunOptions.lifetime,
                        approve=RunOptions.askapproval,
                        comment=RunOptions.comment)


if __name__ == '__main__':
    main()
