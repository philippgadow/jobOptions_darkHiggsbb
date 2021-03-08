#! /usr/bin/env python
from ClusterSubmission.Utils import ResolvePath
import argparse, os, logging
logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)


def getArgumentParser():
    """Get arguments from command line."""
    parser = argparse.ArgumentParser(prog='exScript',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description="Helper script used to provide encapsulated environments for local submission")
    parser.add_argument('--Cmd', help='Location where the command list is stored', required=True)
    return parser


def main():
    """Helper script to obtain encapsulated environments for LocalCluserEnginge"""
    options = getArgumentParser().parse_args()
    ### The environment variables are already encapsulated in the script of usage
    ### Find the location of the script to execute
    cmd_to_exec = ResolvePath(options.Cmd)
    if not cmd_to_exec:
        logging.error("%s does not exist" % (options.Cmd))
        exit(1)
    ### Make sure that we can execute it
    os.system("chmod 0755 %s" % (cmd_to_exec))
    ### Submit it
    exit(os.system(cmd_to_exec))


if __name__ == '__main__':
    main()
