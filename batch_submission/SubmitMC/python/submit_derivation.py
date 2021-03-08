from submit import getArguments
from submit import DerivationSubmit
from ClusterSubmission.Utils import setup_engine


def main():
    RunOptions = getArguments().parse_args()
    runNumbersList = [R for R in RunOptions.runNumbers]
    if RunOptions.runRange:
        if len(RunOptions.runRange) == 2:
            runNumbersList += [i for i in  range(RunOptions.runRange[0], RunOptions.runRange[-1] + 1) ]
        else:
            sys.exit("ERROR: Provide at least two DSIDs to define a range. Exiting.")

    if not runNumbersList:
        sys.exit("ERROR: No DSIDs were provided. Exiting.")
    
    cluster_engine = setup_engine(RunOptions)
    if not cluster_engine.submit_build_job(): exit(1)

    for derivation in RunOptions.derivations:
        daod_submit = DerivationSubmit(
                     runs = runNumbersList,
                     cluster_engine = cluster_engine,
                     derivation = derivation,
                     derivationCache = RunOptions.AthDerivation,
                     derivationRelease="AthDerivation",
                     run_time = RunOptions.deriv_runtime,
                     memory = RunOptions.deriv_memory,
                     preInclude=RunOptions.deriv_preInclude,
                     postInclude=RunOptions.deriv_postInclude,
                     preExec= RunOptions.deriv_preExec,
                     postExec=RunOptions.deriv_postExec,
                     )
        if not daod_submit.submit_job():
           exit(1)

    cluster_engine.submit_clean_all([cluster_engine.subjob_name(D) for D in RunOptions.derivations])

    # schedule jobs
    cluster_engine.print_banner()
    cluster_engine.finish()


if __name__ == '__main__': 
    main()
