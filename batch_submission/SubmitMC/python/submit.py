from ClusterSubmission.Utils import (
    FillWhiteSpaces,
    ClearFromDuplicates,
    ResolvePath,
    WriteList,
    ReadListFromFile,
    CreateDirectory,
    id_generator,
    ResolvePath,
    setup_engine,
    setupBatchSubmitArgParser,
)
from ClusterSubmission.ClusterEngine import TESTAREA, ATLASVERSION, ATLASPROJECT
import os
import random
import sys
import logging
import shutil


def getArguments():
    USERNAME = os.getenv("USER")
    parser = setupBatchSubmitArgParser()
    parser.set_defaults(
        BaseFolder="/nfs/dust/atlas/user/{username}/MC".format(username=USERNAME)
    )
    parser.set_defaults(maxCurrentJobs=100)
    parser.set_defaults(noContainerShipping=False)
    parser.set_defaults(jobOptionsDir="./..")
    parser.add_argument("--modelsDir", default="", help="Path to directory containing additional models.")

    parser.add_argument("-r", "--runNumbers", nargs="+", default=[], type=int)
    parser.add_argument("-R", "--runRange", nargs=2, action="store", type=int)

    parser.add_argument(
        "--nJobs",
        help="Number of jobs to be submitted [default: 5]",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--eventsPerJob",
        help="Events generated per job [default: 10000]",
        type=int,
        default=10000,
    )
    parser.add_argument(
        "--keepOutput",
        help="Keep RunDir after task",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--AthGeneration", help="Event generation release version", default="21.6.61"
    )

    parser.add_argument(
        "--noDerivationJob",
        help="Disable launching derivation jobs.",
        action="store_true",
    )
    parser.add_argument(
        "--derivations",
        help="Format of TRUTH derivations [default: TRUTH1]",
        nargs="+",
        default=["TRUTH1"],
    )
    parser.add_argument(
        "--AthDerivation", help="Derivation release version", default="21.2.63.0"
    )

    parser.add_argument(
        "--nCores", help="Number of cores per node.", type=int, default=1
    )
    parser.add_argument(
        "--evgen_runtime",
        help="Event generation job time limit [default: 8 hours]",
        default="08:00:00",
    )
    parser.add_argument(
        "--evgen_memory",
        help="Event generation job memory limit [default: 4 GB]",
        default=4000,
        type=int,
    )
    parser.add_argument(
        "--deriv_runtime",
        help="Derivation job time limit [default: 8 hours]",
        default="08:00:00",
    )
    parser.add_argument(
        "--deriv_memory",
        help="Derivation job memory limit [default: 2.5 GB]",
        default=2500,
        type=int,
    )

    # Pre includes and execs for the event generation
    parser.add_argument(
        "--evgen_postExec", default="", help="Reco_tf --postExec", type=str
    )
    parser.add_argument(
        "--evgen_preExec", default="", help="Reco_tf --preExec", type=str
    )
    parser.add_argument(
        "--evgen_postInclude", default="", help="Reco_tf --postInclude", type=str
    )
    parser.add_argument(
        "--evgen_preInclude", default="", help="Reco_tf --preInclude", type=str
    )
    # Pre includes and execs for the derivation
    parser.add_argument(
        "--deriv_postExec",
        default="",
        help="Reco_tf --postExec in derivation",
        type=str,
    )
    parser.add_argument(
        "--deriv_preExec", default="", help="Reco_tf --preExec in derivation", type=str
    )
    parser.add_argument(
        "--deriv_postInclude",
        default="",
        help="Reco_tf --postInclude in derivation",
        type=str,
    )
    parser.add_argument(
        "--deriv_preInclude",
        default="",
        help="Reco_tf --preInclude in derivation",
        type=str,
    )

    return parser


class EvGenSubmit(object):
    def __init__(
        self,
        cluster_engine=None,
        run_numbers=[],
        hold_jobs=[],
        nJobs=10,
        eventsPerJob=10000,
        evgenCache="",
        evgenRelease="AthGeneration",
        cores_to_use=1,
        memory=1200,
        run_time="12:00:00",
        keep_output=False,
        joboptions_dir="",
        models_dir="",
        preExec="",
        preInclude="",
        postExec="",
        postInclude="",
    ):
        self.__cluster_engine = cluster_engine
        self.__nJobs = nJobs
        self.__events_per_job = eventsPerJob
        self.__ev_gen_cores = cores_to_use

        self.__evgenCache = evgenCache
        self.__evgenRelease = evgenRelease

        self.__preExec = preExec.replace('"', "'")
        self.__preInclude = preInclude.replace('"', "'")

        self.__postExec = postExec.replace('"', "'")
        self.__postInclude = postInclude.replace('"', "'")

        self.__n_scheduled = 0
        self.__run_time = run_time
        self.__mem = memory
        self.__hold_jobs = [h for h in hold_jobs]
        self.__keep_out = keep_output
        self.__joboptions_dir = joboptions_dir
        self.__models_dir = models_dir
        self.__get_job_options(sorted(ClearFromDuplicates(run_numbers)))

    def engine(self):
        return self.__cluster_engine

    def job_name(self):
        return "EVGEN"

    def __get_job_options(self, runNumbers):
        if not self.engine().submit_hook():
            logging.warning(
                "A job with the name {j} has already been submitted.".format(
                    j=self.engine().job_name()
                )
            )
            return

        CreateDirectory(self.engine().config_dir(), True)

        for r in runNumbers:
            jobFolder = os.path.join(
                self.__joboptions_dir, "{ddd}xxx".format(ddd=str(r)[:3])
            )
            if not os.path.isdir(jobFolder):
                logging.warning(
                    "Job option folder {f} for DSID {r} does not exist. Skipping {r}...".format(
                        f=jobFolder, r=r
                    )
                )
                continue
            dir_to_copy = os.path.join(jobFolder, str(r))
            if len(dir_to_copy) == 0:
                continue
            shutil.copytree(
                dir_to_copy, os.path.join(self.engine().config_dir(), str(r))
            )

            # assemble the config file for the job option
            seeds = []
            while len(seeds) < self.__nJobs:
                s = random.uniform(100000, 500000)
                if s not in seeds:
                    seeds += [s]

            jo = [os.path.join(self.engine().config_dir(), str(r))][0]
            out_dir = os.path.join(self.evgen_dir(), str(r))

            WriteList(
                (
                    ReadListFromFile(self.seed_file())
                    if os.path.exists(self.seed_file())
                    else []
                )
                + ["%d" % (i) for i in seeds],
                self.seed_file(),
            )
            WriteList(
                (
                    ReadListFromFile(self.run_file())
                    if os.path.exists(self.run_file())
                    else []
                )
                + ["%d" % (r) for i in range(self.__nJobs)],
                self.run_file(),
            )
            WriteList(
                (
                    ReadListFromFile(self.job_file())
                    if os.path.exists(self.job_file())
                    else []
                )
                + [jo for i in range(self.__nJobs)],
                self.job_file(),
            )
            WriteList(
                (
                    ReadListFromFile(self.out_file())
                    if os.path.exists(self.out_file())
                    else []
                )
                + [out_dir for i in range(self.__nJobs)],
                self.out_file(),
            )

            # submit the job array
            self.__n_scheduled += self.__nJobs
            logging.info("INFO <__get_job_options> Found %s" % (jo))

    def seed_file(self):
        return os.path.join(self.engine().config_dir(), "Seeds.txt")

    def run_file(self):
        return os.path.join(self.engine().config_dir(), "RunNumbers.txt")

    def job_file(self):
        return os.path.join(self.engine().config_dir(), "JobOptionLoc.txt")

    def out_file(self):
        return os.path.join(self.engine().config_dir(), "outDirs.txt")

    def evgen_dir(self):
        return os.path.join(self.engine().base_dir(), "EVNT")

    def slha_dir(self):
        return os.path.join(self.engine().base_dir(), "SLHA")

    def n_scheduled(self):
        return self.__n_scheduled

    def submit_job(self):
        if self.__n_scheduled == 0:
            logging.error("<submit_job>: no jobs have been scheduled.")
            return False
        if not self.engine().submit_build_job():
            logging.error("<submit_job>: no build jobs has been scheduled.")
            return False
        extra_args = ""

        if len(self.__preExec) > 0:
            extra_args += ' --preExec "{p}" '.format(p=self.__preExec)
        if len(self.__preInclude) > 0:
            extra_args += ' --preInclude "{p}" '.format(p=self.__preInclude)
        if len(self.__postExec) > 0:
            extra_args += ' --postExec "{p}" '.format(p=self.__postExec)
        if len(self.__postInclude) > 0:
            extra_args += ' --postInclude "{p}" '.format(p=self.__postInclude)

        print(self.__events_per_job)

        if not self.engine().submit_array(
            sub_job=self.job_name(),
            script="SubmitMC/batch_evgen.sh",
            mem=self.__mem,
            env_vars=[
                ("SeedFile", self.seed_file()),
                ("RunFile", self.run_file()),
                ("JobFile", self.job_file()),
                ("OutFile", self.out_file()),
                ("Keep", str(self.__keep_out)),
                ("EvgenRelease", self.__evgenRelease),
                ("EvgenCache", self.__evgenCache),
                ("ModelsDirectory", self.__models_dir),
                ("NumberOfEvents", self.__events_per_job),
                ("SeedFile", self.seed_file()),
                ("ExtraArgs", extra_args),
            ]
            + (
                [("ATHENA_PROC_NUMBER", "{c}".format(self.__ev_gen_cores))]
                if self.__ev_gen_cores > 1
                else []
            ),
            hold_jobs=self.__hold_jobs,
            run_time=self.__run_time,
            array_size=self.__n_scheduled,
        ):
            return False
        return True


class DerivationSubmit(object):
    def __init__(
        self,
        cluster_engine=None,
        evgen_submit=None,
        derivation="TRUTH1",
        runs=[],
        derivationCache="",
        derivationRelease="AthDerivation",
        memory=4000,
        run_time="8:00:00",
        hold_jobs=[],
        preExec="",
        preInclude="",
        postExec="",
        postInclude="",
    ):
        self.__cluster_engine = cluster_engine
        self.__evgen_submit = evgen_submit

        self.__derivation = derivation
        self.__derivCache = derivationCache
        self.__derivRelease = derivationRelease
        self.__run_time = run_time
        self.__mem = memory
        self.__hold_jobs = [h for h in hold_jobs]
        self.__n_scheduled = 0

        self.__preExec = preExec.replace('"', "'")
        self.__preInclude = preInclude.replace('"', "'")

        self.__postExec = postExec.replace('"', "'")
        self.__postInclude = postInclude.replace('"', "'")
        for r in runs:
            self.__extract_seeds(r)

    def __extract_seeds(self, run):
        try:
            EVNT_DIR = [
                os.path.join(self.evgen_dir(), R)
                for R in os.listdir(self.evgen_dir())
                if R.startswith(str(run))
            ][0]
        except:
            return
        logging.info(
            "<__extract_seeds> Searching {evntdir} for EVNT files not already processed in derivation format {d}.".format(
                evntdir=EVNT_DIR, d=self.__derivation
            )
        )

        DERIVATION_DIR = os.path.join(
            self.aod_dir(), EVNT_DIR[EVNT_DIR.rfind("/") + 1 :]
        )
        CreateDirectory(DERIVATION_DIR, False)
        Evnt_Seeds = [
            int(E[E.find("EVNT") + 5 : E.find(".pool")])
            for E in os.listdir(EVNT_DIR)
            if E.endswith(".root")
        ]
        DAOD_Seeds = [
            int(A.split(".")[-2])
            for A in os.listdir(DERIVATION_DIR)
            if A.find(self.__derivation) != -1 and A.endswith(".root")
        ]
        Non_ProcSeeds = [seed for seed in Evnt_Seeds if seed not in DAOD_Seeds]
        if len(Non_ProcSeeds) == 0:
            return
        logging.info("Extracted seeds for run {r}:".format(r=run))
        logging.info("   +-=- {s}".format(s=", ".join([str(seed) for seed in Non_ProcSeeds])))

        WriteList(
            (ReadListFromFile(self.seed_file()) if os.path.exists(self.seed_file()) else []) + [str(seed) for seed in Non_ProcSeeds],
            self.seed_file(),
        )
        WriteList(
            (ReadListFromFile(self.run_file()) if os.path.exists(self.run_file()) else []) + [str(run) for seed in Non_ProcSeeds],
            self.run_file(),
        )
        WriteList(
            (ReadListFromFile(self.in_file()) if os.path.exists(self.in_file()) else []) + [EVNT_DIR for seed in Non_ProcSeeds],
            self.in_file(),
        )
        self.__n_scheduled += len(Non_ProcSeeds)

    def hold_jobs(self):
        if self.evgen():
            return [(self.engine().subjob_name(self.evgen().job_name()))]
        return []

    def n_scheduled(self):
        if self.evgen():
            return self.evgen().n_scheduled()
        return self.__n_scheduled

    def engine(self):
        return self.__cluster_engine

    def evgen(self):
        return self.__evgen_submit

    def seed_file(self):
        if self.evgen():
            return self.evgen().seed_file()
        return os.path.join(
            self.engine().config_dir(), "AOD_{d}_Seeds.txt".format(d=self.__derivation)
        )

    def run_file(self):
        if self.evgen():
            return self.evgen().run_file()
        return os.path.join(
            self.engine().config_dir(), "AOD_{d}_runs.txt".format(d=self.__derivation)
        )

    def evgen_dir(self):
        return os.path.join(self.engine().base_dir(), "EVNT/")

    def aod_dir(self):
        return os.path.join(self.engine().base_dir(), "TRUTH/")

    def in_file(self):
        if self.evgen():
            return self.evgen().out_file()
        return os.path.join(
            self.engine().config_dir(), "AOD_{d}_inDirs.txt".format(d=self.__derivation)
        )

    def out_file(self):
        return os.path.join(
            self.engine().config_dir(),
            "AOD_{d}_outDirs.txt".format(d=self.__derivation),
        )

    def submit_job(self):
        WriteList(
            [
                D.replace(self.evgen_dir(), self.aod_dir())
                for D in ReadListFromFile(self.in_file())
            ],
            self.out_file(),
        )

        extra_args = ""
        if len(self.__preExec) > 0:
            extra_args += ' --preExec "%s" ' % (self.__preExec)
        if len(self.__preInclude) > 0:
            extra_args += ' --preInclude "%s" ' % (self.__preInclude)
        if len(self.__postExec) > 0:
            extra_args += ' --postExec "%s" ' % (self.__postExec)
        if len(self.__postInclude) > 0:
            extra_args += ' --postInclude "%s" ' % (self.__postInclude)

        if not self.engine().submit_array(
            sub_job=self.__derivation,
            script="SubmitMC/batch_derivation.sh",
            mem=self.__mem,
            env_vars=[
                ("SeedFile", self.seed_file()),
                ("RunFile", self.run_file()),
                ("InFile", self.in_file()),
                ("OutFile", self.out_file()),
                ("DERIVATION_DIR", self.aod_dir()),
                ("DerivationRelease", self.__derivRelease),
                ("DerivationCache", self.__derivCache),
                ("ReductionConf", self.__derivation),
                ("ExtraArgs", extra_args),
            ],
            hold_jobs=self.hold_jobs(),
            run_time=self.__run_time,
            array_size=self.n_scheduled(),
        ):
            return False
        return True


def main():
    RunOptions = getArguments().parse_args()
    runNumbersList = [R for R in RunOptions.runNumbers]
    if RunOptions.runRange:
        if len(RunOptions.runRange) == 2:
            runNumbersList += [
                i for i in range(RunOptions.runRange[0], RunOptions.runRange[-1] + 1)
            ]
        else:
            sys.exit("ERROR: Please give at least 2 MC sample to define a range")

    if len(runNumbersList) == 0:
        sys.exit("ERROR: Please give at least one MC sample to generate")

    cluster_engine = setup_engine(RunOptions)
    evgen_submit = EvGenSubmit(
        cluster_engine=cluster_engine,
        run_numbers=runNumbersList,
        nJobs=RunOptions.nJobs,
        eventsPerJob=RunOptions.eventsPerJob,
        keep_output=RunOptions.keepOutput,
        joboptions_dir=RunOptions.jobOptionsDir,
        models_dir=RunOptions.modelsDir,
        cores_to_use=RunOptions.nCores,
        evgenRelease="AthGeneration",
        evgenCache=RunOptions.AthGeneration,
        memory=RunOptions.evgen_memory,
        run_time=RunOptions.evgen_runtime,
        preInclude=RunOptions.evgen_preInclude,
        postInclude=RunOptions.evgen_postInclude,
        preExec=RunOptions.evgen_preExec,
        postExec=RunOptions.evgen_postExec,
    )
    if not evgen_submit.submit_job():
        exit(1)

    for derivation in RunOptions.derivations:
        daod_submit = DerivationSubmit(
                     cluster_engine = cluster_engine,
                     evgen_submit = evgen_submit,
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
    hold_jobs = [cluster_engine.subjob_name(evgen_submit.job_name())] + [
                 cluster_engine.subjob_name(D) for D in RunOptions.derivations]
    cluster_engine.submit_clean_all(hold_jobs)

    # schedule jobs
    cluster_engine.print_banner()
    cluster_engine.finish()


if __name__ == "__main__":
    main()
