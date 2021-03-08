#! /usr/bin/env python
from __future__ import print_function
from ClusterSubmission.Utils import ClearFromDuplicates, ResolvePath, prettyPrint
import argparse
import json
import logging
logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)

m_PeriodRunConverter = None
m_GRLdict = None


class Period(object):
    def __init__(self, year, period, project):
        self.__year = year
        self.__period = period
        self.__project = project
        self.__runs = []

    def period(self):
        return self.__period

    def year(self):
        return self.__year

    def project(self):
        return self.__project

    def addRun(self, R):
        if not int(R) in self.__runs:
            self.__runs += [int(R)]

    def finish(self):
        self.__runs = sorted(self.__runs)

    def runs(self):
        return self.__runs


class PeriodRunConverter(object):
    def __init__(self):
        self.__Periods = {}

    def GetYears(self):
        return sorted([Y for Y in self.__Periods.iterkeys()])

    def __LoadPeriodsFromAmi(self, Y):
        from ClusterSubmission.AMIDataBase import getAmiClient
        getAmiClient()

        import pyAMI.client
        import pyAMI.atlas.api as AtlasAPI

        if Y in self.GetYears(): return

        self.__Periods[Y] = []
        PeriodDict = AtlasAPI.list_dataperiods(getAmiClient(), 1, year=Y)

        for period_entry in PeriodDict:
            P = str(period_entry["period"])
            ### We do not need the Van-Der-Meer scan periods. They are not of any use for
            ### data analysis
            if P.find("VdM") != -1: continue
            period_storage = Period(year=Y, period=P, project=period_entry["projectName"])
            self.__Periods[Y] += [period_storage]

        ### Now load the runs from the total year
        RunDict = AtlasAPI.list_runs(getAmiClient(), data_periods=[], year=Y)
        for run in RunDict:
            if run["period"].find("VdM") != -1: continue
            ## Order them into the period holders
            R = int(run['runNumber'])
            storage = self.__FindPeriod(Y=Y, period=run["period"], project=run["projectName"])
            if storage: storage.addRun(R)

        for P in self.__Periods[Y]:
            P.finish()
        ### Sort the periods finally
        self.__Periods[Y] = sorted(self.__Periods[Y], key=lambda P: P.period())
        logging.info("Found %d runs in year %d" % (len(self.GetRunsFromPeriod(Y)), Y))

    def __FindPeriod(self, Y, period, project):
        if not Y in self.GetYears():
            logging.error("The year %d is not yet known" % (Y))
            return None
        for storage in self.__Periods[Y]:
            if storage.period() == period and storage.project() == project: return storage
        logging.warning("Period %s in year %d is unknown" % (period, Y))
        return None

    def GetPeriods(self, Y):
        if Y > 2000: Y -= 2000
        self.__LoadPeriodsFromAmi(Y)
        return sorted(ClearFromDuplicates([P.period()[0] for P in self.__Periods[Y]]))

    def GetSubPeriods(self, Y, project=None):
        if Y > 2000: Y -= 2000
        self.__LoadPeriodsFromAmi(Y)
        return sorted(
            ClearFromDuplicates([P.period() for P in self.__Periods[Y] if project == None or P.project().split("_")[-1] == project]))

    def GetRunsFromPeriod(self, Y, P=None, project="13TeV"):
        Runs = []
        if Y > 2000: Y -= 2000
        # Get everything what's in that year
        if not P:
            for P in self.GetPeriods(Y):
                Runs += self.GetRunsFromPeriod(Y, P, project)

        letters = len(P)
        ### The user has given something like 16 AB or 15 DE
        if letters == 2 and not P[1].isdigit():
            return sorted(self.GetRunsFromPeriod(Y, P[0], project) + self.GetRunsFromPeriod(Y, P[1], project))
        if letters <= 2:
            for storage in self.__Periods[Y]:
                if project != None and storage.project().split("_")[-1] != project: continue
                ### Top period is asked for
                if letters == 1 and storage.period()[0] != P:
                    continue
                    ### Usually subperiods have one letter and one digit
                elif letters == 2 and storage.period() != P:
                    continue
                Runs += storage.runs()
            return sorted(Runs)

        elif len(P) > 1:
            letters = [i for i in range(len(P)) if not P[i].isdigit()]
            for i in range(len(letters)):
                x = letters[i]
                x_1 = letters[i + 1] if i + 1 < len(letters) else len(P)
                if x_1 - x > 1:
                    for y in range(x + 1, x_1):
                        Runs += self.GetRunsFromPeriod(Y, P[x:y], project)
                else:
                    Runs += self.GetRunsFromPeriod(Y, P[x:x_1], project)
        return sorted(Runs)

    def GetPeriodElement(self, Run):
        for Y in self.GetYears():
            for P in self.__Periods[Y]:
                if Run in P.runs(): return P
        logging.warning("Run %d is not known to AMI. Are you sure the thing exists?" % (Run))
        return None

    def GetPeriodFromRun(self, Run):
        PeriodElement = self.GetPeriodElement(Run)
        ### Period elements return A1. B2, C4, etc.
        ##  We'd like to have the top period
        if PeriodElement: return PeriodElement.year(), PeriodElement.period()[0]
        logging.warning("No period could be found for run %d. Are you sure that the thing exists?" % (Run))
        return 0, ""

    def GetProject(self, Run):
        PeriodElement = self.GetPeriodElement(Run)
        if PeriodElement: return Y, PeriodElement.project()
        logging.warning("No period could be found for run %d. Are you sure that the thing exists?" % (Run))
        return "Unknown"

    def GetFirstRunLastRun(self, Y, P=None, project="13TeV"):
        if len(self.GetPeriods(Y)) == 0:
            return 0, 0
        Runs = self.GetRunsFromPeriod(Y, P, project)
        if len(Runs) == 0:
            return 0, 0
        return Runs[0], Runs[-1]


def GetPeriodKeys(first_year=2015, last_year=2018):
    Keys = ["2015_DEF", "2015_H23J"] if first_year == 2015 else []
    for y in range(first_year, last_year + 1):
        Periods = GetPeriodRunConverter().GetPeriods(y) + [""]
        for P in Periods:
            Keys.append("%i" % (y) if len(P) == 0 else "%i_%s" % (y, P))
    return sorted(Keys, key=lambda x: len(x))


def GetPeriodRunConverter():
    global m_PeriodRunConverter
    if not m_PeriodRunConverter:
        m_PeriodRunConverter = PeriodRunConverter()
        for i in range(15, 19):
            GetPeriodRunConverter().GetPeriods(i)
    return m_PeriodRunConverter


def GetPeriods(Y):
    return GetPeriodRunConverter().GetPeriods(Y)


def RunInPeriod(runNumber, year, period):
    return int(runNumber) in GetRunsFromPeriod(year, period)


def GetPeriodFromRun(runNumber):
    return GetPeriodRunConverter().GetPeriodFromRun(runNumber)


def GetRunsFromPeriod(year, period):
    Runs = []
    myPeriods = []
    if isinstance(year, list): myYears = year
    elif isinstance(year, int): myYears = [year]
    else:
        logging.error("GetRunsFromPeriod(runNumber,period,year) needs a list or an integer for year, exiting...")
        exit(1)
    if isinstance(period, list): myPeriods = period
    elif isinstance(period, str) or isinstance(period, unicode): myPeriods = [period]
    else:
        print(type(period), period)
        logging.error("GetRunsFromPeriod(runNumber,period,year) needs a list or a string for period, exiting...")
        exit(1)
    for y in myYears:
        for p in myPeriods:
            if len(p) == 0: Runs.extend(GetPeriodRunConverter().GetRunsFromPeriod(y))
            elif len(p) == 1: Runs.extend(GetPeriodRunConverter().GetRunsFromPeriod(y, p))
            else:
                for x in range(1, len(p)):
                    if p[x].isdigit(): Runs.extend(GetPeriodRunConverter().GetRunsFromPeriod(y, p[0] + p[x]))
                    else: Runs.extend(GetPeriodRunConverter().GetRunsFromPeriod(y, p[x]))
        if len(myPeriods) == 0: Runs.extend(GetPeriodRunConverter().GetRunsFromPeriod(y))
    return sorted(Runs)


def getGRL(year=[15, 16, 17, 18], flavour='GRL', config='ClusterSubmission/GRL.json'):
    """Get from json file either 
    - default Good Run Lists (flavour='GRL') or 
    - default lumi calc files (flavour='lumiCalc') or 
    - default actual mu pile-up reweigthing files (flavour='actualMu'))
    as a list of strings. Can be called without arguments to give just GRLs 
    for all years or with a specific (list of) year(s).
    Default input is config='ClusterSubmission/GRL.json'
    """
    if isinstance(year, list): myYears = ClearFromDuplicates([str(y) for y in year if y < 100] + [str(y - 2000) for y in year if y > 2000])
    elif isinstance(year, int) or isinstance(year, str): myYears = [str(year)] if year < 100 else [str(year - 2000)]
    global m_GRLdict
    if not m_GRLdict: m_GRLdict = json.load(open(ResolvePath(config), 'r'))
    try:
        if flavour == 'actualMu' and ('15' in myYears or '16' in myYears):
            logging.warning("actual mu PRW is only avaliable for data17 and data18.")
            if not ('17' in myYears or '18' in myYears):
                logging.error("The request is ill-defined and does not make sense.")
                raise NameError('actual mu PRW is only avaliable for data17 and data18, not for data15 or data16')
        return [str(value) for key, value in m_GRLdict[flavour].items() if (value and key in ['data' + y for y in myYears])]
    except Exception as e:
        logging.error("Error when accessing GRL/lumiCalc/actualMu information!")
        raise (e)


def SetupArgParser(parser):
    parser.add_argument('-r',
                        '-R',
                        '--runNumber',
                        help='specify a runNumber to be converted into a period',
                        nargs='+',
                        default=[],
                        type=int)
    parser.add_argument('-p', '-P', '--period', help='specify a period to be converted into a list of runs', nargs='+', default=[])
    parser.add_argument('-y', '-Y', '--year', help='specify a year used for the conversion', nargs='+', default=[15, 16, 17, 18], type=int)
    parser.add_argument("--project", help="Do you aim for 13TeV/cos/hi/5TeV", default="13TeV")
    return parser


def main():
    """Return runs for a given period or vice versa."""
    parser = argparse.ArgumentParser(
        description='This script returns the runs for a given period or vice versa. For more help type \"python PeriodRunConverter.py -h\"',
        prog='PeriodRunConverter',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser = SetupArgParser(parser)
    Options = parser.parse_args()
    GetPeriodRunConverter()
    for Y in Options.year:
        prettyPrint(preamble="Found the following periods for year", data="%d" % (Y), width=30, separator=":")

        for P in GetPeriods(Y):
            First, Last = GetPeriodRunConverter().GetFirstRunLastRun(Y, P, Options.project)
            prettyPrint(preamble="Period %s from " % (P), data="%d --- %d" % (First, Last), width=40, separator="***")

    if len(Options.runNumber) > 0 and len(Options.period) > 0:
        logging.info('Both runNumber (%s) and period (%s) were given, checking if given runs are included in given periods...' %
                     (Options.runNumber, Options.period))
        logging.info(RunInPeriod(Options.runNumber, Options.year, Options.period))
    elif len(Options.runNumber) > 0 and len(Options.period) == 0:
        logging.info('A runNumber (%s) was given, checking corresponding period...' % (Options.runNumber))
        logging.info(GetPeriodFromRun(Options.runNumber[0]))
    elif len(Options.runNumber) == 0 and len(Options.period) > 0:
        logging.info('A period (%s) was given, checking corresponding runs for year(s) %s ...' % (Options.period, Options.year))
        logging.info(" ".join(["%d" % (r) for r in GetRunsFromPeriod(Options.year, Options.period)]))
    else:
        logging.error('Please specify at least one runNumber or one period. For help use "python PeriodRunConverter.py -h"')


if __name__ == "__main__":
    main()
