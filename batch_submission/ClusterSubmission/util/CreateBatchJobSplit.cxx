#include <POOLRootAccess/TEvent.h>
#include <TFile.h>
#include <TSystem.h>
#include <xAODRootAccess/Init.h>
#include <xAODRootAccess/TEvent.h>

#include <fstream>
#include <functional>
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <thread>
bool GetLine(std::ifstream &inf, std::string &line);
void FillVectorFromString(std::set<std::string> &str_vector, std::string &str);
std::string EraseWhiteSpaces(std::string str);

int main(int argc, char *argv[]) {
    unsigned long long EventsPerJob = 10000;
    std::string InFileList = "";
    std::string outFile = "";
    std::set<std::string> Files;
    std::ofstream outList;
    // Set up the job for xAOD access:
    if (!xAOD::Init("CreateBatchJobSplit").isSuccess()) {
        Error("CreateBatchJobSplit", "Could not setup xAOD");
        return EXIT_FAILURE;
    }
    // Reading the Arguments parsed to the executable
    for (int a = 1; a < argc; ++a) {
        std::string the_arg = argv[a];
        if ((the_arg.find("-I") == 0 || the_arg.find("--inList") == 0) && (a + 1) != argc)
            InFileList = argv[a + 1];
        else if ((the_arg.find("-O") == 0 || the_arg.find("--outFile") == 0) && (a + 1) != argc)
            outFile = argv[a + 1];
        else if ((the_arg.find("-EpJ") == 0 || the_arg.find("--eventsPerJob") == 0) && (a + 1) != argc)
            EventsPerJob = atoi(argv[a + 1]);
    }
    std::ifstream list(InFileList);
    if (!list.good()) {
        Error("CreateBatchJobSplit()", "Could not read the FileList: %s", InFileList.c_str());
        return EXIT_FAILURE;
    } else {
        std::string Line;
        while (GetLine(list, Line)) { FillVectorFromString(Files, Line); }
    }
    if (Files.empty()) {
        Error("CreateBatchJobSplit()", "No files to split were given ");
        return EXIT_FAILURE;
    }
    Info("CreateBatchJobSplit", "Read in of list done.. Now Checking files");
    if (outFile.empty()) {
        Error("CreateBatchJobSplit", "Please provide an outFile to the script");
        return EXIT_FAILURE;
    }
    if (outFile.find("/") != std::string::npos) { gSystem->mkdir(outFile.substr(0, outFile.rfind("/")).c_str(), true); }
    outList.open(outFile);
    auto return_failure = [&outFile, &outList]() {
        outList.close();
        system(Form("rm -f %s", outFile.c_str()));
        return EXIT_FAILURE;
    };
    if (!outList.good()) {
        Error("CreateBatchJobSplit()", "Could not open the output list");
        return return_failure();
    }

    POOL::TEvent m_event(POOL::TEvent::kClassAccess);

    size_t f(1), files_in_batch(0);
    unsigned long long total_events = 0;
    unsigned long long events_in_batch = 0;

    for (auto &filename : Files) {
        Info("CreateBatchJobSplit", "Check file (%lu/%lu): %s", f, Files.size(), filename.c_str());
        std::shared_ptr<TFile> File(TFile::Open(filename.c_str(), "READ"));
        if (!File || !File->IsOpen()) {
            Error("CreateBatchJobSplit", "The file %s could not be opened", filename.c_str());
            return return_failure();
        }
        if (!m_event.readFrom(File.get()).isSuccess()) {
            Error("CreateBatchJobSplit", "Could not read in the File");
            return return_failure();
        }
        unsigned long long EventsInFile = m_event.getEntries();
        if (EventsInFile == 0) { Warning("CreateBatchJobSplit", "The file contains 0 Events."); }

        /// Check whether that is the first file or not
        if (files_in_batch == 0) {
            outList << " --filesInput ";
        } else {
            outList << ",";
        }
        outList << filename;
        ++files_in_batch;
        total_events += EventsInFile;
        /// The batch is full. It's time to create a new one
        unsigned long long ev_updated = events_in_batch + EventsInFile;
        if (ev_updated == EventsPerJob) {
            outList << std::endl;
            events_in_batch = 0;
            files_in_batch = 0;

        } else if (ev_updated < EventsPerJob) {
            events_in_batch = ev_updated;
        } else {
            unsigned int multiplier = 1;
            while (true) {
                /// Close the batch and limit the number of processed events
                outList << " --evtMax " << EventsPerJob << std::endl;
                /// The next file must skip the first nEventsPerJob
                outList << " --skipEvents " << (multiplier * EventsPerJob - events_in_batch) << " --filesInput " << filename;
                ev_updated -= EventsPerJob + events_in_batch;
                events_in_batch = 0;
                if (ev_updated > EventsPerJob) {
                    ++multiplier;
                } else if (ev_updated == EventsPerJob) {
                    outList << std::endl;
                    files_in_batch = 0;
                    break;
                } else {
                    events_in_batch = ev_updated;
                    break;
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        ++f;
    }
    /// Complete the last line in the splitting
    outList.close();
    return EXIT_SUCCESS;
}
bool GetLine(std::ifstream &inf, std::string &line) {
    if (!std::getline(inf, line)) return false;
    line = EraseWhiteSpaces(line);
    if (line.find("#") == 0 || line.size() < 1) return GetLine(inf, line);
    return true;
}
void FillVectorFromString(std::set<std::string> &str_vector, std::string &str) {
    str += ",";  // adding comma in the end to make the 'while' working for the last element
    while (str.find(",") != std::string::npos) {
        std::size_t Pos = str.find(",");
        std::string Data = str.substr(0, Pos);
        if (!Data.empty()) str_vector.insert(Data);
        str = str.substr(Pos + 1, str.size());
    }
    str.clear();
}
std::string EraseWhiteSpaces(std::string str) {
    str.erase(std::remove(str.begin(), str.end(), '\t'), str.end());
    if (str.find(" ") == 0) return EraseWhiteSpaces(str.substr(1, str.size()));
    if (str.size() > 0 && str.find(" ") == str.size() - 1) return EraseWhiteSpaces(str.substr(0, str.size() - 1));
    return str;
}
