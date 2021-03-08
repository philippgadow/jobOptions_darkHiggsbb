#! /usr/bin/env python
from ClusterSubmission.RucioListBuilder import GetDataSetFiles, downloadDataSets
from ClusterSubmission.ListDisk import ListDataFilesWithSize
from ClusterSubmission.Utils import CheckRemainingProxyTime, CreateDirectory, ReadListFromFile, IsROOTFile
import os, math, logging, ROOT


### Helper class to download rucio containers and merge the
### files into batches each with a size of n GB
class DataSetFileHandler(object):
    def __init__(
        self,
        #### Container with all dataset names
        rucio_container,
        ### RSE where the container is stored
        dest_rse,
        #### Download the container to the disk
        download=False,
        #### Merge the datasets to a common file
        merge=False,
        ## Download directory
        download_dir="/tmp/download",
        #### Destination_dir
        destination_dir="/tmp",
        #### Cluster_engine for potential merge
        cluster_engine=None,
        #### max_size_per_merged_file (B)
        max_merged_size=25 * 1024 * 1024 * 1024,
        ### Logical dataset_name (optional)
        logical_name="",
        #### Rucio groupdisk protocol
        protocol="root",
        ## hold jobs
        hold_jobs=[],
        ### Files per merge job
        files_per_merge_job=20,
    ):

        self.__container_name = rucio_container
        self.__rse = dest_rse

        self.__download = download
        self.__merge = merge

        self.__download_dir = download_dir
        self.__files_per_merge = files_per_merge_job
        while self.__download_dir.find("//") != -1:
            self.__download_dir = self.__download_dir.replace("//", "/")
        self.__dest_dir = destination_dir

        self.__engine = cluster_engine
        self.__max_file_size = max_merged_size

        self.__logical_name = logical_name
        CheckRemainingProxyTime()
        self.__files_on_rse = [f for f in GetDataSetFiles(self.container(), self.rse(), protocol)
                               if self._is_good_file(f)] if len(rucio_container) > 0 and not self.__download else []
        #### List of files to be downloaded on disk
        self.__files_on_disk = []
        if self.__download:
            CreateDirectory(self.ds_download_dir(), False)
            downloadDataSets(InputDatasets=[self.container()], Destination=self.__download_dir, use_singularity=True)
            self.__files_on_disk = [
                "%s/%s" % (self.ds_download_dir(), f) for f in os.listdir(self.ds_download_dir())
                if self._is_good_file(self.ds_download_dir() + "/" + f)
            ]
        self.__merge_interfaces = []
        self.__hold_jobs = hold_jobs

    def _is_good_file(self, f):
        return IsROOTFile(f)

    def download_ds(self):
        return self.__download

    def ds_download_dir(self):
        return "%s/%s" % (self.__download_dir, self.container(True))

    def ds_final_dir(self):
        if not self.__merge: return self.ds_download_dir()
        return "%s/%s" % (self.__dest_dir, self.logical_name())

    def logical_name(self):
        if len(self.__logical_name) == 0: return self.container(True)
        return self.__logical_name

    def container(self, no_scope=False):
        return self.__container_name[self.__container_name.find(":") + 1 if no_scope else 0:]

    def rse(self):
        return self.__rse

    def engine(self):
        return self.__engine

    def root_files(self):
        if not self.__download: return self.__files_on_rse
        return self.__files_on_disk

    def merge(self):
        return self.__merge

    def max_file_size(self):
        return self.__max_file_size

    def merged_files(self):
        if not self.__merge: return self.root_files()
        self.prepare_merge()
        return ["%s/%s.root" % (self.ds_final_dir(), merge.outFileName()) for merge in self.__merge_interfaces]

    def set_hold_jobs(self, jobs):
        self.__hold_jobs = [J for J in jobs]

    def hold_jobs(self):
        return self.__hold_jobs

    def _get_good_files(self, in_files):
        return in_files

    def ds_files_with_size(self):
        files_with_size = []
        if self.__download:
            files_with_size = [(x, os.path.getsize(x)) for x in self.root_files()]
        else:
            for r in self.root_files():
                t_file = ROOT.TFile.Open(r, "READ")
                if not t_file or not t_file.IsOpen(): continue
                files_with_size += [(r, t_file.GetSize())]
                t_file.Close()
        files_with_size.sort(key=lambda x: x[1], reverse=True)
        return files_with_size

    def prepare_merge(self):
        if not self.__merge or not self.engine() or len(self.root_files()) == 0:
            return
        if len(self.__merge_interfaces) > 0: return
        files_with_size = self.ds_files_with_size()
        if len(files_with_size) == 0: return
        mean = sum([f[1] for f in files_with_size]) / len(files_with_size)
        sigma = math.sqrt(sum([(f[1] - mean)**2 for f in files_with_size])) / len(files_with_size)

        file_clusters = [[]]

        for x, s in files_with_size:
            ### The n-tuple structure is different
            if self._split_file(x): file_clusters += [[]]
            file_clusters[-1] += [(x, s)]
        for cluster in file_clusters:
            #### Add everything together
            while len(cluster) > 0:
                to_add = []
                batch_size = 0
                i = 0
                while i < len(cluster) and batch_size <= self.__max_file_size:
                    if len(to_add) == 0 or batch_size + cluster[i][1] <= self.__max_file_size + sigma:
                        to_add += [cluster[i][0]]
                        batch_size += cluster[i][1]
                    i += 1
                self.__merge_interfaces += [
                    self.engine().create_merge_interface(out_name="%s_%d" % (self.logical_name(), len(self.__merge_interfaces)),
                                                         files_to_merge=to_add,
                                                         files_per_job=self.__files_per_merge,
                                                         hold_jobs=self.hold_jobs())
                ]
                ### Kick what's been merged from the list
                cluster = [f for f in cluster if f[0] not in to_add]
        logging.info("The dataset %s is going to be merged into %d files" % (self.logical_name(), len(self.__merge_interfaces)))

    def _split_file(self, x):
        return False

    def submit_merge(self):
        self.prepare_merge()
        ### No merging interfaces are made indeed
        if len(self.__merge_interfaces) == 0: return True
        ### Make sure that the final directory is empty before merging
        if len(self.container(True)) > 0: CreateDirectory(self.ds_final_dir(), True)
        hold_jobs = []
        ### merge jobs are submitted
        for merge in self.__merge_interfaces:
            if not merge.submit_job(): return False
            hold_jobs += [self.engine().subjob_name("merge-%s" % (merge.outFileName()))]
        if self.__download and not self.engine().submit_clean_job(
                hold_jobs=hold_jobs, to_clean=[self.ds_download_dir()], sub_job=self.logical_name()):
            return False

        return self.engine().submit_move_job(
            hold_jobs=hold_jobs + [self.engine().subjob_name("Clean-%s" % (self.logical_name()))],
            to_move=["%s/%s.root" % (self.engine().out_dir(), merge.outFileName())
                     for merge in self.__merge_interfaces],  ### Give particular files to move
            destination=self.ds_final_dir(),
            sub_job=self.logical_name())

    def submit_job(self):
        return self.submit_merge()

    def job_name(self):
        return "Move-" + self.logical_name()


class RucioContainerHandler(DataSetFileHandler):
    def __init__(
        self,
        ### RSE where the container is stored
        dest_rse,
        #### Download the container to the disk
        download=False,
        #### Merge the datasets to a common file
        merge=False,
        ## Download directory
        download_dir="/tmp/download",
        #### Destination_dir
        destination_dir="/tmp",
        #### Cluster_engine for potential merge
        cluster_engine=None,
        #### max_size_per_merged_file (B)
        max_merged_size=25 * 1024 * 1024 * 1024,
        #### Rucio groupdisk protocol
        protocol="root",
        ## hold jobs
        hold_jobs=[],
        ### Logical dataset name
        logical_name="",
    ):

        DataSetFileHandler.__init__(
            self,
            rucio_container="",
            dest_rse=dest_rse,
            download=False,
            merge=merge,
            download_dir=download_dir,
            destination_dir=destination_dir,
            cluster_engine=cluster_engine,
            max_merged_size=max_merged_size,
            logical_name=logical_name,
            protocol=protocol,
            hold_jobs=hold_jobs,
        )
        self.__download = download
        self.__protocol = protocol
        self.__containers = []

    def add_container(self, container_name):
        ### Rucio container already added
        if container_name in self.container(): return
        self.__containers += [
            DataSetFileHandler(
                rucio_container=container_name,
                dest_rse=self.rse(),
                download=self.__download,
                download_dir=self.ds_download_dir(),
                cluster_engine=self.engine(),
                max_merged_size=self.max_file_size(),
                logical_name="",
                protocol=self.__protocol,
            )
        ]

    def rucio_container(self, no_scope=False):
        return [c.container(no_scope=no_scope) for c in self.__containers]

    def root_files(self):
        f_list = []
        for c in self.__containers:
            f_list += c.root_files()
        return f_list

    def ds_files_with_size(self):
        f_list = []
        for c in self.__containers:
            f_list += c.ds_files_with_size()
        return f_list
