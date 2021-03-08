# ClusterSubmission

Package containing infrastructure for submitting jobs to all kind of clusters.

Introduction
--- 

This package is meant to hold a common infrastructure for submitting jobs to all kind of clusters. Thereby, _cluster_ can refer to a computing cluster at a Tier-2 site but also a local multi-core machine to which one wants to submit jobs running in multiple threads.

Supported batch systems
---

The main functionality of this package is encoded inside `python/ClusterEngine.py`. Currently supported batch systems are:

1) Slurm
2) Local machine
3) HTCondor


FAQ
---

**Q:** I have a project in which I include both the `XAMPPplotting` and `XAMPPbase` projects as submodules. When I build the project and modify the python scripts belonging to `ClusterSubmission` hosted in `XAMPPbase`, they show no effect.

**A:** Since you have two packages with the name `ClusterSubmission`, when you build the project with `cmake`, only the last built `ClusterSubmission` package will be linked in the build directory. In your case this is most likely the one hosted in `XAMPPplotting`. You can find out which files are linked by entering 

```ls -l <path to top level directory>/build/*/python/ClusterSubmission```

Help
---

Feel free to contribute and support your favourite type of batch systems. Contact:
* jojungge@cern.ch
* nkoehler@cern.ch
*   pgadow@cern.ch
