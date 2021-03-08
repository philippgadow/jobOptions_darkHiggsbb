## Development of dark Higgs(bb) job options in Atlas MC Generation releases 21.6.23 and later


### Run this example quickly
Run in release `21.6.58` (CC7)

```
git clone git@github.com:philippgadow/jobOptions_darkHiggsbb.git
cd jobOptions_darkHiggsbb
source setup.sh
source run.sh
```


### Batch submission
On DESY NAF run:

```
source setup_batch.sh
source run_batch.sh
```

### Modify job option

Take the job option [`100xxx/100000/MadGraphControl_MadGraphPythia8_N31LO_A14N23LO_monoSbb_CKKWL.py`](https://github.com/philippgadow/jobOptions_darkHiggsbb/blob/master/100xxx/100000/MadGraphControl_MadGraphPythia8_N31LO_A14N23LO_monoSbb_CKKWL.py) to implement your changes for testing.
All other copies of that job option are symlinks to this file.
