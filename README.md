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

Take the job option [`110xxx/110000/MadGraphControl_MadGraphPythia8_N31LO_A14N23LO_monoSbb_CKKWL.py`](https://github.com/philippgadow/jobOptions_darkHiggsbb/blob/master/110xxx/110000/MadGraphControl_MadGraphPythia8_N31LO_A14N23LO_monoSbb_CKKWL.py) to implement your changes for testing.
All other copies of that job option are symlinks to this file.


### Reweight module test

The job option in `110xxx/110000` provides MadGraph reweighting. As a consequence, LHE event weights are written to the output file. These should allow for reweighting the signal to different coupling values of `gx`.
The range from `gx` in `[0.1, 0.2, ...,  3.5]` is scanned in steps of `0.1`. The upper boundary is motivated by the perturbativity bound `gx < (4*pi)^0.5`.

For validation of the reweight module, job options in `111xxx/` are provided. These are generated with different values of `gx` but without reweighting.
The validation is performed by comparing the reweighted sample generated with `110xxx/110000` using the respective weights with the generated samples in `111xxx/`.

