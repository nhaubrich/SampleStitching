# SampleStitching
## setup
Clone this repo, `cd SampleStitching`, then
```
curl -OL https://raw.githubusercontent.com/CoffeaTeam/lpcjobqueue/main/bootstrap.sh
bash bootstrap.sh
```

## Running
With proper grid certificate, start singularity with `./shell`. Fetch sample paths and compute total event weights with `python FetchSamplePaths.py`. Compute stitching for a set of samples with `python ComputeStitchWeights.py -c Wln2017`, then produce a validation plot by running `python ComputeStichWeights.py -c Wln2017 -s stitchingResults.pickle`.
