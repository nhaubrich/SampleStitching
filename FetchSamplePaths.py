import subprocess
import json
import coffea
import coffea.processor as processor
from coffea.nanoevents import BaseSchema
from coffea import hist,nanoevents
import time

import matplotlib.pyplot as plt

from distributed import Client
from lpcjobqueue import LPCCondorCluster

#write json file with all the sample info: filepaths, xsec, sumw
#{ channel: 
#   sample:
#       filelist: []
#       xsec:
#       sumw:
#}

path = "/eos/cms/store/group/phys_higgs/hbb/ntuples/VHbbPostNano/2017/"

Vpt_axis = hist.Bin("Vpt", "LHE_Vpt [GeV]", 40, 0, 500)
VptBins_axis = hist.Bin("VptBins", "LHE_Vpt [GeV]", [0,50,100,150,250,400,2000])
NpNLO_axis = hist.Bin("NpNLO","NpNLO",3,0,3)

def xrdfs(fullpath):
    itemlist = [ filename for filename in subprocess.check_output(['xrdfs',"root://eoscms.cern.ch",'ls','-R',fullpath]).decode().split("\n")]
    rootfiles = [f for f in itemlist if f.endswith(".root")]
    return rootfiles 

class Sample():
    def __init__(self,name,xsec):
        self.name = name
        self.xsec = xsec
        self.genEventSum = 0
        
        badFileNames = ["/eos/cms/store/group/phys_higgs/hbb/ntuples/VHbbPostNano/2017/V11/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/RunIIFall17NanoAODv4-PU2017_1282/210511_130812/0000/tree_38.root",
                                  "/eos/cms/store/group/phys_higgs/hbb/ntuples/VHbbPostNano/2017/V11/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/RunIIFall17NanoAODv4-PU2017_1282/210511_130812/0000/tree_48.root",
                                                         "/eos/cms/store/group/phys_higgs/hbb/ntuples/VHbbPostNano/2017/V11/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/RunIIFall17NanoAODv4-PU2017_1282/210511_130812/0000/tree_45.root"]
        print("finding files for {}".format(name))
        self.filelist = ["root://eoscms.cern.ch/"+filename for filename in xrdfs(path+name) if filename.endswith(".root") and filename not in badFileNames]
        #self.filelist = [self.filelist[0]]
        #self.filelist = ["root://cmsxrootd.fnal.gov//"+filename for filename in xrdfs(path+name) if filename.endswith(".root") and filename not in badFileNames]


class ProcessorSumw(processor.ProcessorABC):
    def __init__(self):
        dataset_axis = hist.Cat("dataset","")
        self._accumulator = processor.dict_accumulator({
            'genEventSumw': processor.defaultdict_accumulator(float),
        })

    @property
    def accumulator(self):
        return self._accumulator
    
    def process(self, runs):
        dataset = runs.metadata['dataset']
        output = self.accumulator.identity()

        output['genEventSumw'][dataset]+=sum(runs.genEventSumw)
        return output

    def postprocess(self, accumulator):
        return accumulator


if __name__ == "__main__":
    samples = {
        "Znn2017": [Sample("V11/Z1JetsToNuNu_M-50_LHEZpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",596.4),
                   #Sample("V11/Z1JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",17.98),
                   #Sample("V11/Z1JetsToNuNu_M-50_LHEZpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",2.057),
                   #Sample("V11/Z1JetsToNuNu_M-50_LHEZpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",0.0224),
                   #Sample("V11/Z2JetsToNuNu_M-50_LHEZpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",325.7),
                   #Sample("V11/Z2JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",29.76),
                   #Sample("V11-NLO/Z2JetsToNuNu_M-50_LHEZpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",5.166),
                   #Sample("V11/Z2JetsToNuNU_M-50_LHEZpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",.08457),
                   ],
        "Wln2017": [
                #Sample("V11/W1JetsToLNu_LHEWpT_0-50_TuneCP5_13TeV-amcnloFXFX-pythia8",0)
                Sample("V11/W1JetsToLNu_LHEWpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",2661),
                Sample("V11/W1JetsToLNu_LHEWpT_100-150_TuneCP5_13TeV-amcnloFXFX-pythia8",286.1),
                Sample("V11_nlo_May21/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",71.9),
                Sample("V11/W1JetsToLNu_LHEWpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",8.05),
                Sample("V11-NLO_EventLumi/W1JetsToLNu_LHEWpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",.885),
                Sample("V11/W2JetsToLNu_LHEWpT_0-50_TuneCP5_13TeV-amcnloFXFX-pythia8",1615),
                Sample("V11/W2JetsToLNu_LHEWpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",1331),
                Sample("V11/W2JetsToLNu_LHEWpT_100-150_TuneCP5_13TeV-amcnloFXFX-pythia8",277.7),
                Sample("V11/W2JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",105.9),
                Sample("V11-NLO_EventLumi/W2JetsToLNu_LHEWpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",18.67),
                Sample("V11/W2JetsToLNu_LHEWpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",3.037),
                Sample("V11-NLO_anigamov/WJetsToLNu_0J_TuneCP5_13TeV-amcatnloFXFX-pythia8",54500),
                Sample("V11-NLO_anigamov/WJetsToLNu_1J_TuneCP5_13TeV-amcatnloFXFX-pythia8",8750),
                Sample("V11-NLO_anigamov/WJetsToLNu_2J_TuneCP5_13TeV-amcatnloFXFX-pythia8",3010),
           ] 
    }

    tic = time.time()
    cluster = LPCCondorCluster()
    cluster.adapt(minimum=1,maximum=10)
    client = Client(cluster)


    samplesToRun = samples["Wln2017"]
    fileset = {}
    for sample in samplesToRun:
        fileset[sample.name] = sample.filelist
    print(fileset)

    #runs = processor.run_uproot_job(
    #    fileset,
    #    treename="Runs",
    #    processor_instance=ProcessorSumw(),
    #    executor=processor.iterative_executor,
    #    executor_args={"schema": BaseSchema },
    #    #maxchunks=4,
    #)
    exe_args = {
    "client": client,
    "savemetrics": True,
    "schema": nanoevents.NanoAODSchema,
    "align_clusters": True,
    }

    client.wait_for_workers(1)
    runs,meta = processor.run_uproot_job(
        fileset,
        treename="Runs",
        processor_instance=ProcessorSumw(),
        executor=processor.dask_executor,
        executor_args=exe_args,
        # remove this to run on the whole fileset:
        #maxchunks=10,
    )

    #scaleFactors = {}
    #for sample in samplesToRun:
    #    scaleFactors[sample.name] = sample.xsec/runs["genEventSumw"][sample.name]


    outputSamples = {}
    for channel in samples:
        outputSamples[channel] = {}
        for sample in samples[channel]:
            outputSamples[channel][sample.name] = {}
            #outputSamples[channel]["name"] = sample.name
            outputSamples[channel][sample.name]["xsec"] = sample.xsec
            outputSamples[channel][sample.name]["filelist"] = sample.filelist
            outputSamples[channel][sample.name]["genEventSumw"] = runs["genEventSumw"][sample.name]
            #outputSamples[channel][sample.name]["intWeight"] = scaleFactors[sample.name]

    with open("sampleInfo.json","w") as f:
        json.dump(outputSamples,f,indent=4)
