import subprocess
import json
import coffea
import coffea.processor as processor
from coffea.nanoevents import BaseSchema
from coffea import hist,nanoevents
import time

import sys

import matplotlib.pyplot as plt
#import pdb
from distributed import Client
from lpcjobqueue import LPCCondorCluster

#write json file with all the sample info: filepaths, xsec, sumw
#{ channel: 
#   sample:
#       filelist: []
#       xsec:
#       sumw:
#}

path = "/eos/cms/store/group/phys_higgs/hbb/ntuples/VHbbPostNano/"

Vpt_axis = hist.Bin("Vpt", "LHE_Vpt [GeV]", 40, 0, 500)
VptBins_axis = hist.Bin("VptBins", "LHE_Vpt [GeV]", [0,50,100,150,250,400,2000])
NpNLO_axis = hist.Bin("NpNLO","NpNLO",3,0,3)

def xrdfs(fullpath):
    itemlist = [ filename for filename in subprocess.check_output(['xrdfs',"root://eoscms.cern.ch",'ls','-R',fullpath]).decode().split("\n")]
    rootfiles = [f for f in itemlist if f.endswith(".root")]
    return rootfiles 

class Sample():
    def __init__(self,name,xsec,year):
        self.name = name
        self.year = year
        self.xsec = xsec
        self.genEventSum = 0
        
        badFileNames = ["/eos/cms/store/group/phys_higgs/hbb/ntuples/VHbbPostNano/2017/V11/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/RunIIFall17NanoAODv4-PU2017_1282/210511_130812/0000/tree_38.root",
                                  "/eos/cms/store/group/phys_higgs/hbb/ntuples/VHbbPostNano/2017/V11/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/RunIIFall17NanoAODv4-PU2017_1282/210511_130812/0000/tree_48.root",
                                                         "/eos/cms/store/group/phys_higgs/hbb/ntuples/VHbbPostNano/2017/V11/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/RunIIFall17NanoAODv4-PU2017_1282/210511_130812/0000/tree_45.root"]
        print("finding files for {}".format(name))
        self.filelist = ["root://eoscms.cern.ch/"+filename for filename in xrdfs(path+str(year)+"/"+name) if filename.endswith(".root") and filename not in badFileNames]
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
        #pdb.set_trace() 
        if "genEventSumw_" in runs.fields:
            output['genEventSumw'][dataset]+=sum(runs.genEventSumw_)
        elif "genEventSumw" in runs.fields:
            output['genEventSumw'][dataset]+=sum(runs.genEventSumw)
        return output

    def postprocess(self, accumulator):
        return accumulator


if __name__ == "__main__":
    samples = {
        #"Znn2017": [Sample("V11/Z1JetsToNuNu_M-50_LHEZpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",596.4,2017),
        #           #Sample("V11/Z1JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",17.98,2017),
        #           #Sample("V11/Z1JetsToNuNu_M-50_LHEZpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",2.057,2017),
        #           #Sample("V11/Z1JetsToNuNu_M-50_LHEZpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",0.0224,2017),
        #           #Sample("V11/Z2JetsToNuNu_M-50_LHEZpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",325.7,2017),
        #           #Sample("V11/Z2JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",29.76,2017),
        #           #Sample("V11-NLO/Z2JetsToNuNu_M-50_LHEZpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",5.166,2017),
        #           #Sample("V11/Z2JetsToNuNU_M-50_LHEZpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",.08457,2017),
        #           ],
        #"Wln2017": [
        #        #Sample("V11/W1JetsToLNu_LHEWpT_0-50_TuneCP5_13TeV-amcnloFXFX-pythia8",0,2017)
        #        Sample("V11/W1JetsToLNu_LHEWpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",2661,2017),
        #        Sample("V11/W1JetsToLNu_LHEWpT_100-150_TuneCP5_13TeV-amcnloFXFX-pythia8",286.1,2017),
        #        Sample("V11_nlo_May21/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",71.9,2017),
        #        Sample("V11/W1JetsToLNu_LHEWpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",8.05,2017),
        #        Sample("V11-NLO_EventLumi/W1JetsToLNu_LHEWpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",.885,2017),
        #        Sample("V11/W2JetsToLNu_LHEWpT_0-50_TuneCP5_13TeV-amcnloFXFX-pythia8",1615,2017),
        #        Sample("V11/W2JetsToLNu_LHEWpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",1331,2017),
        #        Sample("V11/W2JetsToLNu_LHEWpT_100-150_TuneCP5_13TeV-amcnloFXFX-pythia8",277.7,2017),
        #        Sample("V11/W2JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",105.9,2017),
        #        Sample("V11-NLO_EventLumi/W2JetsToLNu_LHEWpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",18.67,2017),
        #        Sample("V11/W2JetsToLNu_LHEWpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",3.037,2017),
        #        Sample("V11-NLO_anigamov/WJetsToLNu_0J_TuneCP5_13TeV-amcatnloFXFX-pythia8",54500,2017),
        #        Sample("V11-NLO_anigamov/WJetsToLNu_1J_TuneCP5_13TeV-amcatnloFXFX-pythia8",8750,2017),
        #        Sample("V11-NLO_anigamov/WJetsToLNu_2J_TuneCP5_13TeV-amcatnloFXFX-pythia8",3010,2017),
        #   ],
        #"Znn2018Xbb": [
        #    Sample("V12/Z1JetsToNuNu_M-50_LHEZpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",596.3,2018),
        #    Sample("V12/Z1JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",17.98,2018),
        #    Sample("V12/Z1JetsToNuNu_M-50_LHEZpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",2.045,2018),
        #    Sample("V12/Z1JetsToNuNu_M-50_LHEZpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",.2243,2018),
        #    Sample("V12/Z2JetsToNuNu_M-50_LHEZpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",325.7,2018),
        #    Sample("V12/Z2JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",29.76,2018),
        #    Sample("V12/Z2JetsToNuNu_M-50_LHEZpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",5.166,2018),
        #    Sample("V12/Z2JetsToNuNU_M-50_LHEZpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",.8457,2018)
        #],
        #"Znn2018Max": [
        #    Sample("V12/Z1JetsToNuNu_M-50_LHEZpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",596.3,2018),
        #    Sample("V12/Z1JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",17.98,2018),
        #    Sample("V12-nlo/Z1JetsToNuNu_M-50_LHEZpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",2.045,2018),
        #    Sample("V12/Z1JetsToNuNu_M-50_LHEZpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",.2243,2018),
        #    Sample("V12/Z2JetsToNuNu_M-50_LHEZpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",325.7,2018),
        #    Sample("V12/Z2JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",29.76,2018),
        #    Sample("V12-NLO_EventLumi/Z2JetsToNuNu_M-50_LHEZpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",5.166,2018),
        #    Sample("V12/Z2JetsToNuNU_M-50_LHEZpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",.8457,2018)
        #],
        "Wln2018": [
            Sample("V12/WJetsToLNu_Pt-50To100_TuneCP5_13TeV-amcatnloFXFX-pythia8",3570,2018),
            Sample("V12/WJetsToLNu_Pt-100To250_TuneCP5_13TeV-amcatnloFXFX-pythia8",770.8,2018),
            Sample("V12/WJetsToLNu_Pt-250To400_TuneCP5_13TeV-amcatnloFXFX-pythia8",28.06,2018),
            Sample("V12/WJetsToLNu_Pt-400To600_TuneCP5_13TeV-amcatnloFXFX-pythia8",3.591,2018),
            Sample("V12/WJetsToLNu_Pt-600ToInf_TuneCP5_13TeV-amcatnloFXFX-pythia8",0.5495,2018),
            Sample("V12/WJetsToLNu_0J_TuneCP5_13TeV-amcatnloFXFX-pythia8",54500,2018),
            Sample("V12/WJetsToLNu_1J_TuneCP5_13TeV-amcatnloFXFX-pythia8",8750,2018),
            Sample("V12/WJetsToLNu_2J_TuneCP5_13TeV-amcatnloFXFX-pythia8",3010,2018),
        ],
        "Zll2018": [
            Sample("V12/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8",6508,2018),
            Sample("V12/DY1JetsToLL_M-50_LHEZpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",316.6,2018),
            Sample("V12/DY1JetsToLL_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",9.543,2018),
            Sample("V12/DY1JetsToLL_M-50_LHEZpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",1.098,2018),
            Sample("V12/DY1JetsToLL_M-50_LHEZpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",.1193,2018),
            Sample("V12/DY2JetsToLL_M-50_LHEZpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",169.6,2018),
            Sample("V12/DY2JetsToLL_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",15.65,2018),
            Sample("V12/DY2JetsToLL_M-50_LHEZpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",2.737,2018),
            Sample("V12/DY2JetsToLL_M-50_LHEZpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",.4477,2018),
            Sample("V12/DYJetsToLL_0J_TuneCP5_13TeV-amcatnloFXFX-pythia8",5333,2018),
            Sample("V12/DYJetsToLL_1J_TuneCP5_13TeV-amcatnloFXFX-pythia8",965,2018),
            Sample("V12/DYJetsToLL_2J_TuneCP5_13TeV-amcatnloFXFX-pythia8",362,2018),
            Sample("V12/DYJetsToLL_Pt-50To100_TuneCP5_13TeV-amcatnloFXFX-pythia8",409.8,2018),
            Sample("V12/DYJetsToLL_Pt-100To250_TuneCP5_13TeV-amcatnloFXFX-pythia8",97.26,2018),
            Sample("V12/DYJetsToLL_Pt-250To400_TuneCP5_13TeV-amcatnloFXFX-pythia8",3.764,2018),
            Sample("V12/DYJetsToLL_Pt-400To650_TuneCP5_13TeV-amcatnloFXFX-pythia8",.5152,2018),
            Sample("V12/DYJetsToLL_Pt-650ToInf_TuneCP5_13TeV-amcatnloFXFX-pythia8",.0483,2018)
        ],

    }

    tic = time.time()
    cluster = LPCCondorCluster()
    cluster.adapt(minimum=1,maximum=50)
    client = Client(cluster)


    samplesToRun = samples["Wln2018"]+samples["Zll2018"]
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
    #    #executor_args={"schema": nanoevents.NanoAODSchema},
    #    #maxchunks=4,
    #)
    exe_args = {
    "client": client,
    "savemetrics": True,
    #"schema": nanoevents.NanoAODSchema,
    "schema": BaseSchema,
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

    with open(sys.argv[1],"w") as f:
        json.dump(outputSamples,f,indent=4)
