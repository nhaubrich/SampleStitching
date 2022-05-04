import subprocess
import coffea
import coffea.processor as processor
from coffea.nanoevents import BaseSchema
from coffea import hist,nanoevents
import time
import json
import matplotlib.pyplot as plt
import pickle
import argparse

from distributed import Client
from lpcjobqueue import LPCCondorCluster

Vpt_axis = hist.Bin("Vpt", "LHE_Vpt [GeV]", 40, 0, 500)
VptBins_axis = hist.Bin("VptBins", "LHE_Vpt [GeV]", [0,50,100,150,250,400,2000])
NpNLO_axis = hist.Bin("NpNLO","NpNLO",3,0,3)

class Processor(processor.ProcessorABC):
    def __init__(self,sampleInfo,stitchResults):
        self.sampleInfo = sampleInfo
        self.stitchResults = stitchResults
        dataset_axis = hist.Cat("dataset", "dataset")

        Vpt_axis = hist.Bin("Vpt", "LHE_Vpt [GeV]", 40, 0, 500)
        VptBins_axis = hist.Bin("VptBins", "LHE_Vpt [GeV]", [0,50,100,150,250,400,2000])
        NpNLO_axis = hist.Bin("NpNLO","NpNLO",3,0,3)
        
        self._accumulator = processor.dict_accumulator({
            'LHE_Vpt': hist.Hist("Counts", dataset_axis, Vpt_axis),
            'reweighting': hist.Hist("Reweighting",dataset_axis,VptBins_axis,NpNLO_axis),
            'LHE_Vptstitched': hist.Hist("Counts", dataset_axis, Vpt_axis),

#             'LHE_Vpt': hist.Hist("Counts", dataset_axis, Vpt_axis, NpNLO_axis),
#             'LHE_NpNLO': hist.Hist("Counts", dataset_axis, Vpt_axis),
        })
    @property
    def accumulator(self):
        return self._accumulator

    def process(self, events):
        output = self.accumulator.identity()
        dataset = events.metadata["dataset"]
#         events = events[events.LHE.NpNLO==2]
        Vpt = events.LHE.Vpt
        nj = events.LHE.NpNLO
        output['LHE_Vpt'].fill(dataset=dataset, Vpt=Vpt,weight=events.genWeight)
        output['reweighting'].fill(dataset=dataset,VptBins=Vpt,NpNLO=events.LHE.NpNLO)

        if self.stitchResults!=None:
            tot=self.stitchResults["reweighting"].sum("dataset",overflow="all").values(overflow="all")[()][VptBins_axis.index(Vpt),NpNLO_axis.index(nj)]
            sampletot=self.stitchResults["reweighting"].values(overflow="all")[str(dataset),][VptBins_axis.index(Vpt),NpNLO_axis.index(nj)]
            output['LHE_Vptstitched'].fill(dataset=dataset, Vpt=Vpt,weight=events.genWeight*sampletot/tot)

        return output

    def postprocess(self, accumulator):
        intWeights = {}
        for sample in self.sampleInfo:
            intWeights[sample] = self.sampleInfo[sample]["xsec"]/self.sampleInfo[sample]["genEventSumw"]
        
        accumulator["LHE_Vpt"].scale(intWeights,axis="dataset")
        if self.stitchResults!=None:
            accumulator["LHE_Vptstitched"].scale(intWeights,axis="dataset")

        return accumulator

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--channel','-c', help='channel to run',default="Wln2017")
    parser.add_argument('--stitchResults','-s', help='stitch results to validate',default=None)
    args = parser.parse_args()

    with open("sampleInfo.json","r") as sampleFile:
        sampleDict = json.load(sampleFile)
    samplesToRun = sampleDict[args.channel] 

    if args.stitchResults!=None:
        with open(args.stitchResults,"rb") as stitchResultFile:
            stitchResults = pickle.load(stitchResultFile)
    else:
        stitchResults=None

    fileset = {}
    for sample in samplesToRun:
        fileset[sample] = samplesToRun[sample]["filelist"]

    cluster = LPCCondorCluster()
    cluster.adapt(minimum=1,maximum=30)
    client = Client(cluster)
    exe_args = {
    "client": client,
    "savemetrics": True,
    "schema": nanoevents.NanoAODSchema,
    "align_clusters": True,
    }
    client.wait_for_workers(1)
    output,meta = processor.run_uproot_job(
        fileset,
        treename="Events",
        processor_instance=Processor(sampleInfo=samplesToRun,stitchResults=stitchResults),
        executor=processor.dask_executor,
        executor_args=exe_args,
    )
    stitchResults = output

    totalWeightSum = output["reweighting"].sum("dataset")
    hist.plot2d(output["reweighting"].sum("dataset"),xaxis="VptBins")
    plt.title("total")
    for dataset in output["reweighting"].identifiers(axis="dataset"):
        hist.plot2d(output["reweighting"][dataset].sum("dataset"),xaxis="VptBins")
        plt.title(dataset)
        plt.savefig(str(dataset).split("/")[-1]+".png")
    
    with open("stitchingResults.pickle","wb") as pickleOut:
        pickle.dump(stitchResults,pickleOut)



    if args.stitchResults!=None:
        from cycler import cycler
        fig, ax = plt.subplots()
        colors = ['#f7fcfd','#e0ecf4','#bfd3e6','#9ebcda','#8c96c6','#8c6bb1','#88419d','#810f7c','#4d004b']
        ax.set_prop_cycle(cycler('color',colors))
        ax.semilogy(True)
        hist.plot1d(output["LHE_Vptstitched"],stack=True,ax=ax,line_opts=None,fill_opts={'alpha':0.2,'edgecolor': (0,0,0,0.3)},clear=False)
        # hist.plot2d(output["LHE_Vpt"],ax=ax,xaxis="LHE_Vpt",clear=False)

        leg = ax.legend(bbox_to_anchor=(1,1), loc="upper left")
        plt.savefig("Vptstitched.png",bbox_inches="tight")