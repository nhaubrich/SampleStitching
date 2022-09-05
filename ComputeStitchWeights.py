import subprocess
import awkward as ak
import coffea
import coffea.processor as processor
from coffea.nanoevents import BaseSchema
from coffea import hist,nanoevents
import time
import json
import matplotlib.pyplot as plt
import pickle
import argparse
import numpy as np
import uproot
#import pdb

from distributed import Client
from lpcjobqueue import LPCCondorCluster

#Vpt_axis = hist.Bin("Vpt", "LHE_Vpt [GeV]", 400, 0, 1000)
#VptBins_axis = hist.Bin("VptBins", "LHE_Vpt [GeV]", [0,50,100,150,250,400,600,650,2000])
#NpNLO_axis = hist.Bin("NpNLO","NpNLO",3,0,3)

def PreselJets(Jets):
    return (((Jets.puId>6) | (Jets.Pt>50)) & 
            (Jets.jetId>4) &
            (Jets.PtReg>20) &
            (abs(Jets.eta)<=2.5))

def DeltaPhi(obj1,obj2):
    return abs(obj1.phi-obj2.phi)*( abs(obj1.phi-obj2.phi)<np.pi) + (2*np.pi - abs(obj1.phi-obj2.phi))*(abs(obj1.phi-obj2.phi)>np.pi)


#def VptReweights(h,LHE_Vpt):
#    rwt = h.values()[np.digitize(LHE_Vpt,h.axis().edges())]

class btagWPs:
    def __init__(self,loose,medium,tight):
        self.loose = loose
        self.medium = medium
        self.tight = tight

class Processor(processor.ProcessorABC):
    def __init__(self,sampleInfo,channel,stitchResults,doPresel,reweight):
        self.sampleInfo = sampleInfo
        self.stitchResults = stitchResults
        self.channel = channel
        self.doPresel = doPresel
        self.reweight = reweight

        self.dataset_axis = hist.Cat("dataset", "dataset")

        self.Vpt_axis = hist.Bin("Vpt", "LHE_Vpt [GeV]", 200, 0, 1000)
        self.VptBins_axis = hist.Bin("VptBins", "LHE_Vpt [GeV]", [0,50,100,150,250,400,600,650,2000])
        self.NpNLO_axis = hist.Bin("NpNLO","NpNLO",3,0,3)
        self.deltaR_axis = hist.Bin("DeltaR","DeltaR",100,0,4)
        self.deltaPhi_axis = hist.Bin("DeltaPhi","DeltaPhi",64,0,3.2)

        self._accumulator = processor.dict_accumulator({
            'LHE_Vpt': hist.Hist("Counts", self.dataset_axis, self.Vpt_axis),
            'reweighting': hist.Hist("Reweighting",self.dataset_axis,self.VptBins_axis,self.NpNLO_axis),
            'validation': hist.Hist("Counts", self.dataset_axis, self.Vpt_axis,self.NpNLO_axis),
            'nostitching': hist.Hist("Counts", self.dataset_axis, self.Vpt_axis,self.NpNLO_axis),
            'nostitching_rwt': hist.Hist("Counts", self.dataset_axis, self.Vpt_axis,self.NpNLO_axis),
            
            'deltaR0': hist.Hist("Counts", self.dataset_axis, self.deltaR_axis,self.NpNLO_axis),
            'deltaR0_rwt': hist.Hist("Counts", self.dataset_axis, self.deltaR_axis,self.NpNLO_axis),

            'deltaPhi0': hist.Hist("Counts", self.dataset_axis, self.deltaPhi_axis,self.NpNLO_axis),
            'deltaPhi0_rwt': hist.Hist("Counts", self.dataset_axis, self.deltaPhi_axis,self.NpNLO_axis),

            'deltaR1': hist.Hist("Counts", self.dataset_axis, self.deltaR_axis,self.NpNLO_axis),
            'deltaR1_rwt': hist.Hist("Counts", self.dataset_axis, self.deltaR_axis,self.NpNLO_axis),

            'deltaPhi1': hist.Hist("Counts", self.dataset_axis, self.deltaPhi_axis,self.NpNLO_axis),
            'deltaPhi1_rwt': hist.Hist("Counts", self.dataset_axis, self.deltaPhi_axis,self.NpNLO_axis),
#             'LHE_Vpt': hist.Hist("Counts", dataset_axis, Vpt_axis, NpNLO_axis),
#             'LHE_NpNLO': hist.Hist("Counts", dataset_axis, Vpt_axis),
        })
    @property
    def accumulator(self):
        return self._accumulator

    def process(self, events):
        output = self.accumulator.identity()
        dataset = events.metadata["dataset"]
        
        DeepCSV = btagWPs(.1241,.4184,.7527) 
        
        jets = events.Jet[PreselJets(events.Jet)]
        fatJets = events.FatJet[(events.FatJet.Pt>250)&(events.FatJet.Msoftdrop>50)&(abs(events.FatJet.eta)<2.5)]
        fatJets = fatJets[ak.argsort(fatJets.deepTagMD_bbvsLight,axis=1,ascending=False)]
        met = events.MET
        muons = events.Muon
        electrons = events.Electron
        
        if self.doPresel:
            #check channel
            if self.channel == "Wln2018":
                #V selection
                
                trigger = ((events.Vtype==2)|(events.Vtype==3)&(events.HLT.Ele32_WPTight_Gsf_L1DoubleEG|events.HLT.IsoMu27))
                BestMu = ak.firsts(muons[(events.Vtype==2)&(abs(muons.eta)<2.4) & (muons.pt>25) & (muons.pfRelIso04_all<.06) & (muons.tightId>0)])

                BestEl = ak.firsts(electrons[(events.Vtype==3)& (abs(electrons.eta)<2.5) & (electrons.pt>30) & (electrons.pfRelIso03_all<.06) & (electrons.mvaFall17V2Iso_WP80>0)])
                 
                lepchan = np.array(["Zmm","Zee","Wmn","Wen","Znn","error"])[events.Vtype]

                goodLeps = ak.concatenate([ak.singletons(BestMu),ak.singletons(BestEl)],axis=1)
                firstLep = ak.firsts(goodLeps[ak.argsort(goodLeps.pt, axis=1,ascending=False)])
                
                #hack for RecoV. Better way to do this?
                met.eta=0
                met.mass=0

                RecoV = ak.zip({
                    "type": lepchan,
                    "lep1": firstLep,
                    "lep2": met,
                    "pt": (firstLep+met).pt,
                    "phi": (firstLep+met).phi,
                    "nAddLep": ak.num(goodLeps,axis=1)-1 ,
                    },with_name="PtEtaPhiMLorentzVector"
                )
                
                #jet selection
                hJets = jets[ak.argsort(jets.btagDeepB,axis=1,ascending=False)]
                hJet0 = ak.firsts(hJets)
                hJet1 = ak.firsts(hJets[:,1:])
                
                PassJetPtAndBtags = (hJet0.PtReg>25) & (hJet1.PtReg>25) & (hJet0.btagDeepB>DeepCSV.loose)
                
                #fatjet selection
                FatJetCand = ak.firsts(fatJets[DeltaPhi(fatJets,RecoV)>1.57],axis=1)
                PassFatJet = ~ak.is_none(FatJetCand)

                #filter events
                events = events[(trigger) & (RecoV.pt>150) & (PassJetPtAndBtags|PassFatJet)]
                events = events[~ak.is_none(events)] #remove "None" events where there were no objects suitable for higher-level quantities


            elif self.channel == "Zll2018":
                trigger = ((events.Vtype==0)|(events.Vtype==1) & (events.HLT.Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8|events.HLT.Ele23_Ele12_CaloIdL_TrackIdL_IsoVL))
                
                #Reconstruct from muons and electrons, then take one with higher pt (only one will exist from vtype sel)
                ZmmMuons = muons[(events.Vtype==0) & (abs(muons.eta)<2.4) & (muons.pt>20) & (muons.pfRelIso04_all<0.25)]
                ZeeElectrons = electrons[(events.Vtype==1) & (abs(electrons.eta)<2.5) & (electrons.pt>20) & (electrons.pfRelIso03_all<.15) & (electrons.mvaFall17V2Iso_WP90>0)]
        
                mu0 = ak.firsts(ZmmMuons[ZmmMuons.charge==1])
                mu1 = ak.firsts(ZmmMuons[ZmmMuons.charge==-1])
                Zmm = ak.unflatten(mu0+mu1,1)

                el0 = ak.firsts(ZeeElectrons[ZeeElectrons.charge==1])
                el1 = ak.firsts(ZeeElectrons[ZeeElectrons.charge==-1])
                Zee = ak.unflatten(el0+el1,1)

                RecoVs = ak.concatenate([Zee,Zmm],axis=1)
                RecoV = ak.firsts(RecoVs[ak.argsort(RecoVs.pt,axis=1,ascending=False)])

                #jet selection
                hJets = jets[ak.argsort(jets.btagDeepB,axis=1,ascending=False)]
                hJet0 = ak.firsts(hJets)
                hJet1 = ak.firsts(hJets[:,1:])
                
                PassJetPtAndBtags = (hJet0.PtReg>20) & (hJet1.PtReg>20) & (hJet0.btagDeepB>0)
                
                #fatjet selection
                FatJetCand = ak.firsts(fatJets[DeltaPhi(fatJets,RecoV)>1.57],axis=1)
                PassFatJet = ~ak.is_none(FatJetCand)


                #filter events
                events = events[(trigger) & (RecoV.pt>75) & (PassJetPtAndBtags|PassFatJet)]
                events = events[~ak.is_none(events)] #remove "None" events where there were no objects suitable for higher-level quantities

        events = events[(events.LHE.Vpt>100)&(events.LHE.Vpt<1000)] 
        Vpt = events.LHE.Vpt
        nj = events.LHE.NpNLO
        GenParts = events.GenPart
        
        if len(events)>0:

            fromV = (ak.fill_none(abs(GenParts.distinctParent.pdgId)==23,False)|(ak.fill_none(abs(GenParts.distinctParent.pdgId)==24,False)))
            GenLeps = GenParts[(((abs(GenParts.pdgId) == 11))|(abs(GenParts.pdgId==13))) & GenParts.hasFlags(['fromHardProcess','isLastCopy']) & fromV]
            GenLep0 = ak.firsts(GenLeps[ak.argsort(GenLeps.pt,axis=1,ascending=False)])

            GenJet0 = ak.firsts(events.GenJet)
            GenJet1 = ak.firsts(events.GenJet[:,1:])

            deltaRLepJet0 =   ak.fill_none(GenJet0.delta_r(GenLep0),-99)
            deltaEtaLepJet0 = ak.fill_none(abs(GenJet0.eta-GenLep0.eta),-99)
            deltaPhiLepJet0 = ak.fill_none(abs(GenJet0.delta_phi(GenLep0)),-99)

            deltaRLepJet1 =   ak.fill_none(GenJet1.delta_r(GenLep0),-99)
            deltaEtaLepJet1 = ak.fill_none(abs(GenJet1.eta-GenLep0.eta),-99)
            deltaPhiLepJet1 = ak.fill_none(abs(GenJet1.delta_phi(GenLep0)),-99)

            rwt = 1.0
            if self.reweight!=None:
                hname = dataset.split("/")[-1]+"_1;1"
                if hname in self.reweight.keys():
                    rwt0 = events.LHE.NpNLO==0
                    
                    h=self.reweight[hname]

                    rwt1 = 1/h.values()[np.digitize(events.LHE.Vpt,h.axis().edges())-1]
                    rwt1 = rwt1*(events.LHE.NpNLO==1)
                
                    
                    h=self.reweight[dataset.split("/")[-1]+"_2;1"]
                    rwt2 = 1/h.values()[np.digitize(events.LHE.Vpt,h.axis().edges())-1]
                    rwt2 = rwt2*(events.LHE.NpNLO==2)

                    rwt = rwt0+rwt1+rwt2
                    rwt_neg = rwt*(rwt<0) #cut out negative weights outside samples
                    rwt = rwt-rwt_neg
                    rwt = ak.nan_to_num(rwt,0)#nans got through?
            
            output['LHE_Vpt'].fill(dataset=dataset, Vpt=Vpt,weight=events.genWeight)        
            output['reweighting'].fill(dataset=dataset,VptBins=Vpt,NpNLO=events.LHE.NpNLO)

            if self.stitchResults!=None:
                tot=self.stitchResults["reweighting"].sum("dataset",overflow="all").values(overflow="all")[()][self.VptBins_axis.index(Vpt),self.NpNLO_axis.index(nj)]
                sampletot=self.stitchResults["reweighting"].values(overflow="all")[str(dataset),][self.VptBins_axis.index(Vpt),self.NpNLO_axis.index(nj)]
                output['validation'].fill(dataset=dataset, Vpt=Vpt, NpNLO=nj,weight=events.genWeight*sampletot/tot)
            
            output['nostitching'].fill(dataset=dataset, Vpt=Vpt, NpNLO=nj,weight=events.genWeight)
            output['nostitching_rwt'].fill(dataset=dataset, Vpt=Vpt, NpNLO=nj,weight=events.genWeight*rwt)
                
            output['deltaR0'].fill(dataset=dataset, DeltaR=deltaRLepJet0, NpNLO=nj,weight=events.genWeight)
            output['deltaR0_rwt'].fill(dataset=dataset, DeltaR=deltaRLepJet0, NpNLO=nj,weight=events.genWeight*rwt)
            output['deltaPhi0'].fill(dataset=dataset, DeltaPhi=deltaPhiLepJet0, NpNLO=nj,weight=events.genWeight)
            output['deltaPhi0_rwt'].fill(dataset=dataset, DeltaPhi=deltaPhiLepJet0, NpNLO=nj,weight=events.genWeight*rwt)
                
            output['deltaR1'].fill(dataset=dataset, DeltaR=deltaRLepJet1, NpNLO=nj,weight=events.genWeight)
            output['deltaR1_rwt'].fill(dataset=dataset, DeltaR=deltaRLepJet1, NpNLO=nj,weight=events.genWeight*rwt)
            output['deltaPhi1'].fill(dataset=dataset, DeltaPhi=deltaPhiLepJet1, NpNLO=nj,weight=events.genWeight)
            output['deltaPhi1_rwt'].fill(dataset=dataset, DeltaPhi=deltaPhiLepJet1, NpNLO=nj,weight=events.genWeight*rwt)
        return output

    def postprocess(self, accumulator):
        intWeights = {}
        for sample in self.sampleInfo:
            intWeights[sample] = self.sampleInfo[sample]["xsec"]/self.sampleInfo[sample]["genEventSumw"]
        
        accumulator["LHE_Vpt"].scale(intWeights,axis="dataset")
        
        accumulator["validation"].scale(intWeights,axis="dataset")
        accumulator["nostitching"].scale(intWeights,axis="dataset")
        accumulator["nostitching_rwt"].scale(intWeights,axis="dataset")

        accumulator['deltaR0'].scale(intWeights,axis="dataset")
        accumulator['deltaR0_rwt'].scale(intWeights,axis="dataset")
        accumulator['deltaPhi0'].scale(intWeights,axis="dataset")
        accumulator['deltaPhi0_rwt'].scale(intWeights,axis="dataset")
        
        accumulator['deltaR1'].scale(intWeights,axis="dataset")
        accumulator['deltaR1_rwt'].scale(intWeights,axis="dataset")
        accumulator['deltaPhi1'].scale(intWeights,axis="dataset")
        accumulator['deltaPhi1_rwt'].scale(intWeights,axis="dataset")
        return accumulator

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--channel','-c', help='channel to run',default="Wln2017")
    parser.add_argument('--samples','-s', help='sample json file',default="sampleInfo.json")
    parser.add_argument('--tag','-t', help='tag for output files',default="test")
    parser.add_argument('--stitchResults','-r', help='stitch results to validate',default=None)
    parser.add_argument('--reweight',help="Ratio histograms for Vpt-binned reweighting",default=None)
    parser.add_argument('--local', help='disable batch mode',action="store_true",default=False)
    parser.add_argument('--preselection', help='use VHbb preselection',action="store_true",default=False)
    args = parser.parse_args()

    with open(args.samples,"r") as sampleFile:
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
    
    #load reweighting hists into dict
    if args.reweight!=None:
        with uproot.open(args.reweight) as f_rwt:
            rwtDict = {}
            for hname in f_rwt.keys():
                rwtDict[hname] = f_rwt[hname]
    else:
        rwtDict=None

    #local
    if args.local:
        output = processor.run_uproot_job(
            fileset,
            "Events",
            Processor(channel=args.channel,sampleInfo=samplesToRun,stitchResults=stitchResults,doPresel=args.preselection,reweight=rwtDict),
            processor.iterative_executor,
            {"schema": nanoevents.NanoAODSchema},
            
            #executor=processor.futures_executor,
            #executor_args={"schema": NanoAODSchema, "workers": 4},
            chunksize=1000,
            maxchunks=1,
        )
    else: 
        #batch 
        cluster = LPCCondorCluster(memory="4 GB")
        cluster.adapt(minimum=1,maximum=200)
        client = Client(cluster)
        exe_args = {
        "client": client,
        "savemetrics": True,
        "schema": nanoevents.NanoAODSchema,
        "align_clusters": True,
        "xrootdtimeout": 12000,
        "skipbadfiles": True,
        }
        client.wait_for_workers(1)
        output,meta = processor.run_uproot_job(
            fileset,
            treename="Events",
            processor_instance=Processor(channel=args.channel,sampleInfo=samplesToRun,stitchResults=stitchResults,doPresel=args.preselection,reweight=rwtDict),
            executor=processor.dask_executor,
            executor_args=exe_args,
            chunksize=100000,
        )
    stitchResults = output

    #totalWeightSum = output["reweighting"].sum("dataset")
    #hist.plot2d(output["reweighting"].sum("dataset"),xaxis="VptBins")
    #plt.title("total")
    #for dataset in output["reweighting"].identifiers(axis="dataset"):
    #    hist.plot2d(output["reweighting"][dataset].sum("dataset"),xaxis="VptBins")
    #    plt.title(dataset)
    #    plt.savefig(str(dataset).split("/")[-1]+".png")
    
    with open("stitchingResults_{}{}.pickle".format(args.channel,args.tag),"wb") as pickleOut:
        pickle.dump(stitchResults,pickleOut)



    #if args.stitchResults!=None:
    #    from cycler import cycler
    #    fig, ax = plt.subplots()
    #    colors = ['#f7fcfd','#e0ecf4','#bfd3e6','#9ebcda','#8c96c6','#8c6bb1','#88419d','#810f7c','#4d004b']
    #    ax.set_prop_cycle(cycler('color',colors))
    #    ax.semilogy(True)
    #    hist.plot1d(output["validation"].integrate("NpNLO"),stack=True,ax=ax,line_opts=None,fill_opts={'alpha':0.2,'edgecolor': (0,0,0,0.3)},clear=False)
    #    # hist.plot2d(output["LHE_Vpt"],ax=ax,xaxis="LHE_Vpt",clear=False)

    #    leg = ax.legend(bbox_to_anchor=(1,1), loc="upper left")
    #    plt.savefig("validation_{}.png".format(args.channel),bbox_inches="tight")
    #    plt.clf()



    #from cycler import cycler
    #fig, ax = plt.subplots()
    #colors = ['#f7fcfd','#e0ecf4','#bfd3e6','#9ebcda','#8c96c6','#8c6bb1','#88419d','#810f7c','#4d004b']
    #ax.set_prop_cycle(cycler('color',colors))
    #ax.semilogy(True)
    #hist.plot1d(output["LHE_Vpt"],stack=False,ax=ax,line_opts=None,fill_opts={'alpha':0.2,'edgecolor': (0,0,0,0.3)},clear=False)
    ## hist.plot2d(output["LHE_Vpt"],ax=ax,xaxis="LHE_Vpt",clear=False)

    #leg = ax.legend(bbox_to_anchor=(1,1), loc="upper left")
    #plt.savefig("Vpt_notStitched.png",bbox_inches="tight")


    #exportDict = {}
    #VptBins_axis = stitchResults['reweighting'][list(stitchResults["reweighting"].values().keys())[0]].axis("VptBins")
    #NpNLO_axis = stitchResults['reweighting'][list(stitchResults["reweighting"].values().keys())[0]].axis("NpNLO")

    #VptBins = [str(k).split(",")[0][1:] for k in VptBins_axis[1:]]
    #NpNLOBins = [str(k).split(",")[0][1:] for k in NpNLO_axis[1:]]


    #eventTotals = stitchResults["reweighting"].sum("dataset")

    #for sample in stitchResults["reweighting"].identifiers(axis="dataset"):
    #    print(sample)
    #    sample = str(sample)
    #    weightList = stitchResults["reweighting"][sample].values()[(sample,)].tolist()
    #    exportDict[sample] = {}
    #    
    #    for i,row in enumerate(weightList):
    #        exportDict[sample]["Vpt"+VptBins[i]] = {}
    #        for j,column in enumerate(row):
    #            if eventTotals.values()[()][i][j]!=0:
    #                fraction = column/eventTotals.values()[()][i][j]
    #            else:
    #                fraction = 0
    #            #if fraction != fraction:
    #                #fraction = 0 #protect against 0/0 NaN
    #            exportDict[sample]["Vpt"+VptBins[i]]["NpNLO"+NpNLOBins[j]] = fraction


    #print(json.dumps(exportDict))
    #with open("stitchingWeights_{}{}.json".format(args.channel,args.tag),"w") as f:
    #    json.dump(exportDict,f,indent=4)
