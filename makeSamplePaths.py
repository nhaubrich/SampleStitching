import subprocess
import json

path = "/eos/cms/store/group/phys_higgs/hbb/ntuples/VHbbPostNano/2017/"

def xrdfsRecursive(fullpath):
    itemlist = [ filename for filename in subprocess.check_output(['xrdfs',"root://eoscms.cern.ch",'ls',fullpath]).decode().split("\n")[:-1]]
    if len(itemlist)==1 and itemlist[0]==fullpath:
        return itemlist

    newpaths = []
    for path in itemlist:
        newpaths+= xrdfsRecursive(path)
    return newpaths


class Sample():
    def __init__(self,name,xsec):
        self.name = name
        self.xsec = xsec
        self.genEventSum = 0
#         print(path+name+"/*/*/*")
        #self.files = glob.glob(path+name+"/*/*/*")[0]
        badFileNames = ["/eos/cms/store/group/phys_higgs/hbb/ntuples/VHbbPostNano/2017/V11/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/RunIIFall17NanoAODv4-PU2017_1282/210511_130812/0000/tree_38.root",
                       "/eos/cms/store/group/phys_higgs/hbb/ntuples/VHbbPostNano/2017/V11/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/RunIIFall17NanoAODv4-PU2017_1282/210511_130812/0000/tree_48.root",
                       "/eos/cms/store/group/phys_higgs/hbb/ntuples/VHbbPostNano/2017/V11/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8/RunIIFall17NanoAODv4-PU2017_1282/210511_130812/0000/tree_45.root"]
        
        self.filelist = ["root://xcache/"+filename for filename in xrdfsRecursive(path+name) if filename.endswith(".root") and filename not in badFileNames]
#         self.filelist = [self.filelist]
        assert len(self.filelist)>0


samples = {
    "Znn2017": [Sample("V11/Z1JetsToNuNu_M-50_LHEZpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",596.4),
               Sample("V11/Z1JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",17.98),
               Sample("V11/Z1JetsToNuNu_M-50_LHEZpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",2.057),
               Sample("V11/Z1JetsToNuNu_M-50_LHEZpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",0.0224),
               Sample("V11/Z2JetsToNuNu_M-50_LHEZpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",325.7),
                Sample("V11/Z2JetsToNuNu_M-50_LHEZpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",29.76),
                Sample("V11-NLO/Z2JetsToNuNu_M-50_LHEZpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",5.166),
                Sample("V11/Z2JetsToNuNU_M-50_LHEZpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",.08457),
               ],
    "Wln2017": [Sample("V11/W1JetsToLNu_LHEWpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",2661),
               Sample("V11/W1JetsToLNu_LHEWpT_100-150_TuneCP5_13TeV-amcnloFXFX-pythia8",286.1),
               Sample("V11/W1JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",71.9),
                Sample("V11/W1JetsToLNu_LHEWpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",8.05),
                Sample("V11/W1JetsToLNu_LHEWpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",0.885),
                Sample("V11/W2JetsToLNu_LHEWpT_0-50_TuneCP5_13TeV-amcnloFXFX-pythia8",1615.0),
                Sample("V11/W2JetsToLNu_LHEWpT_50-150_TuneCP5_13TeV-amcnloFXFX-pythia8",1331),
                Sample("V11/W2JetsToLNu_LHEWpT_100-150_TuneCP5_13TeV-amcnloFXFX-pythia8",277.7),
                Sample("V11/W2JetsToLNu_LHEWpT_150-250_TuneCP5_13TeV-amcnloFXFX-pythia8",105.9),
                Sample("V11/W2JetsToLNu_LHEWpT_250-400_TuneCP5_13TeV-amcnloFXFX-pythia8",18.67),
                Sample("V11/W2JetsToLNu_LHEWpT_400-inf_TuneCP5_13TeV-amcnloFXFX-pythia8",3.037),
                Sample("V11-NLO_anigamov/WJetsToLNu_0J_TuneCP5_13TeV-amcatnloFXFX-pythia8",54500),
                Sample("V11-NLO_anigamov/WJetsToLNu_1J_TuneCP5_13TeV-amcatnloFXFX-pythia8",8750),
                Sample("V11-NLO_anigamov/WJetsToLNu_2J_TuneCP5_13TeV-amcatnloFXFX-pythia8",3010)]
}
files = []
for chan in samples:
    for sample in samples[chan]:
        files+= sample.filelist

with open("filelist.txt","w") as f:
    f.writelines(files)
