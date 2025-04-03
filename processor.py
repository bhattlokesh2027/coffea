from coffea.processor import ProcessorABC
from coffea.analysis_tools import PackedSelection, Cutflow
import awkward
import hist
import hist.dask as hda

hist_meta = {
    'Z_pt':{'bins':100, 'start':80, 'stop':120, 'label':'$Z \ p_T$'},
    'Z_mass':{'bins':100, 'start':80, 'stop':110, 'label':'$Z_mass$'},
    'Z_phi':{'bins':100, 'start':-3.14, 'stop':3.14, 'label':'$Z \ \\phi$'},
    'Z_eta':{'bins':100, 'start':-1.5, 'stop':1.5, 'label':'$Z \ \\eta$'},
}

def get_hist(key, delayed = True):
    '''Construct a histogram from hist_meta dictionary'''
    meta = hist_meta[key]
    if delayed:
        return hda.Hist.new.Regular(**meta).Double()
    else:
        return hist.Hist.new.Regular(**meta).Double()


class Zpeak(ProcessorABC):
    '''Create a Z peak with electrons'''
    def __init__(self):
        pass
    def process(self, events):

        #Event selections
        event_selections = PackedSelection()
        #1. Events with exactly two electrons
        event_selections.add('2ele', awkward.num(events.Electron, axis=1) == 2)
        #2. Events with at least two electrons having pt greater than 25 GeV
        event_selections.add('ptgt25', awkward.sum(events.Electron.pt > 25 , axis=1) >= 2)
        #3. Events with oppositely charged electrons, given that there are exactly two electrons
        event_selections.add('opp_charged_electrons', awkward.sum(events.Electron.charge , axis=1) == 0)
        
        # Apply all the event selections
        selected_events = events[event_selections.all(*event_selections.names)]
        
        # Create the Z candidates
        Dielectrons = selected_events.Electron[:,0] + selected_events.Electron[:,1]
        
        # 4. Z mass window cut
        Z_cand = Dielectrons[Dielectrons.mass > 80]
        Z_cand = Z_cand[Z_cand.mass < 110]
        
        # Get cutflow
        onecut, cutflow, labels = event_selections.cutflow(*event_selections.names).yieldhist()
        
        return {
            #'Dataset':events.metadata['Dataset'],
            'Histograms':{
                'Event Selections':{
                    'Onecut':onecut,
                    'Cutflow':cutflow,
                    'labels':labels
                },
                'Z':{
                    'mass':get_hist('Z_mass').fill(Z_cand.mass),
                    'pt':get_hist('Z_pt').fill(Z_cand.pt),
                    'eta':get_hist('Z_eta').fill(Z_cand.eta),
                    'phi':get_hist('Z_phi').fill(Z_cand.phi)
                }
            }
        }
    def postprocess(self, acumulator):
        pass
