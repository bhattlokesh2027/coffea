# COFFEA Plotter
# Author: Prayag Yadav
# Email: prayag.yadav@cern.ch
# Last Updated: 7 March 2025


from coffea import util
from processor import hist_meta
import mplhep as hep
import argparse
import re
import glob
import copy
import numpy as np
import matplotlib.pyplot as plt

########################
### Helper Functions ###
########################

def get_subdict(dicts, key):
    '''
    Get list of subdictionaries(if available) from a list of dictionaries
    '''
    out = []
    for d in dicts:
        for k in d.keys():
            if key == k:
                out.append(d[key])
    return out

def accumulate(dicts):
    """
    Merges an array of dictionaries and adds up the values of common keys.

    Parameters:
    dicts (list): A list of dictionaries to be merged.

    Returns:
    dict: A dictionary with combined keys and values summed for common keys.
    """
    exception_list = ['Labels'] # These keys will not be repeated but included once.
    outdict = {}

    for diction in dicts:
        dictionary = copy.deepcopy(diction)

        for key, value in dictionary.items():
            # print(f"{key} : {value}")
            # print(type(value))

            if isinstance(value,dict):
                value = accumulate(get_subdict(dicts,key))
                outdict[key] = value
            else:
                if key in outdict.keys():
                    if key in exception_list:
                        pass
                    else:
                        outdict[key] += value  # Add values if the key is common
                else:
                    outdict[key] = value  # Otherwise, add the new key-value pair

    return outdict

###################
# Input arguments #
###################
parser = argparse.ArgumentParser()
parser.add_argument(
    "-i",
    "--input",
    help="Enter the input directory where the coffea files are saved",
    default="outputs/FCCee/higgs/mH-recoil/mumu",
    type=str
)
inputs = parser.parse_args()

#Input configuration
input_path = inputs.input+"/"
base_filename = "output.coffea"
print(f'Current configuration:\n\tinput_path:\t{input_path}\n\tbase_filename:\t{base_filename}\n')
print("Loading coffea files...")

#Find coffea files
coffea_files = glob.glob(input_path+'*.coffea')
print('Detected coffea files:')
for file in coffea_files : print('\t'+file)
print(f'Choosing:\n\t{base_filename}')

#Find chunked coffea files and combine them
chunked_coffea_files = glob.glob(input_path+base_filename.split('.coffea')[0]+'-chunk*.coffea')
if len(chunked_coffea_files) != 0 :
    print('Joining chunks:')
    chunk_index_list = []
    chunk_list = []
    for file in chunked_coffea_files:
        print('\t'+file)
        chunk_list.append(file)
        chunk_index_list.append(int(re.search('-chunk(.*).coffea',file).group(1)))
    chunk_index_list.sort()

    #Check if there are missing chunks
    full_set = set(range(len(chunk_index_list)))
    lst_set = set(chunk_index_list)
    missing = list(full_set - lst_set)
    if len(missing) != 0:
        raise FileNotFoundError(f'Missing chunk indexes : {missing}')

    #Load and accumulate all the chunks
    input_list = [util.load(file) for file in chunk_list]
    input = accumulate(input_list)

#If there is only one chunk no need to join chunks
else :
    input = util.load(input_path+base_filename)

plot_meta = {
    'mass':{'xlabel':'Mass [GeV]', 'title': 'Z candidate mass'},
    'pt':{'xlabel':'$p_T$  [GeV]', 'title': 'Z candidate $p_T$'}
}

all_plots = list(plot_meta.keys())

#################################
### The main plotter function ###
#################################

def plot(list_of_keys, fig, ax):
    for key in list_of_keys:
        print(f'Plotting : Z {key} : {plot_meta[key]['title']}')
        hep.style.use(hep.styles.CMS)
        hep.histplot(
            [
                input['WW']['Histograms']['Z'][key],
                input['ZZ']['Histograms']['Z'][key],
            ],
            label=['WW','ZZ'],
            stack=True,
            histtype='fill',
            edgecolor='black',
            linewidth=1,
            color=['#2473d8','#24cad8'],
            xerr=0,
            yerr=0,
            ax=ax
        )
        ax.legend()
        ax.set_xlabel("Mass [GeV]")
        ax.set_ylabel("Events")
        ax.set_title(plot_meta[key]['title'], fontsize=15)
        ax.set_xticks(np.arange(hist_meta[f'Z_{key}']['start'],hist_meta[f'Z_{key}']['stop'], 10))
        hep.cms.text("Preliminary")
        fig.savefig(f"Z_{key}.png", dpi=240)
        plt.cla()

######################################
### Call the main plotter function ###
######################################

fig, ax = plt.subplots(figsize=(10,10))
plot(all_plots, fig, ax)
