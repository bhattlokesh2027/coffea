from coffea import util

out = util.load('output.coffea')

import mplhep as hep
import numpy as np
import matplotlib.pyplot as plt

from processor import hist_meta
fig,ax = plt.subplots()
hep.style.use(hep.styles.CMS)
hep.histplot(
    [
        out['WW']['Histograms']['Z']['mass'],
        out['ZZ']['Histograms']['Z']['mass'],
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
ax.set_title("Z candidate mass", fontsize=15)
ax.set_xticks(np.arange(hist_meta['Z_mass']['start'],hist_meta['Z_mass']['stop'], 10))
hep.cms.text("Preliminary")
fig.savefig("Z_mass.png", dpi=240)

fig,ax = plt.subplots()
hep.style.use(hep.styles.CMS)
hep.histplot(
    [
        out['WW']['Histograms']['Z']['pt'],
        out['ZZ']['Histograms']['Z']['pt'],
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
ax.set_xlabel("$p_T$ [GeV]")
ax.set_ylabel("Events")
ax.set_title("Z candidate $p_T$", fontsize=15)
ax.set_xticks(np.arange(hist_meta['Z_pt']['start'],hist_meta['Z_pt']['stop'], 10))
hep.cms.text("Preliminary")
fig.savefig("Z_pt.png", dpi=240)


def plot(list_of_keys):
    for key in list_of_keys:

    pass
