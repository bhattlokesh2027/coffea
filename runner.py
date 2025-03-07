from coffea.dataset_tools import preprocess, apply_to_fileset
from coffea.nanoevents import NanoAODSchema, NanoEventsFactory
from coffea import util
from processor import Zpeak
import json, dask

with open('fileset.json', 'r') as file:
    fileset = json.load(file)

print('Started execution ...')

dataset_runnable, dataset_updated = preprocess(
    fileset,
    step_size=50_000,
    align_clusters=False,
    files_per_batch=1,
    skip_bad_files=True,
    save_form=False,
)

print('Computing ...')

to_compute = apply_to_fileset(
    Zpeak(),
    fileset = dataset_runnable,
    schemaclass=NanoAODSchema

)

(out,) = dask.compute(to_compute,)

print('Saving the output')

util.save(out, 'output.coffea')

