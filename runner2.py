# COFFEA Runner
# Author: Prayag Yadav
# Email: prayag.yadav@cern.ch
# Last Updated: 7 March 2025

if __name__=="__main__":
    from coffea import util
    import argparse
    import numpy as np
    import yaml
    import os
    import json
    import subprocess
    from coffea.dataset_tools import apply_to_fileset,max_chunks,preprocess
    from coffea.analysis_tools import Cutflow
    import dask
    import copy
    import time
    from dask.diagnostics import ProgressBar
    pgb = ProgressBar()
    pgb.register()


    #######################
    # Basic Configuration #
    #######################
     
    # Fileset name
    fileset_name = 'fileset.json'

    ## Load schema
    schema_name = 'NanoAODSchema'
    from coffea.nanoevents import NanoAODSchema
    schema = NanoAODSchema

    ## Load the processor
    processor_name = 'Zpeak'
    args_kwargs = ''
    from processor import Zpeak
    processor = Zpeak()


    ##############################
    # Define the terminal inputs #
    ##############################

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-e",
        "--executor",
        choices=["condor", "dask"],
        help="Enter where to run the file : dask(local) or condor (Note: Only dask(local) works at the moment)",
        default="dask",
        type=str
    )
    parser.add_argument(
        "-o",
        "--outfile",
        help="Enter the base-name of the coffea file to output with .coffea extension. By default: 'output'",
        default="output",
        type=str
    )
    parser.add_argument(
        "-p",
        "--path",
        help="Enter the path to save the output files. By default './')",
        default="./",
        type=str
    )
    parser.add_argument(
        "-m",
        "--maxchunks",
        help="Enter the number of chunks to be processed; by default 10",
        default=10,
        type=int
        )
    parser.add_argument(
        "-c",
        "--chunks",
        help="Enter the number of parallel chunks to be processed; by default 1",
        type=int,
        default=1
        )

    inputs = parser.parse_args()

    ###################################
    # Define functions and parameters #
    ###################################
    output_file = inputs.outfile+".coffea"
    path = inputs.path

    def break_into_many(input_fileset,n):
        '''
        Split a given fileset into n almost even filesets
        '''

        # Create an indexed fileset
        fileset = copy.deepcopy(input_fileset)
        index = 0
        for dataset in input_fileset.keys():
            for filename,treename in input_fileset[dataset]['files'].items():
                fileset[dataset]['files'][filename] = {'treename': treename, 'index': index}
                index += 1

        # Split the array as required
        nfiles = sum([len(fileset[dataset]['files']) for dataset in fileset.keys()])
        if n == 0 :
            return [input_fileset]
        elif n > 0 and n <= index:
            index_split = np.array_split(np.arange(nfiles),n)
        else :
            raise ValueError(f'Allowed values of n between 0 and {index}')

        # Choose the required indices for each split
        raw = [copy.deepcopy(input_fileset) for i in range(n)]
        for f in range(n):
            for dataset in fileset.keys():
                for event in fileset[dataset]['files'].keys():
                    if not fileset[dataset]['files'][event]['index'] in index_split[f]:
                        del raw[f][dataset]['files'][event]

        #remove empty fields
        out = copy.deepcopy(raw)
        for f in range(n):
            for dataset in raw[f].keys():
                if len(raw[f][dataset]['files']) == 0 :
                    del out[f][dataset]

        return out

    def create_job_python_file(dataset_runnable, maxchunks,filename, output_file):
        s = f'''
from coffea import util
from coffea.nanoevents import {schema_name}
import os
from processor import {processor_name}
from coffea.dataset_tools import apply_to_fileset,max_chunks
import dask

dataset_runnable = {dataset_runnable}
maxchunks = {maxchunks}

to_compute = apply_to_fileset(
            {processor_name}({args_kwargs}),
            max_chunks(dataset_runnable, maxchunks),
            schemaclass={schema_name},
)
computed = dask.compute(to_compute)
(Output,) = computed

print("Saving the output to : " , "{output_file}")
util.save(output= Output, filename="{output_file}")
print("File {output_file} saved")
print("Execution completed.")

        '''
        with open(filename,'w') as f:
            f.write(s)

    def create_job_shell_file(filename, python_job_file):
        s = f'''#!/usr/bin/bash
export COFFEA_IMAGE=coffeateam/coffea-dask-almalinux8:2024.5.0-py3.11
echo "Coffea Image: ${{COFFEA_IMAGE}}"
EXTERNAL_BIND=${{PWD}}
echo $(pwd)
echo $(ls)
singularity exec -B /etc/condor -B /eos -B /afs -B /cvmfs --pwd ${{PWD}} \
/cvmfs/unpacked.cern.ch/registry.hub.docker.com/${{COFFEA_IMAGE}} \
/usr/local/bin/python3 {python_job_file} -e dask >> singularity.log.{python_job_file.strip('.py')}
echo $(ls)'''
        with open(filename,'w') as f:
            f.write(s)

    def create_submit_file(filename, executable, input, output):
        s = f'''universe=vanilla
executable={executable}
+JobFlavour="espresso"
RequestCpus=1
should_transfer_files=YES
when_to_transfer_output=ON_EXIT_OR_EVICT
transfer_input_files={input}
transfer_output_files={output}
output=out-{executable.strip('job_').strip('.sh')}.$(ClusterId).$(ProcId)
error=err-{executable.strip('job_').strip('.sh')}.$(ClusterId).$(ProcId)
log=log-{executable.strip('job_').strip('.sh')}.$(ClusterId).$(ProcId)
queue 1'''
        with open(filename,'w') as f:
            f.write(s)

    def create_master_submit(submitfile_base_name, chunks, wait_time,):
        s = '#!/usr/bin/bash\n'
        for i in range(chunks):
            s += f'condor_submit {submitfile_base_name}_{i}.sh\nsleep {wait_time}\n'
        with open('condor.sh','w') as f:
            f.write(s)


    ###################
    # Run the process #
    ###################
    
    # Basic configuration
    fileset_name = 'fileset.json'
    ## Load schema
    from coffea.nanoevents import NanoAODSchema
    schema = NanoAODSchema
    ## Load the processor
    from processor import Zpeak
    processor = Zpeak()

    print('Loading fileset ...')

    with open(fileset_name) as file:
        myfileset = json.load(file)
    fileset = break_into_many(input_fileset=myfileset,n=inputs.chunks)

    print('Preparing fileset before run...')

    pwd = os.getcwd()

    dataset_runnable, dataset_updated = zip(*[preprocess(
        fl,
        align_clusters=False,
        step_size=50_000,
        files_per_batch=1,
        skip_bad_files=True,
        save_form=False,
    ) for fl in fileset ]
                                           )

    #For local dask execution
    if inputs.executor == "dask" :
        if not os.path.exists(path):
            os.makedirs(path)
        Output = []
        print("Executing locally with dask ...")
        for i in range(len(dataset_runnable)):
            print('Chunk : ',i)
            to_compute = apply_to_fileset(
                        processor,
                        max_chunks(dataset_runnable[i], inputs.maxchunks),
                        schemaclass=schema,
            )
            computed = dask.compute(to_compute)
            (Out,) = computed
            Output.append(Out)
            if inputs.chunks > 1:
                output_filename = output_file.strip('.coffea')+f'-chunk{i}'+'.coffea'
            else:
                output_filename = output_file
            print("Saving the output to : " , output_filename)
            util.save(output= Out, filename=path+output_filename)
            print(f"File {output_filename} saved at {path}")
        print("Execution completed.")

    #For condor execution
    elif inputs.executor == "condor" :
        print("Executing with condor ...")
        batch_dir = 'Batch'
        if not os.path.exists(batch_dir):
            os.makedirs(batch_dir)
        os.chdir(batch_dir)

        for i in range(len(dataset_runnable)):

            if inputs.chunks > 1:
                output_filename = output_file.split('.coffea')[0]+f'-chunk{i}'+'.coffea'
            else:
                output_filename = output_file
            create_job_python_file(
                dataset_runnable[i],
                inputs.maxchunks,
                f'job_{i}.py',
                output_filename,

            )
            print(f'\tjob_{i}.py created')
            create_job_shell_file(
                filename=f'job_{i}.sh',
                python_job_file=f'job_{i}.py'
            )
            subprocess.run(['chmod','u+x',f'job_{i}.sh'])
            print(f'\tjob_{i}.sh created')
            create_submit_file(
                filename=f'submit_{i}.sh',
                executable=f'job_{i}.sh',
                input=f'{pwd}/{batch_dir}/job_{i}.py,{pwd}/processor.py',
                output=f'singularity.log.job_{i},{output_filename}'
            )
            subprocess.run(['chmod','u+x',f'submit_{i}.sh'])
            print(f'\tsubmit_{i}.sh created')
        create_master_submit(submitfile_base_name='submit',chunks=inputs.chunks,wait_time=5)
        subprocess.run(['chmod','u+x','condor.sh'])
        print('condor.sh created')
        print(f'Action Needed: All the job files created; To submit them, move to the {batch_dir} directory and run ./condor.sh without getting into the singularity container shell.')

        os.chdir('../')
