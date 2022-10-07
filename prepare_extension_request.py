from rucio.client import Client
import importlib
import logging
from multiprocessing import Pool
import os
from datetime import datetime

def find_datasets(sample: str) -> list:
    '''
    Get list of datasets for a given container
    '''
    rucio = Client()
    scope = sample.split('.')[0]
    return [sample['name'] for sample in rucio.list_content(scope=scope, name=sample)]


def main(samples_file, adc_mon_file, debug = False):
    '''
    Get list of datasets to be deleted
    '''
    # Set logger
    logging.basicConfig(level = 'INFO' if not debug else 'DEBUG', format = '%(levelname)s: %(message)s')
    log = logging.getLogger('prepare_extension_request')
    samples_module = importlib.import_module(samples_file.replace('/', '.').replace('.py', ''))
    samples_dict = samples_module.mcSamples
    log.info(f'Will check which datasets from samples_file are listed in {adc_mon_file}')
    
    # Get full list of containers
    containers = []
    for sample_key, samples_list in samples_dict.items():  # loop over sample keys
        log.debug(f'sample = {sample_key}')
        # Loop over containers
        for container in samples_list:
            log.debug(f'container = {container}')
            containers.append(container)
    
    # Get full list of datasets
    with Pool(4) as pool:
        dss = pool.map(find_datasets, containers)
    datasets = [dataset for ds_list in dss for dataset in ds_list]
    log.debug(f'datasets = {datasets}')
    
    # Get list of samples to be deleted
    with open(f'{adc_mon_file}', 'r') as adc_file:
        datasets_to_be_deleted = [dataset.split(' ')[0] for dataset in adc_file.readlines()]
    
    # Find matches
    matches = list(set(datasets).intersection(datasets_to_be_deleted))
    log.debug(f'matches = {matches}')
    
    # Write matches to a list
    out_folder_name = 'ExtensionRequests/'
    if not os.path.exists(out_folder_name):
        os.makedirs(out_folder_name)
    now = datetime.now()
    date = now.strftime("%d%m%Y")
    output_file_name = f'{out_folder_name}{samples_file.split("/")[-1].replace(".py", "")}_{date}.txt'
    log.info(f'Datasets to be deleted saved to {output_file_name}')
    with open(output_file_name, 'w') as ofile:
        for match in matches:
            ofile.write(match + '\n')


if __name__ == '__main__':
    samples_file = 'Samples/STDM4_mcSamples.py'
    adc_mon_file = 'adc-mon-inputs/07102022/everything.txt'
    main(samples_file, adc_mon_file, False)
