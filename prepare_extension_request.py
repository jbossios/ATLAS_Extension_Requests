# Rucio and AMI clients
from rucio.client import Client
import pyAMI.client
import pyAMI.atlas.api as ami_client

# Python modules
import importlib
import logging
import os
import sys
import re
import tqdm
from multiprocessing import Pool
from datetime import datetime
from functools import cache
from functools import partial

# Global settings
ADC_MON_FILE = 'adc-mon-inputs/07102022/everything.txt'
DEBUG = False

# Get Rucio and AMI clients
rucio_client = Client()
pyami_client = pyAMI.client.Client('atlas')
ami_client.init()


def is_mc(sample: str) -> bool:
    '''
    Return True if sample is an MC sample, return False if otherwise
    '''
    if 'data' in sample:
        return False
    return True


def find_ptag(sample: str) -> dict:
    '''
    Find p-tag
    '''
    pattern = re.compile(".*(?P<ptag>(p[0-9]{4})).*")
    match = pattern.match(sample)
    if match:
        return match.groupdict()['ptag']
    else:
        return None


def find_aod_type(sample: str) -> str:
    '''
    Return AOD type
    i.e. AOD or DAOD_FORMAT (examples: DAOD_JETM1, DAOD_PHYS, etc)
    '''
    return sample.split('.')[4]


def is_daod(aod_type: str) -> bool:
    '''
    Check AOD type and return True if it is of the form DAOD_FORMAT
    '''
    if 'DAOD' in aod_type:
        return True
    return False


def check_if_empty(dataset: str):
    '''
    Check if dataset is empty (i.e. if it has no files)
    '''
    scope = dataset.split('.')[0]
    files = [filename['name'] for filename in rucio_client.list_files(scope = scope, name = dataset)]
    if not len(files):  # this dataset has no files!
        message_level = 'WARNING' if 'DAOD' in dataset else '>>> ERROR <<<'
        print(f'{message_level}: No files were foud for the dataset={dataset}, this dataset is empty!')


@cache
def find_aod_container(container: str) -> str:
    '''
    Get corresponding AOD container for a DAOD container with pyAMI
    '''
    #prov = AtlasAPI.get_dataset_prov(client, container)
    prov = ami_client.get_dataset_prov(pyami_client, container)
    aod_container = prov['node'][1]['logicalDatasetName']
    return aod_container


def get_datasets(scope: str, name: str) -> list:
    '''
    Get datasets for a given container
    '''
    datasets = [dataset['name'] for dataset in rucio_client.list_content(scope = scope, name = name)]
    return datasets


def find_datasets(sample: str) -> list:
    '''
    Get list of datasets
    If a container is provided, its dataset list is retrieved with rucio
    If container is empty, will check for latest p-tag
    It will also check for empty datasets
    '''
    scope = sample.split('.')[0]

    # Check if it is a DAOD and identify its ptag
    isdaod = is_daod(find_aod_type(sample))
    if isdaod:
        ptag = find_ptag(sample)

    # Check if sample is MC or data
    ismc = is_mc(sample)

    # Treatment is different for containers and datasets

    if 'tid' not in sample:  # look for datasets in this container
        datasets = get_datasets(scope, sample)
        if not len(datasets):  # this container is empty!
            if isdaod:  # look for newer p-tags
                daod_samples = {find_ptag(dataset): dataset for dataset in rucio_client.list_dids(scope, filters=[{'name': sample.replace(ptag, 'p*')}])}
                latest_ptag = max([int(tag.replace('p', '')) for tag in daod_samples.keys()])
                if latest_ptag > int(ptag.replace('p', '')):
                    newer_sample = daod_samples[f'p{latest_ptag}']
                    print(f'INFO: No datasets were found for sample={sample}, this container is empty')
                    print(f'INFO:   But there is a newer p-tag: {newer_sample}')
                    datasets = [dataset['name'] for dataset in rucio_client.list_content(scope=scope, name=newer_sample)]
                    if not len(datasets):  # this container is empty!
                        print(f'WARNING: No datasets were found for the newer p-tag sample={newer_sample}, this container is empty!')
            else:
                message_level = 'WARNING' if 'DAOD' in sample else '>>> ERROR <<<'
                print(f'{message_level}: No datasets were found for sample={sample}, this container is empty!')

        # If DAOD, find corresponding AOD container
        if isdaod:
            # Left in case it is needed
            #if not is_mc(sample) and 'period' in sample:  # data period container
            #    # Find datasets for this container, then look for the corresponding AOD containers, then find the list of datasets for each AOD container
            #    daod_datasets = get_datasets(scope, sample)
            #    if not len(datasets):  # it's not possible to look for the AOD container of an empty DAOD container
            #        print(f'>>> ERROR <<<: This container is empty, hence I can not look for corresponding AODs!')
            #    else:
            #        aod_containers = [find_aod_container(dataset.split('_tid')[0]) for dataset in daod_datasets]
            #        for aod_container in aod_containers:
            #            datasets += get_datasets(scope, aod_container)
            #else:  # MC or data but not a period data container
            if ismc:
                # Find corresponding AOD container and then its datasets
                aod_datasets = get_datasets(scope, find_aod_container(sample))
                if not len(aod_datasets):  # this AOD container is empty!
                    print(f'>>> ERROR <<<: No datasets were found for this AOD container:{newer_sample}, this container is empty!')
                else:
                    datasets += aod_datasets

        # Check if any dataset is empty
        for dataset in datasets:
            check_if_empty(dataset)

        return datasets

    # This sample is a dataset
    datasets = [sample]

    # Find corresponding AOD container and its datasets
    if isdaod and ismc:
        datasets += get_datasets(scope, find_aod_container(sample.split('_tid')[0]))

    # Check if any dataset is empty
    for dataset in datasets:
        check_if_empty(dataset)

    return datasets


def remove_scope(sample: str) -> str:
    '''
    Remove scope from container name
    e.g. mc16_13TeV:mc16_TeV.dsid -> e.g. mc16_TeV.dsid
    '''
    if ':' in sample:
        return sample.split(':')[1]
    return sample


def format_line(line: str) -> str:
    '''
    Format string:
     remove end of line
     remove leading and trailing whitespaces
    '''
    return remove_scope(line.replace('\n', '').strip())


@cache
def find_datasets_to_be_deleted() -> list:
    '''
    Get list of samples to be deleted
    '''
    with open(ADC_MON_FILE, 'r') as adc_file:
        datasets_to_be_deleted = [dataset.split(' ')[0] for dataset in adc_file.readlines()]
    return datasets_to_be_deleted


def find_matches(sample: str) -> list:
    '''
    Check if this sample will be deleted
    If sample is a container, its datasets are checked
    For an MC sample, the corresponding AOD is also checked
    '''
    # Get full list of datasets
    datasets = find_datasets(sample)
    if DEBUG:
        print(f'DEBUG: datasets = {datasets}')

    # Get list of datasets to be deleted
    datasets_to_be_deleted = find_datasets_to_be_deleted()

    # Find matches
    matches = list(set(datasets).intersection(datasets_to_be_deleted))
    if DEBUG:
        print(f'DEBUG: matches = {matches}')
    return matches


def main(samples_file):
    '''
    Get list of datasets to be deleted
    '''
    # Set logger
    logging.basicConfig(level = 'INFO' if not DEBUG else 'DEBUG', format = '%(levelname)s: %(message)s')
    log = logging.getLogger('prepare_extension_request')
    log.info(f'Will check which datasets from {samples_file} are listed in {ADC_MON_FILE}')
    
    # Get full list of sample names
    samples = []
    if not os.path.exists(samples_file):
        log.fatal(f'{samples_file} can not be found, exiting')
        sys.exit(1)
    if samples_file.endswith('.py'):
        samples_module = importlib.import_module(samples_file.replace('/', '.').replace('.py', ''))
        samples_dict = samples_module.mcSamples
        for sample_key, samples_list in samples_dict.items():  # loop over sample keys
            log.debug(f'sample_key = {sample_key}')
            # Loop over sample names
            for sample in samples_list:
                log.debug(f'sample = {sample}')
                samples.append(sample)
    elif samples_file.endswith('.txt'):
        with open(samples_file, 'r') as ifile:
            samples = [format_line(line) for line in ifile.readlines() if format_line(line) and not line.startswith('#')]
    else:
        log.fatal(f'Format not supported: {samples_file}')
        sys.exit(1)

    # Get list of datasets and find those that will be deleted
    with Pool(4) as pool:
        mss = list(tqdm.tqdm(pool.imap(find_matches, samples), total=len(samples)))
    matches = []
    if len(mss):
        for matches_list in mss:
            if len(matches_list):
                matches += matches_list
    log.debug(f'matches = {matches}')

    # Write matches to a list
    out_folder_name = 'ExtensionRequests/'
    if not os.path.exists(out_folder_name):
        os.makedirs(out_folder_name)
    now = datetime.now()
    date = now.strftime("%d%m%Y")
    output_file_name = f'{out_folder_name}{samples_file.split("/")[-1].replace(".py", "").replace(".txt", "")}_{date}.txt'
    if len(matches):
        log.info(f'Datasets to be deleted ({len(matches)}) saved to {output_file_name}')
        with open(output_file_name, 'w') as ofile:
            for match in matches:
                ofile.write(match + '\n')
    else:
        log.info(f'No datasets will be deleted')


if __name__ == '__main__':
    samples_file = 'Samples/Insitu/R21_JETM1_SmallR_EtaIntercalibration.txt'
    #samples_file = 'Samples/Insitu/R21_JETM3_SmallR_Zjet.txt'
    #samples_file = 'Samples/Insitu/R21_JETM4_SmallR_gammajet.txt'
    main(samples_file)
