# Extension requests

## Setup

```
source Setup.sh
```

This repository contains the following python3 scripts:

## ```prepare_extension_request.py```

Use this script to obtain the list of samples of interest to be deleted.

1. Get the list of samples to be deleted from [https://adc-mon.cern.ch/lifetime-model/results/latest/beyond-lifetime-centrally-managed/](https://adc-mon.cern.ch/lifetime-model/results/latest/beyond-lifetime-centrally-managed/). You should download the ```everything.txt.gz``` file and uncompress it to obtain the ```everything.txt``` file and locate it in the ```adc-mon-inputs/``` folder (preferably within a new folder).

2. Take a look at the ```prepare_extension_request.py```. Set ```samples_file``` to the name of a TXT file listing all containers/datasets (it can support a mix of them) and set ```adc_mon_file``` to the name of the file containing the list of samples to be deleted (see 1.). ```samples_file``` could also be a python file containing a ```mcSamples``` dict (of the form ```sample_key:[containers]````).

## ```make_extension_request.py```

Use this script to make an extension request using the list obtained using the above script. Set the following variables before running:

```filename```: Name of the file produced with the above script

```reason```: Reason for requesting an extension

```expiration_date```: New expiration date
