# Extension requests

## Setup

```
source Setup.sh
```

This repository contains the following python3 scripts:

## ```prepare_extension_request.py```

It is used for obtaining the list of samples of interest to be deleted. Set ```samples_file``` to the name of a python file containing a ```mcSamples``` dict (of the form ```sample_key:[containers]```) or to a TXT file listing all containers, and set ```adc_mon_file``` to the name of the file containing the list of samples to be deleted.

## ```make_extension_request.py```

It is used for making an extension request using the list obtained using the above script. Set the following variables before running:

```filename```: Name of the file produced with the above script

```reason```: Reason for requesting an extension

```expiration_date```: New expiration date
