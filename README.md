# Extension requests

## Setup

```
source Setup.sh
```

This repository contains the following python3 scripts:

## ```prepare_extension_request.py```

It is used for obtaining the list of samples of interest to be deleted. Set ```STDM4_mcSamples.py``` with the name of the file containing the ```mcSamples``` dict (of the form ```sample_key:[containers]```) and set ```adc_mon_file``` with the name of the file containing the list of samples to be deleted.

## ```make_extension_request.py```

It is used for making an extension request using the list obtained using the above script. Set the following variables before running:

```filename```: Name of the file produced with the above script

```reason```: Reason for requesting an extension

```expiration_date```: New expiration date
