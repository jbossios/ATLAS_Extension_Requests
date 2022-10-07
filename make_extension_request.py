import os

def make_extension_request(input_file, reason, expiration)
    command = f'rucio add-lifetime-exception --inputfile {input_file} --reason {reason} --expiration {expiration}'
    os.system(command)

if __name__ == '__main__':
    # settings
    filename = 'STDM4_mcSamples_07102022.txt'
    reason = 'Used by the SM W+jets analysis aiming for a paper for Moriond23'
    expiration_date = '2023-07-01'
    # run
    make_extension_request(f'ExtensionRequests/{filename}', f'"{reason}"', expiration_date)
