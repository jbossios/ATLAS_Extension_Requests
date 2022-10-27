setupATLAS -3
source /cvmfs/sft.cern.ch/lcg/views/setupViews.sh LCG_101_ATLAS_2 x86_64-centos7-gcc8-opt
lsetup rucio
localSetupPyAMI
voms-proxy-init -voms atlas
