""" 
Input:
+ timestep
+ filename_bunch_idendifiers
+ filename_filtered_file
""" 
###### init ####
import numpy as np
import scipy as sp
import h5py
import scipy.constants as const
from settings import *
import sys

timestep =int(sys.argv[1])
#timestep                   = 60000
#filename_bunch_idendifiers = "bunch-identifiers.dat"
################ WARING ################
# The filtered file will be overwritten, copy this file instead from the original data
filename_filtered_file     = "simData_filtered_{}.h5".format(timestep)

# optional
#data_directory = './extract-particles-for-gpt/particle_reduction/'
data_directory = ''

# read particle ids for filtering
ids = np.loadtxt(filename_bunch_idendifiers, dtype=np.uint64)

##### open h5 files 
filtered_file = h5py.File(data_directory+filename_filtered_file, "r+")
#orig_file = h5py.File(data_directory+"/simData_run006_orig_{}.h5".format(timestep), "r")

h = filtered_file["/data/{}/particles/en_all".format(timestep)]
current_ids = h["particleId"][()]
m   = np.in1d(current_ids, ids)
if m.sum() != len(ids):
    print("ERR: requested IDs are not fully contained in H5 file. Abort.")
    exit

paths = ["particleId", "weighting",
             "momentum/x", "momentum/y", "momentum/z",
             "momentumPrev1/x", "momentumPrev1/y", "momentumPrev1/z",
             "position/x", "position/y", "position/z",
             "positionOffset/x", "positionOffset/y", "positionOffset/z"]
for p in paths:
    temp = h[p][m]
    temp_items = h[p].attrs.items()
    h.__delitem__(p)
    h[p]  = temp
    for i in temp_items:
        h[p].attrs.create(name=i[0], data=i[1])
for p in ["mass", 'charge']:
    temp = h[p].attrs['shape']
    temp[0] = np.sum(m)
    h[p].attrs['shape'] = temp

#### delete particle patches because reduction script does not process them correctly
filtered_file["/data/{}/particles/en_all/".format(timestep)].__delitem__('particlePatches')

# important: close them to save them to disk
filtered_file.close()
#orig_file.close()

