###### init ####
import numpy as np
import scipy as sp
import h5py
import scipy.constants as const
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

##### local imports
#### this import Richard's color map for publications
#from richard import *
#### import simple definitions for getting fields and particles easily from h5 files
from get_fields_and_particles import *

from settings import *
import sys

timestep =int(sys.argv[1])

N_macro = "-N100000"

fs = 15
plt.rcParams.update({'font.size': fs})
plt.rcParams['xtick.direction'] = 'in'
plt.rcParams['ytick.direction'] = 'in'
plt.rcParams['axes.facecolor'] = 'white'
plt.rcParams['figure.facecolor'] = 'white'
filename = "reduced_data{}ts{}.h5".format(timestep, N_macro)
#species  = 'en_all'
simulation_box_size = [1.36089598e-04, 8.93087984e-05, 1.36089598e-04]
outfilename = "reduced-{}ts{}.txt".format(timestep, N_macro)

f = h5py.File(filename, "r")
ux = load_momentum("x", timestep, f, species=species)
uy = load_momentum("y", timestep, f, species=species)
uz = load_momentum("z", timestep, f, species=species)
x  = load_position("x", timestep, f, species=species) - simulation_box_size[0]*0.5e6
y  = load_position("y", timestep, f, species=species)
z  = load_position("z", timestep, f, species=species) - simulation_box_size[2]*0.5e6
w  = load_weighting(timestep, f, species=species)

## compile data for GPT
G = np.sqrt(1.0 + ux**2.0 + uy**2.0 + uz**2.0)
Bx = ux/G
By = uy/G
Bz = uz/G
y  -= np.average(y)

#data_array = np.array([x,y,z, GBx, GBy, G])
### the data array contains the data in the order for GPT, ie. beam moves in Y[PIC] == Z[GPT]
### for this reason, y and z are flipped
data_array = np.array([1e-6*x,1e-6*z,1e-6*y, Bx, Bz, By, G, w])
#header  = "x y z GBx GBy GBz"
header  = "x y z Bx By Bz G nmacro"
labels    = "x", "y", "z", "Bx", "By", "Bz", "G", "nmacro"

### ploting data for overview issues
fig = plt.figure(constrained_layout=False, figsize=(10,10))
spec = fig.add_gridspec(ncols=3, nrows=3)
spec.update(wspace=0.2, hspace=0.20) # set the spacing between axes.

for i, ii in enumerate(data_array):
    fig.add_subplot(spec[i//3, i%3])
    plt.hist(ii, bins=50, alpha=0.8)
    plt.ylabel("#")
    plt.text(np.mean(ii),0, "RMS {:.1e}".format(np.std(ii)), horizontalalignment='center')
    plt.title(labels[i])
    #plt.show()
plt.savefig(outfilename+"-overview.png")
plt.hist2d(y, G*0.511, bins=100)
plt.xlabel("z (PIC)[µm]")
plt.ylabel("Energy [MeV/c²]")
plt.savefig(outfilename+"-longitudinal-ps.png")

### Saving data as TXT
np.savetxt(fname=outfilename, X=data_array.T, header=header, comments='')
