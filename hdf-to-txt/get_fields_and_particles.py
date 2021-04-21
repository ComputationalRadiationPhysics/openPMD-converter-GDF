## funciton definition
import numpy as np
import scipy.constants as const
import h5py

def mask(array, mask):
    """
    instead of loading all data into memory at once, just compare it block by block 
    with a blocksize of "stepsize"
    enables loading of huge arrays and mask them
    
    this works for one dimensional arrays
    
    returns logically array[mask==True]
    """
    stepsize = 2**20
    return np.concatenate([array[a:a+stepsize][mask[a:a+stepsize]] for a in range(0,mask.shape[0],stepsize)])


def load_position(label, timestep, f, species="en"):
    """
    returns position on axis "label" in µm
    TODO: use maybe .value
    """
    x_incell = f['/data/{}/particles/{}/positionOffset/{}'.format(timestep, species, label)]
    x_incell_unitSI = x_incell.attrs["unitSI"]
    x_offset= f['/data/{}/particles/{}/position/{}'.format(timestep, species, label)]
    x_offset_unitSI = x_offset.attrs["unitSI"]
    
    #if label=="y":
    #    properties = f['/data/{}/fields/{}'.format(timestep, "E")]
    #    offset = properties.attrs["gridGlobalOffset"] 
    #    unitSI = properties.attrs["gridUnitSI"]
    #    ## normaly the laser propagates along y axis, there is no global offset for other axis reasonable
    #    offset = offset[1]
    #else:
    #    offset = 0.0
    ##return (offset*unitSI + x_incell_unitSI*np.array(x_incell) + x_offset_unitSI*np.array(x_offset))*1e6
    return (x_incell_unitSI*np.array(x_incell) + x_offset_unitSI*np.array(x_offset))*1e6

def load_position_masked(label, timestep, f, dataset_mask, species="en"):
    """
    returns position on axis "label" in µm
    TODO: use maybe .value
    """
    x_incell = f['/data/{}/particles/{}/positionOffset/{}'.format(timestep, species, label)]
    x_incell_unitSI = x_incell.attrs["unitSI"]
    x_offset= f['/data/{}/particles/{}/position/{}'.format(timestep, species, label)]
    x_offset_unitSI = x_offset.attrs["unitSI"]
    
    return (x_incell_unitSI*mask(x_incell,dataset_mask) + x_offset_unitSI*mask(x_offset, dataset_mask))*1e6

def load_momentum(label, timestep, f, species="en"):
    """
    labels is on of {x,y,z}
    returns normalized momentum on axis "label" (\gamma \beta_x)
    weighting needs to taken into account for correct momentum
    """
    u = f['/data/{}/particles/{}/momentum/{}'.format(timestep, species, label)]  # ~ gamma m \beta c
    w = f['/data/{}/particles/{}/weighting'.format(timestep, species)]  # 
    u_unitSI = u.attrs["unitSI"]
    return u_unitSI*np.array(u) /np.array(w)  /(const.m_e*const.c) 
                                                                                                 
def load_momentum_masked(label, timestep, f, dataset_mask, species="en"):
    """
    labels is on of {x,y,z}
    returns normalized momentum on axis "label" (\gamma \beta_x)
    weighting needs to taken into account for correct momentum
    """
    u = f['/data/{}/particles/{}/momentum/{}'.format(timestep, species, label)]  # ~ gamma m \beta c
    w = f['/data/{}/particles/{}/weighting'.format(timestep, species)]  # 
    u_unitSI = u.attrs["unitSI"]
    return u_unitSI * mask(u, dataset_mask) / (mask(w, dataset_mask))  /(const.m_e*const.c) 

def load_weighting(timestep, f, species="en"):
    """
    returns weighting on axis "label" 
    weighting needs to taken into account for correct momentum, charge, aso
    """
    w        = f['/data/{}/particles/{}/weighting'.format(timestep, species)]  # 
    return w


def load_weighting_masked(timestep, f, dataset_mask, species="en"):
    """
    returns weighting on axis "label" 
    weighting needs to taken into account for correct momentum, charge, aso
    """
    w        = f['/data/{}/particles/{}/weighting'.format(timestep, species)]  # 
    return mask(w[()], dataset_mask)


def load_id(timestep, f, species="en"):
    """
    returns id of particle 
    """
    w        = f['/data/{}/particles/{}/particleId'.format(timestep, species)]  # 
    return w

def load_momentums(timestep, f, species="en"):
    """
    returns 
        * energy of particle (gamma)
        * normalized momentums for all axis (\gamma \beta_x)
    Important: weighting needs to taken into account for correct momentum
    """
    momentums = np.array([load_momentum(l, timestep, f,species) for l in ["x","y","z"]])
    return np.sqrt(np.sum(momentums**2.0, axis=0)), momentums

def load_field(fieldtype, label, timestep, f, 
                 xstart=None, xstop=None,
                 ystart=None, ystop=None,
                 zstart=None, zstop=None,
                     y_offset=0.0):
    """
    latest and improved version
    """        
    field_handle = f['/data/{}/fields/{}/{}'.format(timestep, fieldtype, label)]
    field_unitSI = field_handle.attrs["unitSI"]
    field = field_handle[zstart:zstop, ystart:ystop, xstart:xstop]
    properties = f['/data/{}/fields/{}'.format(timestep, fieldtype)]
    offset   = properties.attrs["gridGlobalOffset"]  #+ np.array([z0,y0,x0])
    spacing  = properties.attrs["gridSpacing"]
    unitSI   = properties.attrs["gridUnitSI"]
    size     = np.array(field.shape)
    z,y,x    = 1e6*(offset + np.array([np.arange(0,size[i]*spacing[i], spacing[i]) for i in range(3)]))*unitSI
    z         -= z[int(field.shape[0]//2)]
    x         -= x[int(field.shape[2]//2)]
    # reformulate 
    y       -= y_offset
    return field_unitSI*np.array(field), z,y,x

def get_position(timestep, f):
    """
    returns position in mm of snapshot
    """
    pos = f["/data/{}".format(timestep)].attrs["timeUnitSI"]*timestep*const.c
    return pos*1e3

def get_size(fieldtype, label, timestep, f):
    return f['/data/{}/fields/{}/{}'.format(timestep, fieldtype, label)].attrs["_size"]

def get_field(fieldtype, label, timestep, f, 
              z_slice=None, y_slice=None, x_slice=None):
    """
    """
    z0,y0,x0 = [0,0,0]
    z1,y1,x1 = get_size(fieldtype, label, timestep, f)
    # overwrite values if submitted as arguments
    if z_slice==None:
        z_slice = slice(z0, z1)
    if y_slice==None:
        y_slice = slice(y0, y1)
    if x_slice==None:
        x_slice = slice(x0, x1)
    
    if z_slice.start==None:
        z0=z_slice.stop
    else:
        z0=z_slice.start
    if y_slice.start==None:
        y0=y_slice.stop
    else:
        y0=y_slice.start
    if x_slice.start==None:
        x0=x_slice.stop
    else:
        x0=x_slice.start
    field = f['/data/{}/fields/{}/{}'.format(timestep, fieldtype, label)]
    field_unitSI = field.attrs["unitSI"]
    field = field[z_slice, y_slice, x_slice]
    properties = f['/data/{}/fields/{}'.format(timestep, fieldtype)]
    offset = properties.attrs["gridGlobalOffset"]  + np.array([z0,y0,x0])
    spacing = properties.attrs["gridSpacing"]
    unitSI = properties.attrs["gridUnitSI"]
    size   = np.array(field.shape)
    z,y,x  = (offset + np.array([np.arange(0,size[i]*spacing[i], spacing[i]) for i in range(3)]))*unitSI
    return field_unitSI*np.array(field), z,y,x

def get_density(density, timestep, f):
    """
    """
    d = f['/data/{}/fields/{}'.format(timestep, density)]
    d_unitSI  = d.attrs["unitSI"]
    return np.array(d)*d_unitSI

def get_si_size(timestep, f, d="e_chargeDensity"):
    """
    returns the size of the simulation box in SI units (meter)
    """
    path = "/data/{}/fields/".format(timestep) + d
    handle = f[path].attrs
    simSize = handle["_size"] 
    gridSpacing = handle["gridSpacing"]
    gridUnitSI = handle["gridUnitSI"]
    return np.array([simSize[i]*gridSpacing[i]*gridUnitSI for i in range(3)])

def get_cell_bins(f, timestep, fieldtype="E", axis="y"):
    """
    returns a 1d array for the axis $axis.
    The size is gridsize+1 such as x0, x1, ...x(n-1), x(n)
    The array is derived from the cell grid
    TODO: what does grid["position"] mean? 
    """
    grid        = f["/data/{}/fields/{}".format(timestep, fieldtype)]
    field       = grid[axis]
    offset      = grid.attrs["gridGlobalOffset"]
    spacing     = grid.attrs["gridSpacing"]
    unitSI      = grid.attrs["gridUnitSI"]
    size        = field.attrs["_size"]
    ##size        = field.shape
    cell_axis_y = np.arange(offset[1], offset[1] + ((size[1]+1)*spacing[1]), spacing[1])
    return unitSI * cell_axis_y
