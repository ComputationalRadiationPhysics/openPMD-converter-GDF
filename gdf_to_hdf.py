"""Converter from GPT format to openPMD"""


from __future__ import division
from pylab import *
import h5py
import struct
import os
import sys
import datetime
import re
import numpy as np



class Datasets_functor():
    """
    """

    def __init__(self):
        self.datasets = []

    def __call__(self, name, node):
        if isinstance(node, h5py.Dataset):
            self.datasets.append(node)
        return None


class Momentum_base_group_functor():
    """
    """

    def __init__(self):
        self.momentum = []

    def __call__(self, name, node):

        if isinstance(node, h5py.Group):
            if node.name.endswith('momentum'):
                self.momentum.append(node)
        return None


class Particle_types_elements_functor():

    def __init__(self):
        self.mass = []
        self.charge = []

    def __call__(self, name, node):
        if isinstance(node, h5py.Dataset):
            if node.name.endswith('mass'):
                self.mass = node.value

            if node.name.endswith('charge'):
                self.charge = node.value
        return None


def parse_name_array(gdf_file, size_gdf_name):
    """Parse ascii value"""

    list_name = list(gdf_file.read(size_gdf_name))
    name = []
    for element in list_name:
        if element is 0:
            break
        else:
            name.append(chr(element))
    parsing_name = ''.join(name)
    return parsing_name


def add_creator_name(gdf_file, hdf_file, size_gdf_name):
    """ Add name of creator to root structure"""

    software_name = parse_name_array(gdf_file, size_gdf_name)
    hdf_file.attrs.create('software', software_name, None, dtype='<S10')


def add_dest_name(gdf_file, hdf_file, size_gdf_name):
    """Add destination name to root directory """

    destination_name = parse_name_array(gdf_file, size_gdf_name)
    hdf_file.attrs.create('destination', destination_name, None, dtype='<S10')


def add_creation_time(gdf_file, hdf_file):
    """Add when the gdf file file was created to root directory
    of openPMD file.
    We use next time and data format: YYYY-MM-DD HH:mm:ss tz
        """
    time_created = struct.unpack('i', gdf_file.read(4))[0]
    format_time = datetime.datetime.fromtimestamp(time_created)
    format_time = format_time.strftime("%Y-%m-%d %H:%M:%S %Z")
    hdf_file.attrs.create('date', format_time, None, dtype='<S25')


def add_gdf_version(gdf_file, hdf_file):
    """Add gdf version to root directory """

    major = struct.unpack('B', gdf_file.read(1))[0]
    minor = struct.unpack('B', gdf_file.read(1))[0]
    hdf_file.attrs.create('gdf_version', str(major) + '.' + str(minor), None, dtype='<S8')


def add_software_version(gdf_file, hdf_file):
    """Add software version to root directory """

    major = struct.unpack('B', gdf_file.read(1))[0]
    minor = struct.unpack('B', gdf_file.read(1))[0]
    hdf_file.attrs.create('softwareVersion', str(major) + '.' + str(minor), None, dtype='<S8')


def add_destination_version(gdf_file, hdf_file):
    """Add destination version to root directory """

    major = struct.unpack('B', gdf_file.read(1))[0]
    minor = struct.unpack('B', gdf_file.read(1))[0]
    hdf_file.attrs.create('destination_version', str(major) + '.' + str(minor), None, dtype='<S8')


def add_root_attributes(hdf_file, gdf_file, size_gdf_name):
    """Add root attributes to result hdf_file
    Attributes:
        gdf_version, software version, destination_version, iterationEncoding,
        iterationFormat, particlesPath openPMD version, openPMDextension,
        base path
       """

    add_creation_time(gdf_file, hdf_file)
    add_creator_name(gdf_file, hdf_file, size_gdf_name)
    add_dest_name(gdf_file, hdf_file, size_gdf_name)
    add_gdf_version(gdf_file, hdf_file)
    add_software_version(gdf_file, hdf_file)
    add_destination_version(gdf_file, hdf_file)

    hdf_file.attrs.create('iterationEncoding', 'fileBased', None, dtype='<S9')
    hdf_file.attrs.create('iterationFormat', 'test_hierical_%T.h5', None, dtype='<S10')
    hdf_file.attrs.create('particlesPath', 'particles/', None, dtype='<S10')

    hdf_file.attrs.create('openPMD', '1.1.0', None, dtype='<S5')
    hdf_file.attrs.create('iterationFormat', 'data%T.h5', None, dtype='<S9')
    hdf_file.attrs.create('openPMDextension', 0, None, dtype=np.dtype('uint32'))
    hdf_file.attrs.create('basePath', '/data/%T/', None, dtype='<S9')


def find_one_symbol_attribute(name):
    dict_one_symbol = {'x': ['position', 'x'], 'y': ['position', 'y'], 'z': ['position', 'z'],
                       'G': ['none', 'G'], 'q': ['charge', 'charge'], 'm': ['mass', 'mass']}
    return dict_one_symbol.get(name[0])


def find_two_symbols_attribute(name):
    dict_two_symbols = {'Bx': ['momentum', 'x'], 'By': ['momentum', 'y'], 'Bz': ['momentum', 'z'],
                        'ID': ['none', 'id']}
    if len(name) < 2:
        return None
    current_name = name[0:2]
    return dict_two_symbols.get(current_name)


def find_three_symbols_attribute(name):
    dict_three_symbols = {'fBx': ['fB', 'x'], 'fBy': ['fB', 'y'], 'fBz': ['fB', 'z'],
                          'fEx': ['fE', 'x'], 'fEy': ['fE', 'y'], 'fEz': ['fE', 'z'],
                          'rxy': ['none', 'rxy']}
    if len(name) < 3:
        return None
    current_name = name[0:3]
    return dict_three_symbols.get(current_name)


def find_multiple_symbols_attribute(name):
    dict_multiple_symbols = {'stdx': ['std', 'x'], 'stdy': ['std', 'y'], 'stdz': ['std', 'z'],
                          'avgx': ['avg', 'x'], 'avgy': ['avg', 'y'], 'avgz': ['avg', 'z'],
                          'avgBx': ['avgB', 'x'], 'avgBy': ['avgB', 'y'], 'avgBz': ['avgB', 'z'],
                          'avgFEx': ['avgFE', 'x'], 'avgFEy': ['avgFE', 'y'], 'avgFEz': ['avgFE', 'z'],
                          'avgFBx': ['avgFB', 'x'], 'avgFBy': ['avgFB', 'y'], 'avgFBz': ['avgFB', 'z'],
                          'avgr': ['none', 'avgr'], 'avgG': ['none', 'avgG'],
                          'stdt': ['none', 'stdt'], 'stdG': ['none', 'stdG'],
                          'stdBx': ['stdB', 'x'], 'stdBy': ['stdB', 'y'], 'stdBz': ['stdB', 'z'],
                          'rmacro': ['none', 'rmacro'], 'nmacro': ['none', 'nmacro'], 'avgt': ['none', 'avgt'],
                          'nemixrms': ['none', 'nemixrms'], 'nemiyrms': ['none', 'nemiyrms'],
                          'nemizrms': ['none', 'nemizrms'], 'avgzrms': ['none', 'avgzrms'],
                          'time': ['none', 'time'], 'positionOffset_x': ['positionOffset', 'x'],
                          'positionOffset_y': ['positionOffset', 'y'], 'positionOffset_z': ['positionOffset', 'z']}

    return dict_multiple_symbols.get(name)


def find_attribute(name):
    if find_one_symbol_attribute(name) != None:
        return find_one_symbol_attribute(name)
    elif find_two_symbols_attribute(name) != None:
        return find_two_symbols_attribute(name)
    elif find_three_symbols_attribute(name) != None:
        return find_three_symbols_attribute(name)
    elif find_multiple_symbols_attribute(name) != None:
        return find_multiple_symbols_attribute(name)
    else:
        return None


class Elements:
    """ Dictionary of each Record unitDimension
    1 - length L, 2 - mass M, 3 - time T, 4 - electric current I,
    4 - thermodynamic temperature theta, 6 - amount of substance N,
    7 - luminous intensity J
    https://github.com/openPMD/openPMD-standard/blob/latest/STANDARD.md#required-for-each-record
        """

    dict_dimensions = {'position': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),   # length = 1

                       'mass': (0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0),       # mass = 1
                       'momentum': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),  # length = 1, time = -1
                       'G': (-1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0),         # length = -1, time = 1
                       'charge': (0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0),     # time = 1, electric current = 1
                       'fE': (1.0, 1.0, -3.0, -1.0, 0.0, 0.0, 0.0),       # length = 1, mass = 1,  time = -3, electric current = -1
                       'fB': (0.0, 1.0, -2.0, -1.0, 0.0, 0.0, 0.0),       # mass  = 1, time = -2, electric current = -1
                       'std': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),        # length = 1
                       'stdB': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),      # length = 1, time = -1
                       'stdt': (0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0),       # time = 1
                       'stdG': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),      # length = -1, time = 1
                       'time': (0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0),       # time = 1
                       'rmacro': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),     # length = 1
                       'nmacro': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),     # length = 1
                       'avg': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),        # length = 1
                       'avgB': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),      # length = 1, time = -1
                       'avgr': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),       # length = 1
                       'avgG': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),      # length = -1, time = 1
                       'avgt': (0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0),       # time = 1
                       'avgFE': (1.0, 1.0, -3.0, -1.0, 0.0, 0.0, 0.0),    # length = 1, mass = 1,  time = -3, electric current = -1
                       'avgFB': (0.0, 1.0, -2.0, -1.0, 0.0, 0.0, 0.0),    # mass  = 1, time = -2, electric current = -1
                       'rxy': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),        # length = 1
                       'id': (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),         # 0
                       'avgzrms': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),    # length = 1
                       'avgzyms': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),    # length = 1, time = -1
                       'nemiyrms': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),   # length = 1, time = -1
                       'nemizrms': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),   # length = 1, time = -1
                       'nemixrms': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0)}   # length = 1, time = -1


    dict_weightingPower = {'position': 0., 'mass': 1., 'charge': 1., 'momentum': 1.,
                           'G': 1., 'rmacro': 0., 'nmacro': 0., 'fE': 1., 'fB': 1.}

    dict_macroWeighted = {'position': 0, 'mass': 1, 'charge': 1, 'momentum': 1,
                           'G': 1, 'rmacro': 0, 'nmacro': 0, 'fE': 1, 'fB': 1}


def add_weightingPower_attribute(name_atribute, attribute_dataset):
    if Elements.dict_weightingPower.get(name_atribute[1]) != None:
        attribute_dataset.attrs.create \
            ('weightingPower', Elements.dict_weightingPower.get(name_atribute[1]), None, dtype=np.dtype('float'))


def add_macroWeighted_attribute(name_atribute, attribute_dataset):
    if Elements.dict_macroWeighted.get(name_atribute[1]) != None:
        attribute_dataset.attrs.create \
            ('macroWeighted', Elements.dict_macroWeighted.get(name_atribute[1]), None, dtype=np.dtype('uint32'))


def add_dataset_attributes(gdf_file, particles_group, name_atribute, size):
    """  Add required attributes to dataset """

    value = fromfile(gdf_file, dtype=dtype('f8'), count=int(size / 8))
    particles_group.create_dataset(name_atribute[1], data=value)
    attribute_dataset = particles_group.require_dataset(name_atribute[1], value.shape, dtype=dtype('f8'))
    attribute_dataset.attrs.create('unitSI', 1.0, None, dtype=np.dtype('float'))
    attribute_dataset.attrs.create('timeOffset', 0.0, None, dtype=np.dtype('float'))
    attribute_dataset.attrs.create('unitDimension',
                                 Elements.dict_dimensions.get(name_atribute[1]), None, dtype=np.dtype('float'))
    add_weightingPower_attribute(name_atribute, attribute_dataset)
    add_macroWeighted_attribute(name_atribute, attribute_dataset)


def add_group_attributes(gdf_file, particles_group, name_atribute, size):
    """  Add required attributes to dataset """

    sub_group = particles_group.require_group(name_atribute[0])
    sub_group.attrs.create('unitDimension', Elements.dict_dimensions.get(name_atribute[0]), None, dtype=np.dtype('float'))
    sub_group.attrs.create('timeOffset', 0.0, None, dtype=np.dtype('float'))
    add_weightingPower_attribute(name_atribute, sub_group)
    add_macroWeighted_attribute(name_atribute, sub_group)
    value = fromfile(gdf_file, dtype=dtype('f8'), count=int(size / 8))
    sub_group.create_dataset(name_atribute[1], data=value)
    sub_group.require_dataset(name_atribute[1], value.shape, dtype=dtype('f8')).attrs.create\
        ('unitSI', 1.0, None, dtype=np.dtype('float'))


def name_to_group(name, particles, size, gdf_file):
    """Add dataset to correct group in particles group
        Args:
            particles - particles group
            name - name of dataset in gdf_file
            size - size of dataset in gdf_file, in bytes
            gdf_file - input file GPT

           """

    if find_attribute(name) != None:
        name_atribute = find_attribute(name)
        if name_atribute[0] == 'none':
            add_dataset_attributes(gdf_file, particles, name_atribute, size)
        else:
            add_group_attributes(gdf_file, particles, name_atribute, size)
    else:
        value = fromfile(gdf_file, dtype=dtype('f8'), count=int(size / 8))
        particles.create_dataset(name, data=value)
        attribute_dataset = particles.require_dataset(name, value.shape, dtype=dtype('f8'))
        attribute_dataset.attrs.create('unitSI', 1.0, None, dtype=np.dtype('float'))
        attribute_dataset.attrs.create('timeOffset', 0.0, None, dtype=np.dtype('float'))
        attribute_dataset.attrs.create('unitDimension',
                                       Elements.dict_dimensions.get(name), None, dtype=np.dtype('float'))
        print_warning_unknown_type(name, Block_types.arr, size)


class Block_types:
    """ Block types for each type in GDF file"""

    dir = 256  # Directory entry start
    edir = 512  # Directory entry end
    sval = 1024  # Single valued
    arr = 2048  # Array
    ascii_character = int('0001', 16)  # ASCII character
    signed_long = int('0002', 16)  # Signed long
    double_type = int('0003', 16)  # Double
    no_data = int('0010', 16)  # No data


class Constants:
    GDFID  = 94325877
    GDFNAMELEN = 16


def check_gdf_file(gdf_file):
    """Check that input file is correct GPT file
        Args:
           gdf_file - input file
        Returns:
            raise RuntimeWarning - if input file is not correct GPT file
        """
    gdf_id_check = struct.unpack('i', gdf_file.read(4))[0]
    if gdf_id_check != Constants.GDFID:
        raise RuntimeWarning('File directory is not a .gdf file')


def read_gdf_block_header(gdf_file):
    """ Function read block header of gdf file
        Args:
           gdf_file - input gpt file
        """
    name = gdf_file.read(16)
    primitive_type = ''
    namesplit = ''
    size = ''
    if len(name) < 15:
        return namesplit, primitive_type, size
    namesplit = name.split()[0]
    primitive_type = struct.unpack('i', gdf_file.read(4))[0]
    #size in bytes
    size = struct.unpack('i', gdf_file.read(4))[0]
    return namesplit, primitive_type, size


def get_block_type(primitive_type):
    """return type of current block
        Args:
          primitive_type - input type from GPT file

           """
    dir = int(primitive_type & Block_types.dir > 0)
    edir = int(primitive_type & Block_types.edir > 0)
    single_value = int(primitive_type & Block_types.sval > 0)
    arr = int(primitive_type & Block_types.arr > 0)
    return dir, edir, single_value, arr


def print_warning_unknown_type(name, primitive_type, size):
    """Print warning if type of GDF file are unknown"""

    print('unknown datatype of value')
    print('name=', name)
    print('type=', primitive_type)
    print('size=', size)


def decode_name(attribute_name):
    """ Decode name from binary """

    decoding_name = attribute_name.decode('ascii', errors='ignore')
    decoding_name = re.sub(r'\W+', '', decoding_name)
    return decoding_name


def read_array_type(gdf_file, dattype, particles_group, name, primitive_type, size):
    """Function read array type from GDF file
        Args:
           gdf_file - input file
           dattype - type of array block
           particles_group - group of particles in result openPMD file
           typee - type of block
           size - size of block
        """

    if dattype == Block_types.double_type:
        decoding_name = decode_name(name)
        name_to_group(decoding_name, particles_group, size, gdf_file)
    else:
        print_warning_unknown_type(name, primitive_type, size)


def add_time_attributes(iteration_number_group, last_iteration_time, decoding_name, value):
    """ Add time attributes to each iteration """

    iteration_number_group.attrs.create(decoding_name, value)
    iteration_number_group.attrs.create('timeUnitSI', 1E-3)
    dt = value - last_iteration_time
    iteration_number_group.attrs.create('dt', dt)
    last_iteration_time.__add__(value)


def read_single_value_type(gdf_file, data_type, primitive_type, size, name,
                           particles_group, subparticles_group, iteration_number_group, last_iteration_time):
    """Read single value from gdf file """

    time = 0
    var = 0
    if data_type == Block_types.no_data:
        pass
    elif data_type == Block_types.signed_long:
        value = struct.unpack('i', gdf_file.read(4))[0]
    elif data_type == Block_types.ascii_character:
        var, subparticles_group = \
            read_ascii_character(data_type, particles_group, subparticles_group, gdf_file, var, size, name)

    elif data_type == Block_types.double_type:
        time = read_double_value(name, gdf_file, iteration_number_group, last_iteration_time)
    else:
        print_warning_unknown_type(name, primitive_type, size)
    return var, subparticles_group, time


def create_iteration_sub_groups(iteration_number, data_group):
    """Function create subgroup according iteration
        Args:
         iteration_number - number of current iteration
         data_group - base group
        Returns:
          iteration_number_group - group for current iteration
          particles_group - group for particles in result openPMD file
          iteration_number - result number of iteration
        """

    iteration_number += 1
    iteration_number_group = data_group.create_group(str(iteration_number))
    particles_group = iteration_number_group.create_group('particles')

    return iteration_number_group, particles_group, iteration_number


def add_positionOffset_attributes(axis_positionOffset_group, shape):
    """Add default position offset group attributes"""

    axis_positionOffset_group.attrs.create('value', 0.0, None, dtype=np.dtype('float'))
    axis_positionOffset_group.attrs.create('unitSI', 1.0, None, dtype=np.dtype('float'))
    axis_positionOffset_group.attrs.create('shape', shape, None, dtype=np.dtype('uint'))


def add_empty_time(iteration_number_group):
    """Add default time attributes """

    iteration_number_group.attrs.create('time', 0.0)
    iteration_number_group.attrs.create('timeUnitSI', 1E-3)
    iteration_number_group.attrs.create('dt', 0.0)


def add_positionOffset(particles_group, size):
    """Add default position offset group """

    positionOffset_group = particles_group.require_group('positionOffset')
    positionOffset_group.attrs.create('unitDimension', (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), None,
                           dtype=np.dtype('float'))
    positionOffset_group.attrs.create('timeOffset', 0.0, None, dtype=np.dtype('float'))

    shape = [int(size / 8)]
    x_positionOffset_group = positionOffset_group.require_group('x')
    add_positionOffset_attributes(x_positionOffset_group, shape)
    y_positionOffset_group = positionOffset_group.require_group('y')
    add_positionOffset_attributes(y_positionOffset_group, shape)
    z_positionOffset_group = positionOffset_group.require_group('z')
    add_positionOffset_attributes(z_positionOffset_group, shape)


class Collect_moving_groups():
    def __init__(self, old_base_group):

        self.old_base_group = old_base_group
        self.group_for_moving =[]
        self.datasets_for_moving = []
        self.datasets_name = []

    def __call__(self, name, node):

        if isinstance(node, h5py.Group) and not node.name.endswith('electrons')\
                and not node.name.endswith('/x') and not node.name.endswith('/y') \
                and not node.name.endswith('/z'):
            self.group_for_moving.append(node)

        elif isinstance(node, h5py.Dataset) and node.parent == self.old_base_group:
            self.datasets_for_moving.append(node)
            self.datasets_name.append(node.name)


def check_name_particles_exist(group):

    base_particles_name = False
    for key in group.keys():
        if key == 'position' or key == 'momentum' or key == 'G':
            base_particles_name = True

    return base_particles_name


def move_dataset(base_group_moving, new_particles_group, indexes):

    dataset_names = []

    for dataset_name in base_group_moving.datasets_name:
        dataset_names.append(dataset_name)

    for i in range(0, len(base_group_moving.datasets_name)):
        name_dataset = base_group_moving.datasets_name[i]

        name_of_particles_idx = name_dataset.rfind("/")
        name_of_dataset = name_dataset[name_of_particles_idx + 1: len(name_dataset)]
        current_attrs = base_group_moving.datasets_for_moving[i].attrs
        current_dataset_value = base_group_moving.datasets_for_moving[i].value[indexes]
        new_particles_group.create_dataset(name_of_dataset, data=current_dataset_value)
        current_dataset = new_particles_group.require_dataset(name_of_dataset,
                                                              base_group_moving.datasets_for_moving[i].shape, dtype=dtype('f8'))
        for name, value in current_attrs.items():
            current_dataset.attrs.create(name, value)


def move_group(hdf_file, base_group_moving, new_particles_group, indexes):

    for group in base_group_moving.group_for_moving:
        datasets_reader = Datasets_functor()
        group.visititems(datasets_reader)
        hdf_file.copy(group, new_particles_group)

        for dataset in datasets_reader.datasets:
            type_of_dataset = str(dataset.dtype)

            current_dataset = new_particles_group.require_dataset(dataset.name, dataset.shape, dtype=type_of_dataset)
            current_dataset_name = dataset.name

            current_dataset_value = dataset.value
            del hdf_file[current_dataset.name]
            new_datset_values = current_dataset_value[indexes]
            new_particles_group.create_dataset(current_dataset_name, new_datset_values.shape, dtype=type_of_dataset)


def get_particles_idxes_by_types(mass_array, charge_array, mass_value, charge_value):

    indexes_array_mass = [i for i in range(len(mass_array)) if math.isclose(mass_array[i], mass_value, rel_tol=1e-5)]
    indexes_array_second = [i for i in range(len(charge_array)) if math.isclose(charge_array[i], charge_value, rel_tol=1e-5)]
    compare_values = [i for i, j in zip(indexes_array_mass, indexes_array_second) if i == j]

    return compare_values


def get_other_indexes(start_indexes, electrons_indexes, protons_indexes, positrons_indexes):

    electrons_indexes.sort()
    protons_indexes.sort()
    positrons_indexes.sort()
    categoriased_indexes = electrons_indexes + protons_indexes + positrons_indexes
    uncategoriased_indexes = list(set(start_indexes) - set(categoriased_indexes))

    return uncategoriased_indexes


def get_particle_types(mass_array, charge_array):

    particle_mass = [9.10953E-31, 1.672621898E-27, 9.10953E-31]
    particle_charge = [-1.60217662E-19, 1.60217662E-19,1.60217662E-19]

    start_indexes = list(range(0, len(mass_array)))

    electrons_indexes = get_particles_idxes_by_types(mass_array, charge_array, particle_mass[0], particle_charge[0])
    protons_indexes = get_particles_idxes_by_types(mass_array, charge_array, particle_mass[1], particle_charge[1])
    positrons_indexes = get_particles_idxes_by_types(mass_array, charge_array, particle_mass[2], particle_charge[2])

    uncategoriased_indexes = get_other_indexes(start_indexes, electrons_indexes, protons_indexes, positrons_indexes)

    return electrons_indexes, protons_indexes, positrons_indexes, uncategoriased_indexes


def delete_old_groups(hdf_file, base_group_moving):

    group_names = []

    for group in base_group_moving.group_for_moving:
        group_names.append(group.name)

    for name in group_names:
        del hdf_file[name]


def delete_old_datasets(hdf_file, base_group_moving):

    datasets_names = []

    for group in base_group_moving.datasets_for_moving:
        datasets_names.append(group.name)

    for name in datasets_names:
        del hdf_file[name]


def add_unit_SI_momentum(mass_spices, new_particles_group):

    collect_momentum = Momentum_base_group_functor()
    new_particles_group.visititems(collect_momentum)

    datasets_reader = Datasets_functor()
    collect_momentum.momentum[0].visititems(datasets_reader)

    c = 299792458
    for dataset in datasets_reader.datasets:
        dataset.attrs['unitSI'] = mass_spices * c


def create_particles_group_by_type(hdf_file, base_group_moving, group, elements_indexes, name_of_group):

    new_particles_group = group.create_group(name_of_group)

    move_group(hdf_file, base_group_moving, new_particles_group, elements_indexes)
    move_dataset(base_group_moving, new_particles_group, elements_indexes)
    return new_particles_group


def add_base_partilces_types(data_group, hdf_file):
    collect_particles = Particles_base_group_functor()
    data_group.visititems(collect_particles)
    first_group = collect_particles.particles_groups[0]
    collect_particle_type = Particle_types_elements_functor()
    first_group.visititems(collect_particle_type)

    electrons_indexes, protons_indexes, positrons_indexes, uncategorised_indexes =\
        get_particle_types(collect_particle_type.mass, collect_particle_type.charge)

    if check_name_particles_exist(first_group):
        base_group_moving = Collect_moving_groups(first_group)
        first_group.visititems(base_group_moving)

        if len(electrons_indexes) != 0:
            particle_type_group = create_particles_group_by_type(hdf_file, base_group_moving, first_group, electrons_indexes, 'electrons')
            mass_spices = collect_particle_type.mass[electrons_indexes[0]]
            add_unit_SI_momentum(mass_spices, particle_type_group)

        if len(protons_indexes) != 0:
            particle_type_group = create_particles_group_by_type(hdf_file, base_group_moving, first_group, protons_indexes, 'protons')
            mass_spices = collect_particle_type.mass[protons_indexes[0]]
            add_unit_SI_momentum(mass_spices, particle_type_group)

        if len(positrons_indexes) != 0:
            particle_type_group = create_particles_group_by_type(hdf_file, base_group_moving, first_group, positrons_indexes, 'positrons')
            mass_spices = collect_particle_type.mass[positrons_indexes[0]]
            add_unit_SI_momentum(mass_spices, particle_type_group)

        if len(uncategorised_indexes) != 0:
            particle_type_group = create_particles_group_by_type(hdf_file, base_group_moving, first_group, uncategorised_indexes, 'uncategorised')
            mass_spices = collect_particle_type.mass[uncategorised_indexes[0]]
            add_unit_SI_momentum(mass_spices, particle_type_group)

        delete_old_groups(hdf_file, base_group_moving)
        delete_old_datasets(hdf_file, base_group_moving)


def gdf_file_to_hdf_file(gdf_file, hdf_file):

    check_gdf_file(gdf_file)
    add_root_attributes(hdf_file, gdf_file, Constants.GDFNAMELEN)

    gdf_file.seek(2, 1)

    iteration_number = -1
    data_group = hdf_file.create_group('data')

    iteration_number_group, particles_group, iteration_number\
        = create_iteration_sub_groups(iteration_number, data_group)
    last_iteration_time = 0
    last_arr = False
    subparticles_group = particles_group
    last_time = False
    last_iteration = False

    while True:
        if gdf_file.read(1) == '':
            break
        gdf_file.seek(-1, 1)
        name, primitive_type, size = read_gdf_block_header(gdf_file)
        print(name)

        if size == '':
            break
        dir, edir, sval, arr = get_block_type(primitive_type)
        data_type = primitive_type & 255

        var = 0
        time = 0

        if sval:
            var, subparticles_group, time = read_single_value_type(gdf_file, data_type, primitive_type, size, name,
                                   particles_group, subparticles_group, iteration_number_group, last_iteration_time)

        if time and not last_iteration:

            iteration_number, particles_group, subparticles_group =\
                create_time_subroup(iteration_number, data_group, particles_group, subparticles_group)

        if last_arr and not arr and (not var and not last_time):
            iteration_number_group, subparticles_group, iteration_number \
                = create_iteration_sub_groups(iteration_number, data_group)

        if arr:

            read_array_type(gdf_file, data_type, subparticles_group, name, primitive_type, size)
            add_positionOffset(subparticles_group, size)

        last_time = time
        last_iteration = last_arr and not arr and (not var and not time)
        last_arr = arr

    if subparticles_group != None:
        if subparticles_group.keys().__len__() == 0:
            data_group.__delitem__(str(iteration_number_group.name))

    if iteration_number_group.attrs.get('time') == None:
        add_empty_time(iteration_number_group)

    add_base_partilces_types(data_group, hdf_file)


def create_time_subroup(iteration_number, data_group, particles_group, subparticles_group):
    """Create new iteration if find new time  """

    if iteration_number != 0:
        iteration_number_group = data_group.create_group(str(iteration_number))
        particles_group = iteration_number_group.create_group('particles')
        subparticles_group = particles_group
    iteration_number += 1

    return iteration_number, particles_group, particles_group


def read_ascii_character(data_type, particles_group, subparticles_group, gdf_file, var, size, name):
    """Read ascii characters from gdf file """

    if data_type == Block_types.ascii_character:
        value = gdf_file.read(size)
        decoding_value = decode_name(value)
        decoding_name = decode_name(name)
        if (decoding_name == 'var'):
            particles_name = decoding_value
            var = 1
            subparticles_group = particles_group.require_group(particles_name)

    return var, subparticles_group


def read_double_value(name, gdf_file, iteration_number_group, last_iteration_time):
    """Read double from gdf file """

    time = 0
    value = struct.unpack('d', gdf_file.read(8))[0]
    decoding_name = decode_name(name)
    if decoding_name == 'time':
        add_time_attributes(iteration_number_group, last_iteration_time, decoding_name, value)
        time = 1
    return time


def gdf_to_hdf(gdf_file_directory, hdf_file_directory):
    """find GDF file in gdf_file_directory,
       and convert to hdf file openPMD,
       write to hdf_file_directory
        Args:
         gdf_file_directory - path to GDF file
         hdf_file_directory - path where the hdf  file is created
        """

    print('Converting .gdf to .hdf file')
    if os.path.exists(hdf_file_directory):
        os.remove(hdf_file_directory)
    hdf_file = h5py.File(hdf_file_directory, 'a')
    with open(gdf_file_directory, 'rb') as gdf_file:
        gdf_file_to_hdf_file(gdf_file, hdf_file)

    gdf_file.close()
    hdf_file.close()
    print('Converting .gdf to .hdf file... Complete.')


def files_from_args(file_names):
    gdf_file = ''
    hdf_file = ''
    if len(file_names) >= 2:
        gdf_file = file_names[1]
    if len(file_names) >= 3:
        hdf_file = file_names[2]
    return gdf_file, hdf_file


def converter(gdf_file, hdf_file):
    if gdf_file != '':
        if os.path.exists(gdf_file):
            if hdf_file == '':
                hdf_file = gdf_file[:-4] + '.hdf'
                print('Destination .hdf directory not specified. Defaulting to ' + hdf_file)

            gdf_to_hdf(gdf_file, hdf_file)
        else:
            print('The .gdf file does not exist to convert to .hdf')


def main(file_names):
    gdf_path, hdf_path = files_from_args(file_names)
    converter(gdf_path, hdf_path)


if __name__ == "__main__":
    file_names = sys.argv
    main(file_names)
