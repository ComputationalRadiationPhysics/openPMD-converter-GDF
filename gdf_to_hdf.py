"""Converter from GPT format to openPMD"""


from __future__ import division
from pylab import *
import h5py
import struct
import os
import sys
import datetime
import re


def parse_name_array(gdf_file, size_gdf_name):
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
    hdf_file.attrs.create('openPMD', format_time, None, dtype='<S11')


def add_gdf_version(gdf_file, hdf_file):
    """Add gdf version to root directory """
    major = struct.unpack('B', gdf_file.read(1))[0]
    minor = struct.unpack('B', gdf_file.read(1))[0]
    hdf_file.attrs.create('gdf_version', str(major) + '.' + str(minor), None, dtype='<S8')


def add_software_version(gdf_file, hdf_file):
    """Add software version to root directory """
    major = struct.unpack('B', gdf_file.read(1))[0]
    minor = struct.unpack('B', gdf_file.read(1))[0]
    hdf_file.attrs.create('software version', str(major) + '.' + str(minor), None, dtype='<S8')


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
    dict_two_symbols = {'Bx': ['momentum', 'x'], 'By': ['momentum', 'y'], 'Bz': ['momentum', 'z']}
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
    current_name = name[0:4]
    return dict_three_symbols.get(current_name)


def find_multiple_symbols_attribute(name):
    dict_three_symbols = {'stdx': ['std', 'x'], 'stdy': ['std', 'y'], 'stdz': ['std', 'z'],
                          'avgx': ['avg', 'x'], 'avgy': ['avg', 'y'], 'avgz': ['avg', 'z'],
                          'avgBx': ['avgB', 'x'], 'avgBy': ['avgB', 'y'], 'avgBz': ['avgB', 'z'],
                          'avgFEx': ['avgFE', 'x'], 'avgFEy': ['avgFE', 'y'], 'avgFEz': ['avgFE', 'z'],
                          'avgFBx': ['avgFB', 'x'], 'avgFBy': ['avgFB', 'y'], 'avgFBz': ['avgFB', 'z'],
                          'avgr': ['none', 'avgr'], 'avgG': ['none', 'avgG'],
                          'stdt': ['none', 'stdt'], 'stdG': ['none', 'stdG'],
                          'stdBx': ['stdB', 'x'], 'stdBy': ['stdB', 'y'], 'stdBz': ['stdB', 'z'],
                          'rmacro': ['none', 'rmacro'], 'nmacro': ['none', 'nmacro'], 'avgt': ['none', 'avgt']}
    return dict_three_symbols.get(name)


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
                       'rmacro': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),     # length = 1
                       'nmacro': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),     # length = 1
                       'avg': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),        # length = 1
                       'avgB': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),      # length = 1, time = -1
                       'avgr': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),       # length = 1
                       'avgG': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),      # length = -1, time = 1
                       'avgt': (0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0),       # time = 1
                       'avgFE': (1.0, 1.0, -3.0, -1.0, 0.0, 0.0, 0.0),    # length = 1, mass = 1,  time = -3, electric current = -1
                       'avgFB': (0.0, 1.0, -2.0, -1.0, 0.0, 0.0, 0.0)}    # mass  = 1, time = -2, electric current = -1

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
            size - size of dataset in gdf_file
            gdf_file - input file GPT
           """

    if find_attribute(name) != None:
        name_atribute = find_attribute(name)
        if name_atribute[0] == 'none':
            add_dataset_attributes(gdf_file, particles, name_atribute, size)
        else:
            add_group_attributes(gdf_file, particles, name_atribute, size)
    elif name[0:2] == 'ID':
            value = fromfile(gdf_file, dtype=dtype('f8'), count=int(size / 8))
            particles.create_dataset('ID', data=value)
    else:
        value = fromfile(gdf_file, dtype=dtype('f8'), count=int(size / 8))


class Block_types:
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
        return namesplit, primitive_type, size;
    namesplit = name.split()[0]

    primitive_type = struct.unpack('i', gdf_file.read(4))[0]
    size = struct.unpack('i', gdf_file.read(4))[0]
    return namesplit, primitive_type, size


def get_block_type(primitive_type):
    """Function return type of curent block
        Args:
          typee - input type from GPT file

           """
    dir = int(primitive_type & Block_types.dir > 0)
    edir = int(primitive_type & Block_types.edir > 0)
    single_value = int(primitive_type & Block_types.sval > 0)
    arr = int(primitive_type & Block_types.arr > 0)
    return dir, edir, single_value, arr


def print_warning_unknown_type(gdf_file, name, primitive_type, size):
    """Print warning if type of GDF file are unknown
        Args:
           gdf_file - input file
           name  - name of block
           typee - type of block
           size - size of block
        """
    print('unknown datatype of value')
    print('name=', name)
    print('type=', primitive_type)
    print('size=', size)
    value = gdf_file.read(size)
    print('value=' + value)


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
        print_warning_unknown_type(gdf_file, name, primitive_type, size)


def add_time_attributes(iteration_number_group, last_iteration_time, decoding_name, value):
    """ Add time attributes to each iteration """

    iteration_number_group.attrs.create(decoding_name, value)
    iteration_number_group.attrs.create('timeUnitSI', 1E-3)
    dt = value - last_iteration_time
    iteration_number_group.attrs.create('dt', dt)
    last_iteration_time.__add__(value)


def read_single_value_type(gdf_file, data_type, iteration_number_group, primitive_type, size, name,
                           last_iteration_time):
    """Read single value from gdf file """

    if data_type == Block_types.double_type:
        value = struct.unpack('d', gdf_file.read(8))[0]
        decoding_name = decode_name(name)
        if decoding_name == 'time':
            add_time_attributes(iteration_number_group, last_iteration_time, decoding_name, value)
    elif data_type == Block_types.no_data:
        pass
    elif data_type == Block_types.ascii_character:
        value = str(gdf_file.read(size))
        value = value.strip(' \t\r\n\0')
    elif data_type == Block_types.signed_long:
        value = struct.unpack('i', gdf_file.read(4))[0]
    else:
        print_warning_unknown_type(gdf_file, name, primitive_type, size)


def add_electrons_attribute(electorns_group):
    electorns_group.attrs.create('particleShape', 0.0, None, dtype=np.dtype('float'))
    electorns_group.attrs.create('particleSmoothing', 'none', None, dtype=np.dtype('<S4'))
    electorns_group.attrs.create('particlePush', 'Vay', None, dtype=np.dtype('<S3'))
    electorns_group.attrs.create('currentDeposition', 'Esirkepov', None, dtype=np.dtype('<S9'))
    electorns_group.attrs.create('particleInterpolation', 'momentumConserving', None, dtype=np.dtype('<S18'))


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
    print(iteration_number)
    iteration_number_group = data_group.create_group(str(iteration_number))
    particles_group = iteration_number_group.create_group('particles')
    electorns_group = particles_group.create_group('electorns')
    add_electrons_attribute(electorns_group)

    return iteration_number_group, electorns_group, iteration_number

def add_positionOffset_attributes(axis_positionOffset_group, shape):
    axis_positionOffset_group.attrs.create('value', 0.0, None, dtype=np.dtype('float'))
    axis_positionOffset_group.attrs.create('unitSI', 1.0, None, dtype=np.dtype('float'))
    axis_positionOffset_group.attrs.create('shape', shape, None, dtype=np.dtype('uint'))


def add_positionOffset(particles_group, size):
    """Add position offset group """

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

def gdf_file_to_hdf_file(gdf_file, hdf_file):

    check_gdf_file(gdf_file)
    add_root_attributes(hdf_file, gdf_file, Constants.GDFNAMELEN)

    gdf_file.seek(2, 1)  # skip to next block

    iteration_number = 0
    data_group = hdf_file.create_group('data')

    iteration_number_group, particles_group, iteration_number\
        = create_iteration_sub_groups(iteration_number, data_group)
    last_iteration_time = 0
    lastarr = False

    while True:
        if gdf_file.read(1) == '':
            break
        gdf_file.seek(-1, 1)
        name, primitive_type, size = read_gdf_block_header(gdf_file)
        if size == '':
            break
        dir, edir, sval, arr = get_block_type(primitive_type)
        data_type = primitive_type & 255

        if lastarr and not arr:
            iteration_number_group, particles_group, iteration_number \
                = create_iteration_sub_groups(iteration_number, data_group)
        if sval:
            read_single_value_type(gdf_file, data_type,
                                   iteration_number_group, primitive_type, size, name, last_iteration_time)
        if arr:
            read_array_type(gdf_file, data_type, particles_group, name, primitive_type, size)
            add_positionOffset(particles_group, size)

        lastarr = arr
    if particles_group.keys().__len__() == 0:
        data_group.__delitem__(str(iteration_number_group.name))


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
    for arg in file_names:
        if arg[-4:] == '.gdf':
            gdf_file = arg
        elif arg[-4:] == '.hdf':
            hdf_file = arg
    return gdf_file, hdf_file


def converter(gdf_file, hdf_file):
    if gdf_file != '':
        if os.path.exists(gdf_file):
            if hdf_file == '':
                hdf_file = gdf_file[:-4] + '.hdf'
                print('Destination .hdf directory not specified. Defaulting to ' + hdf_file)
            else:
                hdf_file = hdf_file[:-4] + '.hdf'
            gdf_to_hdf(gdf_file, hdf_file)
        else:
            print('The .gdf file does not exist to convert to .hdf')


def main(file_names):
    gdf_path, hdf_path = files_from_args(file_names)
    converter(gdf_path, hdf_path)


if __name__ == "__main__":
    file_names = sys.argv
    main(file_names)
