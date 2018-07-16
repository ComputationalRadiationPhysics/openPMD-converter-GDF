"""Converter from GPT format to openPMD"""


from __future__ import division
from pylab import *
import h5py
import struct
import os
import sys
import datetime
import re


def add_creator_name(gdf_file, hdf_file, size_gdf_name):
    """ Add name of creator to root structure"""

    creator = list(gdf_file.read(size_gdf_name))
    new_creator = []
    for element in creator:
        new_creator.append(element)
    creator_name = []
    for element in new_creator:
        if element is 0:
            break
        else:
            creator_name.append(chr(element))
    hdf_file.attrs['software'] = ''.join(creator_name)


def add_dest_name(gdf_file, hdf_file, size_gdf_name):
    """Add destination name to root directory """

    destination = []
    for element in new_dest:
        if element is 0:
            break
        else:
            destination.append(chr(element))
    hdf_file.attrs['destination'] = ''.join(destination)


def add_creation_time(gdf_file, hdf_file):
    """Add when the gdf file file was created to root directory
    of openPMD file.
    We use next time and data format: YYYY-MM-DD HH:mm:ss tz
        """
    time_created = struct.unpack('i', gdf_file.read(4))[0]
    format_time = datetime.datetime.fromtimestamp(time_created)
    format_time = format_time.strftime("%Y-%m-%d %H:%M:%S %Z")
    hdf_file.attrs['date'] = format_time


    add_creation_time(gdf_file, hdf_file)
    add_creator_name(gdf_file, hdf_file, GDFNAMELEN)
    add_dest_name(gdf_file, hdf_file, GDFNAMELEN)

    # get other metadata about the GDF file
    major = struct.unpack('B', gdf_file.read(1))[0]
    minor = struct.unpack('B', gdf_file.read(1))[0]
    hdf_file.attrs['gdf_version'] = str(major) + '.' + str(minor)

    major = struct.unpack('B', gdf_file.read(1))[0]
    minor = struct.unpack('B', gdf_file.read(1))[0]
    hdf_file.attrs['software version'] = str(major) + '.' + str(minor)

    major = struct.unpack('B', gdf_file.read(1))[0]
    minor = struct.unpack('B', gdf_file.read(1))[0]
    hdf_file.attrs['destination_version'] = str(major) + '.' + str(minor)
    hdf_file.attrs['iterationEncoding'] = 'groupBased'
    hdf_file.attrs['iterationFormat'] = 'test_hierical_%T.h5'
    hdf_file.attrs['particlesPath'] = 'particles/'
    hdf_file.attrs['openPMD'] = '1.1.0'
    hdf_file.attrs['openPMDextension'] = '1'
    hdf_file.attrs['basePath'] = '/data/%T/'


def add_root_attributes(hdf_file, gdf_file, size_gdf_name):
    """Add root attributes to result hdf_file
    Attributes:
        gdf_version, software version, destination_version, iterationEncoding,
        iterationFormat, particlesPath openPMD version, openPMDextension,
        base path
       """


def find_one_symbol_attribute(name):
    dict_one_symbol = {'x': ['position', 'x'], 'y': ['position', 'y'], 'z': ['position', 'z'],
                       'G': ['none', 'G'], 'q': ['charge', 'charge'], 'm': ['mass', 'mass']}
    return dict_one_symbol.get(name[0])


def find_two_symbols_attribute(name):
    dict_two_symbols = {'Bx': ['B', 'x'], 'By': ['B', 'y'], 'Bz': ['B', 'z']}
    if len(name) < 2:
        return None
    current_name = name[0:1]
    return dict_two_symbols.get(current_name)


def find_three_symbols_attribute(name):
    dict_three_symbols = {'fBx': ['fB', 'x'], 'fBy': ['fB', 'y'], 'fBz': ['fB', 'z'],
                          'fEx': ['fE', 'x'], 'fEy': ['fE', 'y'], 'fEz': ['fE', 'z'],
                          'rxy': ['none', 'rxy']}
    if len(name) < 3:
        return None
    current_name = name[0:2]
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


    dict_particles = {'IDC': ['ID', 'none']}

    dict_demantions = {'position': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
                       'mass': (0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0),
                       'B': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),
                       'G': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),
                       'charge': (0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0),
                       'fE': (1.0, 1.0, -3.0, -1.0, 0.0, 0.0, 0.0),
                       'fB': (0.0, 1.0, -2.0, -1.0, 0.0, 0.0, 0.0),
                       'std': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
                       'stdB': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),
                       'stdt': (0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0),
                       'rmacro': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
                       'nmacro': (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
                       'avg': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
                       'avgB': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),
                       'avgr': (1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
                       'avgG': (1.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0),
                       'avgt': (0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0),
                       'avgFE': (1.0, 1.0, -3.0, -1.0, 0.0, 0.0, 0.0),
                       'avgFB': (0.0, 1.0, -2.0, -1.0, 0.0, 0.0, 0.0)}


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
            value = fromfile(gdf_file, dtype=dtype('f8'), count=int(size / 8))
            particles.create_dataset(name_atribute[1], data=value)
            particles.require_dataset(name_atribute[1], value.shape, dtype=dtype('f8')).attrs['timeOffset'] = '0.0'
            particles.require_dataset(name_atribute[1], value.shape, dtype=dtype('f8')).attrs['unitDimension']\
                = str(dict_demantions.get(name_atribute[1]))
        else:
            sub_group = particles.require_group(name_atribute[0])
            sub_group.attrs['unitDimension'] = str(dict_demantions.get(name_atribute[0]))
            sub_group.attrs['timeOffset'] = '0.0'
            value = fromfile(gdf_file, dtype=dtype('f8'), count=int(size / 8))
            sub_group.create_dataset(name_atribute[1], data=value)
    elif dict_particles.get(name) != None:
        if dict_particles.get(name)[0] == 'ID':
            value = fromfile(gdf_file, dtype=dtype('f8'), count=int(size / 8))
            particles.create_dataset('id', data=value, dtype=dtype('int'))
    else:
        value = fromfile(gdf_file, dtype=dtype('f8'), count=int(size / 8))


class Block_types:
    dir = 256  # Directory entry start
    edir = 512  # Directory entry end
    sval = 1024  # Single valued
    arr = 2048  # Array
    t_ascii = int('0001', 16)  # ASCII character
    t_s32 = int('0002', 16)  # Signed long
    t_dbl = int('0003', 16)  # Double
    t_null = int('0010', 16)  # No data


class Constants:
    GDFID  = 94325877
    GDFNAMELEN = 16


def check_gdf_file(gdf_file):
    """Fuction check that input file is correct GPT file
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


def get_block_type(typee, block_types):
    """Function return type of curent block
        Args:
          typee - input type from GPT file
          block_types - all types block in GDF file
           """

    dir = int(typee & block_types.dir > 0)
    edir = int(typee & block_types.edir > 0)
    sval = int(typee & block_types.sval > 0)
    arr = int(typee & block_types.arr > 0)
    return dir, edir, sval, arr


def print_warning_unknown_type(gdf_file, name, typee, size):
    """Function print warning if type of GDF file are unknown
        Args:
           gdf_file - input file
           name  - name of block
           typee - type of block
           size - size of block
        """
    print('unknown datatype of value!!!')
    print('name=', name)
    print('type=', typee)
    print('size=', size)
    value = gdf_file.read(size)
    print('value=' + value)


def read_array_type(gdf_file, dattype, particles_group, name, typee, size):
    """Function read array type from GDF file
        Args:
           gdf_file - input file
           dattype - type of array block
           particles_group - group of particles in result openPMD file
           typee - type of block
           size - size of block
        """
    if dattype == Block_types.t_dbl:
        decode_name = name.decode('ascii', errors='ignore')
        correct_name = re.sub(r'\W+', '', decode_name)
        name_to_group(correct_name, particles_group, size, gdf_file)
    else:
        print_warning_unknown_type(gdf_file, name, typee, size)


def read_single_value_type(gdf_file, data_type, iteration_number_group, primitive_type, block_types, size, name,
                           last_iteration_time):
    if data_type == block_types.t_dbl:
        value = struct.unpack('d', gdf_file.read(8))[0]
        decode_name = name.decode('ascii', errors='ignore')
        correct_name = re.sub(r'\W+', '', decode_name)
        if correct_name == 'time':
            iteration_number_group.attrs[correct_name] = value
            iteration_number_group.attrs['timeUnitSI'] = '1E-3'
            iteration_number_group.attrs['dt'] = str(value - last_iteration_time)
            last_iteration_time.__add__(value)
    elif data_type == Block_types.t_null:
        pass
    elif data_type == Block_types.t_ascii:
        value = str(gdf_file.read(size))
        value = value.strip(' \t\r\n\0')
    elif data_type == Block_types.t_s32:
        value = struct.unpack('i', gdf_file.read(4))[0]
    else:
        print_warning_unknown_type(gdf_file, name, primitive_type, size)


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
    return iteration_number_group, particles_group, iteration_number


def gdf_file_to_hdf_file(gdf_file, hdf_file):

    block_types = Block_types()
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
        dir, edir, sval, arr = get_block_type(primitive_type, block_types)
        data_type = primitive_type & 255
        
        if lastarr and not arr:
            iteration_number_group, particles_group, iteration_number\
                = create_iteration_sub_groups(iteration_number, data_group)
        if sval:
            read_single_value_type(gdf_file, data_type,
                                   iteration_number_group, primitive_type, block_types, size, name, last_iteration_time)
        if arr:
            read_array_type(gdf_file, data_type, particles_group, name, primitive_type, size)

        lastarr = arr;


def gdf_to_hdf(gdf_file_directory, hdf_file_directory):
    """Function find GDF file in gdf_file_directory,
       and convert to hdf file openPMD standart,
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


def run_converter_according_files(gdf_file, hdf_file):
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
    gdf_file, hdf_file = files_from_args(file_names)
    run_converter_according_files(gdf_file, hdf_file)


if __name__ == "__main__":
    file_names = sys.argv
    main(file_names)
