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
from openpmd_api import Series, Access_Type, Dataset, Mesh_Record_Component, \
    Unit_Dimension



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


def add_creator_name(gdf_file, series, size_gdf_name):
    """ Add name of creator to root structure"""

    software_name = parse_name_array(gdf_file, size_gdf_name)
    series.set_software(software_name)


def add_dest_name(gdf_file, series, size_gdf_name):
    """Add destination name to root directory """

    destination_name = parse_name_array(gdf_file, size_gdf_name)
    series.set_attribute('destination', destination_name)


def add_creation_time(gdf_file, series):
    """Add when the gdf file file was created to root directory
    of openPMD file.
    We use next time and data format: YYYY-MM-DD HH:mm:ss tz
        """
    time_created = struct.unpack('i', gdf_file.read(4))[0]
    format_time = datetime.datetime.fromtimestamp(time_created)
    format_time = format_time.strftime("%Y-%m-%d %H:%M:%S %Z")
    series.set_date(format_time)


def add_gdf_version(gdf_file, series):
    """Add gdf version to root directory """

    major = struct.unpack('B', gdf_file.read(1))[0]
    minor = struct.unpack('B', gdf_file.read(1))[0]
    result_version = str(major) + '.' + str(minor)
    series.set_attribute('gdf_version', result_version)


def add_software_version(gdf_file, series):
    """Add software version to root directory """

    major = struct.unpack('B', gdf_file.read(1))[0]
    minor = struct.unpack('B', gdf_file.read(1))[0]
    result_version = str(major) + '.' + str(minor)
    series.set_software_version(result_version)


def add_destination_version(gdf_file, series):
    """Add destination version to root directory """

    major = struct.unpack('B', gdf_file.read(1))[0]
    minor = struct.unpack('B', gdf_file.read(1))[0]
    result_destination = str(major) + '.' + str(minor)
    series.set_attribute('gdf_version', result_destination)


def add_root_attributes(series, gdf_file, size_gdf_name):
    """Add root attributes to result hdf_file
    Attributes:
        gdf_version, software version, destination_version, iterationEncoding,
        iterationFormat, particlesPath openPMD version, openPMDextension,
        base path
       """

    add_creation_time(gdf_file, series)
    add_creator_name(gdf_file, series, size_gdf_name)
    add_dest_name(gdf_file, series, size_gdf_name)
    add_gdf_version(gdf_file, series)
    add_software_version(gdf_file, series)
    add_destination_version(gdf_file, series)

    series.set_iteration_encoding(openpmd_api.Iteration_Encoding.group_based)
    series.set_iteration_format('test_hierical_%T.h5')
    series.set_particles_path('particles/')
    series.set_openPMD('1.1.2')
    series.set_base_path('/data/%T/')
    series.set_openPMD_extension(0)


def find_one_symbol_attribute(name):
    dict_one_symbol = {'x': ['position', 'x'], 'y': ['position', 'y'], 'z': ['position', 'z'],
                       'G': ['G', 'G'], 'q': ['charge', 'charge'], 'm': ['mass', 'mass']}
    return dict_one_symbol.get(name[0])


def find_two_symbols_attribute(name):
    dict_two_symbols = {'Bx': ['momentum', 'x'], 'By': ['momentum', 'y'], 'Bz': ['momentum', 'z'],
                        'ID': ['none', 'id']}
    if len(name) < 2:
        return None
    current_name = name[0:2]
    return dict_two_symbols.get(current_name)


def find_three_symbols_attribute(name):
    dict_three_symbols = {'fBx': ['B', 'x'], 'fBy': ['B', 'y'], 'fBz': ['B', 'z'],
                          'fEx': ['E', 'x'], 'fEy': ['E', 'y'], 'fEz': ['E', 'z'],
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

    dict_dimensions = {'position': {Unit_Dimension.L: 1},
                       'mass': {Unit_Dimension.M: 1},
                       'momentum': {Unit_Dimension.L: 1, Unit_Dimension.M: 1, Unit_Dimension.T: 1},
                       'G': {Unit_Dimension.L: 0},
                       'charge': {Unit_Dimension.T: 1, Unit_Dimension.I: 1},
                       'fE': {Unit_Dimension.L: 1, Unit_Dimension.M: 1, Unit_Dimension.T: -3, Unit_Dimension.theta: -1},
                       'fB': {Unit_Dimension.M: 1, Unit_Dimension.T: -2, Unit_Dimension.theta: -1},
                       'std': {Unit_Dimension.L: 1},
                       'stdB': {Unit_Dimension.L: 1, Unit_Dimension.T: 1},
                       'stdt': {Unit_Dimension.T: 1},
                       'stdG': {Unit_Dimension.L: 0},
                       'time': {Unit_Dimension.T: 1},
                       'rmacro': {Unit_Dimension.L: 1},
                       'nmacro': {Unit_Dimension.L: 1},
                       'avg': {Unit_Dimension.L: 1},
                       'avgB': {Unit_Dimension.L: 1, Unit_Dimension.T: 1},
                       'avgr': {Unit_Dimension.L: 1},
                       'avgG': {Unit_Dimension.L: 0},
                       'avgt': {Unit_Dimension.T: 1},
                       'avgFE': {Unit_Dimension.L: 1, Unit_Dimension.M: 1, Unit_Dimension.T: -3, Unit_Dimension.theta: -1},
                       'avgFB': {Unit_Dimension.M: 1, Unit_Dimension.T: -2, Unit_Dimension.theta: -1},
                       'rxy': {Unit_Dimension.L: 1},
                       'id': {Unit_Dimension.L: 0},
                       'avgzrms': {Unit_Dimension.L: 1, Unit_Dimension.T: -1},
                       'avgzyms': {Unit_Dimension.L: 1, Unit_Dimension.T: -1},
                       'nemiyrms': {Unit_Dimension.L: 1, Unit_Dimension.T: -1},
                       'nemizrms': {Unit_Dimension.L: 1, Unit_Dimension.T: -1},
                       'nemixrms': {Unit_Dimension.L: 1, Unit_Dimension.T: -1}}


    dict_weightingPower = {'position': 0., 'mass': 1., 'charge': 1., 'momentum': 1.,
                           'G': 1., 'rmacro': 0., 'nmacro': 0., 'fE': 1., 'fB': 1.}

    dict_macroWeighted = {'position': 0, 'mass': 1, 'charge': 1, 'momentum': 1,
                           'G': 1, 'rmacro': 0, 'nmacro': 0, 'fE': 1, 'fB': 1}


def add_weightingPower_attribute(name_atribute, record):
    if Elements.dict_weightingPower.get(name_atribute[1]) != None:
        record.set_attribute('weightingPower', Elements.dict_weightingPower.get(name_atribute[1]))
    else:
        record.set_attribute('weightingPower', 0)


def add_macroWeighted_attribute(name_atribute, record):
    if Elements.dict_macroWeighted.get(name_atribute[1]) != None:
        record.set_attribute('macroWeighted', Elements.dict_macroWeighted.get(name_atribute[1]))
    else:
        record.set_attribute('macroWeighted', 0)


def is_record_value(name):

    if find_attribute(name) != None:
        name_atribute = find_attribute(name)
        if name_atribute[0] != 'none':
            return True
    return False


def is_field_value(name):

    name_array = find_attribute(name)
    if name_array == None:
        return False

    fields_values = ['B', 'E']

    return name_array[0] in fields_values


def is_particles_value(name):

    name_array = find_attribute(name)
    if name_array == None:
        return False

    particles_values = ['position', 'G', 'charge', 'mass', 'id', 'momentum']

    return name_array[0] in particles_values


def add_spices_values(name, dataset_format, values, current_spicies, series):

    name_atribute = find_attribute(name)
    dataset_address = current_spicies[name_atribute[0]][name_atribute[1]]
    record_component = current_spicies[name_atribute[0]]
    record_component.set_unit_dimension(Elements.dict_dimensions.get(name_atribute[0]))
    add_weightingPower_attribute(name_atribute, record_component)
    add_macroWeighted_attribute(name_atribute, record_component)
    record_component.set_time_offset(0.0)
    dataset_address.reset_dataset(dataset_format)
    dataset_address[()] = values
    series.flush()


def add_field_values(name, dataset_format, values, current_fields, series):
    name_atribute = find_attribute(name)
    record_component = current_fields[name_atribute[0]]
    record_component.set_time_offset(0.0)
    dataset_address = current_fields[name_atribute[0]][name_atribute[1]]
    dataset_address.reset_dataset(dataset_format)
    dataset_address[()] = values
    series.flush()


def name_to_group(series, name, size, gdf_file, current_spicies, current_fields):
    """Add dataset to correct group in particles group
        Args:
            particles - particles group
            name - name of dataset in gdf_file
            size - size of dataset in gdf_file, in bytes
            gdf_file - input file GPT

           """
    values = fromfile(gdf_file, dtype=dtype('f8'), count=int(size / 8))

    dataset_format = Dataset(values.dtype, [int(size / 8)])

    if is_field_value(name):
        add_field_values(name, dataset_format, values, current_fields, series)

    if is_particles_value(name):
        add_spices_values(name, dataset_format, values, current_spicies, series)

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


def read_array_type(series, gdf_file, dattype, name, primitive_type, size, current_spicies, current_fields):
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
        name_to_group(series, decoding_name, size, gdf_file, current_spicies, current_fields)
    else:
        print_warning_unknown_type(name, primitive_type, size)


def add_time_attributes(iteration_number_group, last_iteration_time, decoding_name, value):
    """ Add time attributes to each iteration """

    iteration_number_group.attrs.create(decoding_name, value)
    iteration_number_group.attrs.create('timeUnitSI', 1E-3)
    dt = value - last_iteration_time
    iteration_number_group.attrs.create('dt', dt)
    return value


def read_single_value_type(gdf_file, data_type, primitive_type, size, name, series, last_iteration_time):
    """Read single value from gdf file """

    time = 0
    is_ascii_name = False
    if data_type == Block_types.no_data:
        pass
    elif data_type == Block_types.signed_long:
        value = struct.unpack('i', gdf_file.read(4))[0]
    elif data_type == Block_types.ascii_character:
        is_ascii_name = read_ascii_character(data_type, gdf_file, size, name)

    elif data_type == Block_types.double_type:
        time = read_double_value(name, gdf_file, series, last_iteration_time)
    else:
        print_warning_unknown_type(name, primitive_type, size)

    return is_ascii_name,  time


def create_iteration_sub_groups(iteration_number, series):
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
    first_iteration = series.iterations[iteration_number] \
        .set_time(42.0) \
        .set_dt(1.0) \
        .set_time_unit_SI(1.39e-16)

    return first_iteration, iteration_number


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


def need_new_iteration_group(is_last_data_array, is_current_data_array, is_current_data_particles_name,
                             is_first_iteration, data_type) :
    if is_first_iteration:
        return is_first_iteration
    return is_last_data_array and not is_current_data_array and not is_current_data_particles_name \
           and data_type != Block_types.no_data


def is_spicies_group_needed(current_iteration):

    if len(current_iteration.particles) == 0:
        return True
    else:
        return False


def is_fields_group_needed(current_iteration):

    if len(current_iteration.meshes) == 0:
        return True
    else:
        return False


def create_new_spices_group(current_iteration):

    spicies = current_iteration.particles["spicies"]
    spicies.set_attribute('particleShape', 3.0)
    spicies.set_attribute('particleSmoothing', 'none')

    return spicies


def create_new_fields_group(current_iteration):

    fields = current_iteration.particles["fields"]

    return fields


def gdf_file_to_hdf_file(gdf_file, series):

    check_gdf_file(gdf_file)
    add_root_attributes(series, gdf_file, Constants.GDFNAMELEN)

    gdf_file.seek(2, 1)

    iteration_number = -1

    last_iteration_time = 0
    last_arr = False

    last_time = False
    first_iteration = True

    current_iteration = None
    current_spicies = None

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
            var, time = read_single_value_type(gdf_file, data_type,
                        primitive_type, size, name, series, last_iteration_time)

        is_new_iteration_nessesary = need_new_iteration_group(last_arr, arr, var, first_iteration, data_type)

        if is_new_iteration_nessesary:
            current_iteration, iteration_number \
                = create_iteration_sub_groups(iteration_number, series)
        if arr:
            if is_spicies_group_needed(current_iteration):
                current_spicies = create_new_spices_group(current_iteration)

            if is_fields_group_needed(current_iteration):
                current_fields = create_new_fields_group(current_iteration)

            read_array_type(series, gdf_file, data_type, name, primitive_type, size, current_spicies, current_fields)

        last_time = time
        last_arr = arr
        first_iteration = False


def read_ascii_character(data_type, gdf_file, size, name):

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
