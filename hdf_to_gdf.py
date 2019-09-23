"""Converter from openPMD to GPT format"""


from __future__ import division
import os
import sys
import struct
import h5py
from datetime import datetime
import time


def hdf_to_gdf(hdf_file_directory, gdf_file_directory):
    """ Find hdf file in hdf_file_directory, find gdf_file_directory"""

    print('Converting .gdf to .hdf file')
    if os.path.exists(gdf_file_directory):
        os.remove(gdf_file_directory)

    hdf_file = h5py.File(hdf_file_directory, 'a')
    with open(gdf_file_directory, 'wb') as gdf_file:
        hdf_file_to_gdf_file(gdf_file, hdf_file)

    gdf_file.close()
    hdf_file.close()
    print('Converting .hdf to .gdf file... Complete.')


def hdf_file_to_gdf_file(gdf_file, hdf_file):
    """ Convert from hdf file to gdf file """

    add_gdf_id(gdf_file)
    add_time_root_attribute(gdf_file, hdf_file)
    add_creator_name_root_attribute(gdf_file, hdf_file)
    add_dest_name_root_attribute(gdf_file, hdf_file)
    add_required_version_root_attribute(gdf_file, hdf_file)
    write_first_block(gdf_file)
    write_iteration(hdf_file, gdf_file)


def write_first_block(gdf_file):
    """ Write required empty first block """

    name = '00'
    chars_name = []
    for c in name:
        chars_name.append(c)

    for s in chars_name:
        s_pack = struct.pack('c', s.encode('ascii'))
        gdf_file.write(s_pack)


class Collect_Datasets():
    """ Collect values from datasets in hdf file """

    list_values_group = ['charge', 'mass']

    def __init__(self):
        self.sets = []
        self.grops_values = []

    def __call__(self, name, node):
        if isinstance(node, h5py.Dataset):
            self.sets.append(node)
        if isinstance(node, h5py.Group):
            for value in self.list_values_group:
                if value in node.name:
                    self.grops_values.append(node)
        return None


class Particles_Groups():
    """

    Collect particles groups from hdf file
    particles_name -- name of main partcles group

    """

    def __init__(self, particles_name):

        self.name_particles = particles_name
        self.particles_groups = []
        self.particles_names = []

    def __call__(self, name, node):
        if isinstance(node, h5py.Group):
            name_idx = node.name.find(self.name_particles)
            if name_idx != -1:
                group_particles_name = node.name[name_idx + len(self.name_particles) + 1:]
                if group_particles_name.find('/') == -1 and group_particles_name != '':
                    self.particles_names.append(group_particles_name)
                    self.particles_groups.append(node)
        return None


class Iteration_Groups():
    """

    Collect iteration groups from hdf file
    particles_name -- name of main partcles group

    """

    def __init__(self):
        self.iteration_groups = []
        self.iteration_names = []

    def __call__(self, name, node):
        if isinstance(node, h5py.Group):
            data_idx = node.name.find("data")
            iteration_name = node.name[data_idx + 5:]
            if iteration_name.find('/') == -1 and iteration_name != '' and RepresentsInt(iteration_name):
                self.iteration_groups.append(node)
                self.iteration_names.append(iteration_name)
        return None


def decode_name(attribute_name):
    """ Decode name from binary """

    decoding_name = attribute_name.decode('ascii', errors='ignore')
    decoding_name = re.sub(r'\W+', '', decoding_name)
    return decoding_name


def get_particles_name(hdf_file):
    """ Get name of particles group """

    particles_name = ''
    if hdf_file.attrs.get('particlesPath') != None:
        particles_name = hdf_file.attrs.get('particlesPath')
        particles_name = decode_name(particles_name)
    else:
        particles_name = 'particles'
    return particles_name



class Name_of_arrays:
    """ Storage of datasets in h5 file """

    dict_datasets = {'momentum/x': 'Bx',
                     'momentum/y': 'By',
                     'momentum/z': 'Bz',
                     'position/x': 'x',
                     'position/y': 'y',
                     'position/z': 'z',
                     'id': 'ID',
                     'charge': 'charge',
                     'weighting': 'weighting',
                     'mass': 'm',
                     'positionOffset/x': 'positionOffset_x',
                     'positionOffset/y': 'positionOffset_y',
                     'positionOffset/z': 'positionOffset_z'}


def read_position_offset(hdf_datasets):


    position_offset_values = DatasetReader('positionOffset')
    position_offset_group = hdf_datasets.position_offset[0]
    position_offset_group.visititems(position_offset_values)

    offset_unit_si = position_offset_values.get_unit_si_array()

    return position_offset_values, offset_unit_si


   """ Write all iteration to hdf_file """

   dict_array_names = {}
   data_group = hdf_file.get('data')
   hdf_datasets = Collect_Datasets()
   hdf_file.visititems(hdf_datasets)
   size_of_main_array = add_datasets_values(hdf_file, hdf_datasets, dict_array_names)
   add_group_values(hdf_datasets, size_of_main_array, dict_array_names, hdf_file)
   sorted_values = sorted(dict_array_names, key=lambda x: (x[0], x[1]))
   iterate_values(gdf_file, dict_array_names, sorted_values)

def get_absolute_values(hdf_file, path_dataset, position_offset, unit_si_offset, unit_si_position, idx_axis):
    array_dataset = hdf_file[path_dataset][()]
    size = hdf_file[path_dataset][()].size
    offset = hdf_file[position_offset][()]
    absolute_values = get_absolute_coordinates(array_dataset, offset, unit_si_offset, unit_si_position, idx_axis)
    return absolute_values


def write_coord_values(axis_idx, vector_values, position_offset, name_dataset, gdf_file, hdf_file, unit_si_offset, unit_si_position):


    write_dataset_header(Name_of_arrays.dict_datasets.get(name_dataset), gdf_file)
    absolute_values = get_absolute_values(hdf_file, vector_values, position_offset, unit_si_offset, unit_si_position, axis_idx)
    write_dataset(gdf_file, absolute_values)


def get_absolute_momentum(hdf_file, axis_idx, path_dataset, unit_si_momentum):

    array_dataset = hdf_file[path_dataset][()]
    absolute_momentum = []
    for point in array_dataset:
        absolute_momentum.append(point * unit_si_momentum[axis_idx])

    return absolute_momentum


class DatasetReader():
    """

     Read datasets values from hdf file
     name_dataset -- name of base group

    """

    def __init__(self, name_dataset):
        self.vector_x = ''
        self.vector_y = ''
        self.vector_z = ''
        self.unit_SI_x = 1.
        self.unit_SI_y = 1.
        self.unit_SI_z = 1.
        self.name_dataset = name_dataset
        self.size = 0

    def __call__(self, name, node):

        dataset_x = self.name_dataset + '/x'
        dataset_y = self.name_dataset + '/y'
        dataset_z = self.name_dataset + '/z'
        if isinstance(node, h5py.Dataset):
            if node.name.endswith(dataset_x):
                self.vector_x = node.name
                self.unit_SI_x = node.attrs["unitSI"]

            if node.name.endswith(dataset_y):
                self.vector_y = node.name
                self.unit_SI_y = node.attrs["unitSI"]

            if node.name.endswith(dataset_z):
                self.vector_z = node.name
                self.unit_SI_z = node.attrs["unitSI"]

        return None
    """ Write one iteration to hdf_file """

    last_name_of_particles = ''
    last_name_of_iteration = 0
    for name_of_iteration, name_of_particles, name_of_dataset in sorted_values:
        array = dict_array_names[name_of_iteration, name_of_particles, name_of_dataset]
        if Name_of_arrays.dict_datasets.get(name_of_dataset) != None:
            if not(float(last_name_of_iteration) == float(name_of_iteration)):
                write_float('time', gdf_file, float(name_of_iteration))
                last_name_of_iteration = name_of_iteration

            if last_name_of_particles != name_of_particles:
                write_ascii_name('var', len(name_of_particles), gdf_file, name_of_particles)
                last_name_of_particles = name_of_particles
            write_double_dataset(gdf_file, Name_of_arrays.dict_datasets.get(name_of_dataset), len(array), array)


def add_datasets_values(hdf_file, hdf_datasets, dict_array_names):
    """ Add values from dataset """

    size_of_main_array = 0
    for key in hdf_datasets.sets:
        name_of_particles, name_of_dataset, name_of_iteration = parse_group_name(key, hdf_file)
        if name_of_dataset != '' and name_of_particles != '':
            my_array = hdf_file[key.name][()]
            dict_array_names[name_of_iteration, name_of_particles, name_of_dataset] = my_array
            size_of_main_array = len(my_array)

    return size_of_main_array


def parse_group_name(key_value, hdf_file):
    """ Separate name of group to particles name and dataset name """
    numer_of_iteration = key_value.name.find('data')

    particles_idx = key_value.name.find("particles")
    if (particles_idx == -1):
        return '', '', ''


    name_of_iteration = 0
    if hdf_file[key_value.name[0: particles_idx]].attrs.get('time') != None:
        name_of_iteration = hdf_file[key_value.name[0: particles_idx]].attrs.get('time')


    substring = key_value.name[particles_idx + 10: len(key_value.name)]
    name_of_particles_idx = substring.find("/")
    name_of_particles = substring[0: name_of_particles_idx]
    name_of_dataset = substring[substring.find("/") + 1: len(substring)]
    return name_of_particles, name_of_dataset, name_of_iteration


def add_group_values(hdf_datasets, size_of_main_array, dict_array_names, hdf_file):
    """ Add values from groups with single value """

    for key in hdf_datasets.grops_values:
        if key.attrs.get('value') != None:
            value = key.attrs['value']
            i = 0
            my_array = []
            while i < size_of_main_array:
                my_array.append(value)
                i = i + 1
            name_of_particles, name_of_dataset, name_of_iteration = parse_group_name(key, hdf_file)
            if name_of_dataset != '' and name_of_particles != '':
                dict_array_names[name_of_iteration, name_of_particles, name_of_dataset] = my_array


def write_ascii_name(name, size, gdf_file, ascii_name):
    """ Write ascii name of value """

    write_string(name, gdf_file)
    type_bin = struct.pack('i', int(1025))
    gdf_file.write(type_bin)
    size_bin = struct.pack('i', int(size))
    gdf_file.write(size_bin)
    charlist = list(ascii_name)
    type_size = str(size) + 's'
    gdf_file.write(struct.pack(type_size, ascii_name.encode('ascii')))


def write_float(name, gdf_file, value):
    write_string(name, gdf_file)
    type_bin = struct.pack('i', int(1283))
    gdf_file.write(type_bin)
    size_bin = struct.pack('i', 8)
    gdf_file.write(size_bin)
    gdf_file.write(struct.pack('d', value))


def write_double_dataset(gdf_file, name, size, array_dataset):
    """ Write dataset of double values """

    write_string(name, gdf_file)
    type_bin = struct.pack('i', int(2051))
    gdf_file.write(type_bin)
    size_bin = struct.pack('i', int(size * 8))
    gdf_file.write(size_bin)
    type_size = str(size)  +'d'
    gdf_file.write(struct.pack(type_size, *array_dataset))


class Block_types:
    """ Block types for each type in GDF file"""

    directory = 256  # Directory entry start
    edir = 512  # Directory entry end
    single_value = 1024  # Single valued
    array = 2048  # Array
    ascii_character = int('0001', 16)  # ASCII character
    signed_long = int('0002', 16)  # Signed long
    double_type = int('0003', 16)  # Double
    no_data = int('0010', 16)  # No data


def add_gdf_id(gdf_file):
   """ Add required indefication block of gdf file"""

   gdf_id_byte = struct.pack('i', Constants.GDFID)
   gdf_file.write(gdf_id_byte)


def add_time_root_attribute(gdf_file, hdf_file):
    """ Add time of creation to root"""

    if  hdf_file.attrs.get('date') != None:
        time_created = hdf_file.attrs.get('date')
        decoding_name = time_created.decode('ascii', errors='ignore')
        time_format = datetime.strptime(str(decoding_name), "%Y-%m-%d %H:%M:%S %z")
        seconds = time.mktime(time_format.timetuple())
        time_created_byte = struct.pack('i', int(seconds))
        gdf_file.write(time_created_byte)
    else:
        seconds = int(round(time.time()))
        time_created_byte = struct.pack('i', int(seconds))
        gdf_file.write(time_created_byte)


def add_creator_name_root_attribute(gdf_file, hdf_file):
    """ Add name of creator to root"""

    if hdf_file.attrs.get('software') != None:
        software = hdf_file.attrs.get('software')
        decode_software = software.decode('ascii', errors='ignore')
        write_string(decode_software, gdf_file)
    else:
        software = 'empty'
        write_string(software, gdf_file)


def add_dest_name_root_attribute(gdf_file, hdf_file):
    """ Add dest name to root attribute """

    if hdf_file.attrs.get('destination') != None:
        destination = hdf_file.attrs.get('destination')
        decode_destination = destination.decode('ascii', errors='ignore')
        write_string(decode_destination, gdf_file)
    else:
        destination = 'empty'
        write_string(destination, gdf_file)


def add_required_version_root_attribute(gdf_file, hdf_file):
    """ Write one iteration to hdf_file """

    add_versions('gdf_version', gdf_file, hdf_file, 1, 1)
    add_versions('softwareVersion', gdf_file, hdf_file, 3, 0)
    add_versions('destination_version', gdf_file, hdf_file)


def add_versions(name, gdf_file, hdf_file, major = 0, minor = 0):
    """Write version of file to gdf file"""

    if hdf_file.attrs.get(name) != None:
        version = hdf_file.attrs.get(name)
        decode_version = version.decode('ascii', errors='ignore')
        point_idx = decode_version.find('.')
        if point_idx == -1:
            major = decode_version
            minor = 0
        else:
            if RepresentsInt(decode_version[0: point_idx - 1]):
                major = decode_version[0: point_idx - 1]
            else:
                major = 0
            if RepresentsInt(decode_version[point_idx - 1: len(decode_version) - 1]):
                minor = decode_version[point_idx - 1: len(decode_version) - 1]
            else:
                minor = 0
        major_bin = struct.pack('B', int(major))
        minor_bin = struct.pack('B', int(minor))
        gdf_file.write(major_bin)
        gdf_file.write(minor_bin)
    else:
        major_bin = struct.pack('B', major)
        minor_bin = struct.pack('B', minor)
        gdf_file.write(major_bin)
        gdf_file.write(minor_bin)


def RepresentsInt(s):
    """Check that argument is int value"""

    try:
        int(s)
        return True
    except ValueError:
        return False


def write_string(name, gdf_file):
    """Write string value to gdf file"""

    while len(name) < Constants.GDFNAMELEN:
        name += chr(0)

    chars_name = []
    for c in name:
        chars_name.append(c)

    for s in chars_name:
        s_pack = struct.pack('c', s.encode('ascii'))
        gdf_file.write(s_pack)


class Constants:
    GDFID = 94325877
    GDFNAMELEN = 16


def files_from_args(file_names):
    gdf_file = ''
    hdf_file = ''
    if len(file_names) >= 2:
        hdf_file = file_names[1]
    if len(file_names) >= 3:
        gdf_file = file_names[2]
    return gdf_file, hdf_file


def converter(hdf_file, gdf_file):
    """ Check correct of arguments"""

    if hdf_file != '':
        if os.path.exists(hdf_file):
            if gdf_file == '':
                gdf_file = hdf_file[:-4] + '.gdf'
                print('Destination .gdf directory not specified. Defaulting to ' + gdf_file)

            hdf_to_gdf(hdf_file, gdf_file)
        else:
            print('The .hdf file does not exist to convert to .gdf')


def main(file_names):
    gdf_path, hdf_path = files_from_args(file_names)
    converter(hdf_path, gdf_path)


if __name__ == "__main__":
    file_names = sys.argv
    main(file_names)
