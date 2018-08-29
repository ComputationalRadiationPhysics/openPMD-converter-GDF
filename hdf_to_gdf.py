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
                     'mass': 'm'}


def write_iteration(hdf_file, gdf_file):
   """ Write all iteration to hdf_file """

   dict_array_names = {}
   data_group = hdf_file.get('data')
   hdf_datasets = Collect_Datasets()
   hdf_file.visititems(hdf_datasets)
   size_of_main_array = add_datasets_values(hdf_file, hdf_datasets, dict_array_names)
   add_group_values(hdf_datasets, size_of_main_array, dict_array_names)
   sorted_values = sorted(dict_array_names, key=lambda x: (x[0], x[1]))
   iterate_values(gdf_file, dict_array_names, sorted_values)


def iterate_values(gdf_file, dict_array_names, sorted_values):
    """ Write one iteration to hdf_file """

    last_name_of_particles = ''
    for name_of_particles, name_of_dataset in sorted_values:
        array = dict_array_names[name_of_particles, name_of_dataset]
        if Name_of_arrays.dict_datasets.get(name_of_dataset) != None:
            if last_name_of_particles != name_of_particles:
                write_ascii_name('var', len(name_of_particles), gdf_file, name_of_particles)
                last_name_of_particles = name_of_particles
            write_double_dataset(gdf_file, Name_of_arrays.dict_datasets.get(name_of_dataset), len(array), array)


def add_datasets_values(hdf_file, hdf_datasets, dict_array_names):
    """ Add values from dataset """

    size_of_main_array = 0
    for key in hdf_datasets.sets:
        name_of_particles, name_of_dataset = parse_group_name(key)
        if name_of_dataset != '' and name_of_particles != '':
            my_array = hdf_file[key.name][()]
            dict_array_names[name_of_particles, name_of_dataset] = my_array
            size_of_main_array = len(my_array)

    return size_of_main_array


def parse_group_name(key_value):
    """ Separate name of group to particles name and dataset name """

    particles_idx = key_value.name.find("particles")
    if (particles_idx == -1):
        return '', ''
    substring = key_value.name[particles_idx + 10: len(key_value.name)]
    name_of_particles_idx = substring.find("/")
    name_of_particles = substring[0: name_of_particles_idx]
    name_of_dataset = substring[substring.find("/") + 1: len(substring)]
    return name_of_particles, name_of_dataset


def add_group_values(hdf_datasets, size_of_main_array, dict_array_names):
    """ Add values from groups with single value """

    for key in hdf_datasets.grops_values:
        value = key.attrs['value']
        i = 0
        my_array = []
        while i < size_of_main_array:
            my_array.append(value)
            i = i + 1
        name_of_particles, name_of_dataset = parse_group_name(key)
        if name_of_dataset != '' and name_of_particles != '':
            dict_array_names[name_of_particles, name_of_dataset] = my_array


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
    """ Parse files from input arguments"""

    gdf_file = ''
    hdf_file = ''
    for arg in file_names:
        if arg[-4:] == '.gdf':
            gdf_file = arg
        elif arg[-3:] == '.h5':

            hdf_file = arg
    return gdf_file, hdf_file


def converter(hdf_file, gdf_file):
    """ Check correct of arguments"""

    if hdf_file != '':
        if os.path.exists(hdf_file):
            if gdf_file == '':
                gdf_file = hdf_file[:-4] + '.gdf'
                print('Destination .gdf directory not specified. Defaulting to ' + gdf_file)
            else:
                gdf_file = gdf_file[:-4] + '.gdf'

            hdf_to_gdf(hdf_file, gdf_file)
        else:
            print('The .hdf file does not exist to convert to .gdf')


def main(file_names):
    gdf_path, hdf_path = files_from_args(file_names)
    converter(hdf_path, gdf_path)


if __name__ == "__main__":
    file_names = sys.argv
    main(file_names)
