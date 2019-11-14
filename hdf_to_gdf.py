"""Converter from openPMD to GPT format"""


from __future__ import division
import os
import sys
import struct
import h5py
from datetime import datetime
import time
import re
import argparse
import numpy
import openpmd_api


def hdf_to_gdf(hdf_file_directory, gdf_file_directory, max_cell_size, species):
    """ Find hdf file in hdf_file_directory, find gdf_file_directory"""

    print('Converting .gdf to .hdf file')

    default_max_cell_size = 1000000
    if gdf_file_directory == None:
        gdf_file_directory = hdf_file_directory[:-3] + '.gdf'

    if max_cell_size == None:
        max_cell_size = default_max_cell_size

    if species == None:
        species = ''

    print('Destination .gdf directory not specified. Defaulting to ' + gdf_file_directory)

    series_hdf = openpmd_api.Series(hdf_file_directory, openpmd_api.Access_Type.read_only)

    with open(gdf_file_directory, 'wb') as gdf_file:
        hdf_file_to_gdf_file(gdf_file, series_hdf, max_cell_size, species)


    gdf_file.close()
    print('Converting .hdf to .gdf file... Complete.')


def hdf_file_to_gdf_file(gdf_file, series_hdf, max_cell_size, species):
    """ Convert from hdf file to gdf file """

    add_gdf_id(gdf_file)

    add_time_root_attribute(gdf_file, series_hdf)
    add_creator_name_root_attribute(gdf_file, series_hdf)
    add_dest_name_root_attribute(gdf_file, series_hdf)
    add_required_version_root_attribute(gdf_file, series_hdf)
    write_first_block(gdf_file)
    write_file(series_hdf, gdf_file, max_cell_size, species)



def write_first_block(gdf_file):
    """ Write required empty first block """

    name = '00'
    chars_name = []
    for c in name:
        chars_name.append(c)

    for s in chars_name:
        s_pack = struct.pack('c', s.encode('ascii'))
        gdf_file.write(s_pack)


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
                     'weighting': 'nmacro',
                     'mass': 'm'}


def read_position_offset(hdf_datasets):


    position_offset_values = DatasetReader('positionOffset')
    position_offset_group = hdf_datasets.position_offset[0]
    position_offset_group.visititems(position_offset_values)

    offset_unit_si = position_offset_values.get_unit_si_array()

    return position_offset_values, offset_unit_si


def write_scalar(gdf_file, particle_species, size_dataset, max_cell_size, name_scalar):

    if not check_item_exist(particle_species, name_scalar):
        return

    SCALAR = openpmd_api.Mesh_Record_Component.SCALAR
    mass = particle_species[name_scalar][SCALAR]

    value = mass.get_attribute("value")
    mass_unit = mass.get_attribute("unitSI")

    write_double_dataset_values(gdf_file, name_scalar, size_dataset, value * mass_unit, max_cell_size)


def write_particles_type(series, particle_species, gdf_file, max_cell_size):

    position_offset = particle_species["positionOffset"]
    position = particle_species["position"]

    iterate_coords(series, gdf_file, position, position_offset, max_cell_size)

    momentum_values = particle_species["momentum"]
    iterate_momentum(series, gdf_file, momentum_values, max_cell_size)
    size_dataset = 1000000
    write_scalar(gdf_file, particle_species, size_dataset, max_cell_size, "mass")
    write_scalar(gdf_file, particle_species, size_dataset, max_cell_size, "charge")


def check_item_exist(particle_species, name_item):

    item_exist = False

    for value in particle_species.items():
        if value[0] == name_item:
            item_exist = True

    return item_exist


def all_species(series, iteration, gdf_file, max_cell_size):

    for name_group in iteration.particles:
        if not (check_item_exist(iteration.particles[name_group], "momentum") and
                check_item_exist(iteration.particles[name_group], "position")):
            continue

        write_ascii_name('var', len(name_group), gdf_file, name_group)
        write_particles_type(series, iteration.particles[name_group], gdf_file, max_cell_size)


def one_type_species(series, iteration, gdf_file, max_cell_size, species):

    for name_group in iteration.particles:
        if name_group == species:
            if not (check_item_exist(iteration.particles[name_group], "momentum") and
                    check_item_exist(iteration.particles[name_group], "position")):
                continue

            write_ascii_name('var', len(name_group), gdf_file, name_group)
            write_particles_type(series, iteration.particles[name_group], gdf_file, max_cell_size)


def write_data(series,iteration, gdf_file, max_cell_size, species):

    time = iteration.time()
    write_float('time', gdf_file, float(time))

    if species == '':
        all_species(series, iteration, gdf_file, max_cell_size)
    else:
        one_type_species(series, iteration, gdf_file, max_cell_size, species)


def write_file(series_hdf, gdf_file, max_cell_size, species):
    for iteration in series_hdf.iterations:
        write_data(series_hdf, series_hdf.iterations[iteration], gdf_file, max_cell_size, species)


def get_absolute_values(series, position_axis, position_offset_axis, idx_start, idx_end):

    position_dataset = position_axis[idx_start:idx_end]
    position_offset = position_offset_axis[idx_start:idx_end]
    series.flush()
    absolute_values = get_absolute_coordinates(position_dataset, position_offset, position_offset_axis.unit_SI, position_axis.unit_SI)

    return absolute_values


def write_coord_values(series, name_dataset, position_axis, position_offset_axis, gdf_file, max_cell_size):

    write_dataset_header(Name_of_arrays.dict_datasets.get(name_dataset), gdf_file)
    size = position_axis.shape[0]
    size_bin = struct.pack('I', int(size * 8))
    gdf_file.write(size_bin)

    number_cells = int(size / max_cell_size)
    for i in range(1, number_cells + 1):
        idx_start = (i - 1) * max_cell_size
        idx_end = i * max_cell_size
        absolute_values = get_absolute_values(series, position_axis, position_offset_axis, idx_start, idx_end)
        type_size = str(max_cell_size) + 'd'
        gdf_file.write(struct.pack(type_size, *absolute_values))

    absolute_values = get_absolute_values(series, position_axis, position_offset_axis, number_cells * max_cell_size, size)
    last_cell_size = size - number_cells * max_cell_size
    type_size = str(last_cell_size) + 'd'
    gdf_file.write(struct.pack(type_size, *absolute_values))


def get_absolute_momentum(series, momentum_values, idx_start, idx_end):

    array_dataset = momentum_values[idx_start:idx_end]
    series.flush()
    unit_si_momentum = momentum_values.unit_SI
    absolute_momentum = []
    for point in array_dataset:
        absolute_momentum.append(point * unit_si_momentum)

    return absolute_momentum


def write_momentum_values(series, name_dataset, momentum_values, gdf_file, max_cell_size):

    write_dataset_header(Name_of_arrays.dict_datasets.get(name_dataset), gdf_file)
    size = momentum_values.shape[0]
    size_bin = struct.pack('i', int(size * 8))
    gdf_file.write(size_bin)

    number_cells = int(size / max_cell_size)
    for i in range(1, number_cells + 1):
        idx_start = (i - 1) * max_cell_size
        idx_end = i * max_cell_size
        absolute_momentum = get_absolute_momentum(series, momentum_values, idx_start, idx_end)
        type_size = str(max_cell_size) + 'd'
        gdf_file.write(struct.pack(type_size, *absolute_momentum))

    absolute_momentum = get_absolute_momentum(series, momentum_values, number_cells * max_cell_size, size)
    last_cell_size = size - number_cells * max_cell_size
    type_size = str(last_cell_size) + 'd'
    gdf_file.write(struct.pack(type_size, *absolute_momentum))


def iterate_momentum(series, gdf_file, momentum_values, max_cell_size):

    dimension = len(momentum_values)

    if dimension == 2:
        name_dataset = str("momentum" + '/x')
        write_momentum_values(series, name_dataset, momentum_values["x"], gdf_file, max_cell_size)

        name_dataset = str("momentum" + '/y')
        write_momentum_values(series, name_dataset, momentum_values["y"], gdf_file, max_cell_size)

    if dimension == 3:
        name_dataset = str("momentum" + '/x')
        write_momentum_values(series, name_dataset, momentum_values["x"], gdf_file, max_cell_size)

        name_dataset = str("momentum" + '/y')
        write_momentum_values(series, name_dataset, momentum_values["y"], gdf_file, max_cell_size)

        name_dataset = str("momentum" + '/z')
        write_momentum_values(series, name_dataset, momentum_values["z"], gdf_file, max_cell_size)


def iterate_coords(series, gdf_file, position, position_offset, max_cell_size):

    dimension = len(position)
    if dimension == 2:
        name_dataset = str("position" + '/x')
        write_coord_values(series, name_dataset, position["x"], position_offset["x"], gdf_file, max_cell_size)

        name_dataset = str("position" + '/y')
        write_coord_values(series, name_dataset, position["y"], position_offset["y"], gdf_file, max_cell_size)

    if dimension == 3:
        name_dataset = str("position" + '/x')
        write_coord_values(series, name_dataset, position["x"], position_offset["x"], gdf_file, max_cell_size)

        name_dataset = str("position" + '/y')
        write_coord_values(series, name_dataset, position["y"], position_offset["y"], gdf_file, max_cell_size)

        name_dataset = str("position" + '/z')
        write_coord_values(series, name_dataset, position["z"], position_offset["z"], gdf_file, max_cell_size)



def write_dataset(gdf_file, absolute_values):
    """" Write dataset of double values """

    size = len(absolute_values)
    size_bin = struct.pack('i', int(size * 8))
    gdf_file.write(size_bin)
    type_size = str(size) + 'd'
    gdf_file.write(struct.pack(type_size, *absolute_values))


def get_absolute_coordinates(values, offset, unit_si_offset, unit_si_position):

    absolute_result = []

    i = 0
    for point in values:
        i=+1
        absolute_coord = point * unit_si_position + offset[i] * unit_si_offset
        absolute_result.append(absolute_coord)

    return absolute_result


def add_group_values(hdf_datasets, size_of_main_array, gdf_file, max_cell_size):
    """ Add values from groups with single value """

    for key in hdf_datasets.grops_values:
        if key.attrs.get('value') != None:
            value = key.attrs['value']
            name_attribute = key.name[key.name.rfind('/')+1:]
            write_double_dataset_values(gdf_file, Name_of_arrays.dict_datasets.get(name_attribute), size_of_main_array, value, max_cell_size)


def write_double_dataset_values(gdf_file, name, size_dataset, value, max_cell_size):
    """" Write dataset of double values """

    write_dataset_header(name, gdf_file)

    size_bin = struct.pack('i', int(size_dataset * 8))
    gdf_file.write(size_bin)

    number_cells = int(size_dataset / max_cell_size)
    for i in range(1, number_cells + 1):
        array_dataset = [value] * max_cell_size
        type_size = str(max_cell_size) + 'd'
        gdf_file.write(struct.pack(type_size, *array_dataset))

    last_cell_size = size_dataset - number_cells * max_cell_size
    array_dataset = [value] * last_cell_size
    type_size = str(last_cell_size) + 'd'
    gdf_file.write(struct.pack(type_size, *array_dataset))



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


def write_dataset_header(name, gdf_file):
    write_string(name, gdf_file)
    type_bin = struct.pack('i', int(2051))
    gdf_file.write(type_bin)


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


def add_time_root_attribute(gdf_file, series_hdf):
    """ Add time of creation to root"""

    data_name = series_hdf.date
    time_format = datetime.strptime(data_name, "%Y-%m-%d %H:%M:%S %z")
    seconds = time.mktime(time_format.timetuple())
    time_created_byte = struct.pack('i', int(seconds))
    gdf_file.write(time_created_byte)


def add_creator_name_root_attribute(gdf_file, series_hdf):
    """ Add name of creator to root"""

    software = series_hdf.software
    write_string(software, gdf_file)


def add_dest_name_root_attribute(gdf_file, hdf_file):
    """ Add dest name to root attribute """

    destination = 'empty'
    write_string(destination, gdf_file)


def add_required_version_root_attribute(gdf_file, series_hdf):
    """ Write one iteration to hdf_file """

    add_versions('gdf_version', gdf_file, series_hdf, 1, 1)
    add_versions('softwareVersion', gdf_file, series_hdf, 3, 0)
    add_versions('destination_version', gdf_file, series_hdf)


def add_versions(name, gdf_file, hdf_file, major = 0, minor = 0):
    """Write version of file to gdf file"""

    major_bin = struct.pack('B', int(major))
    minor_bin = struct.pack('B', int(minor))
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


if __name__ == "__main__":
    """ Parse arguments from command line """

    parser = argparse.ArgumentParser(description="conversion from gdf to hdf")

    parser.add_argument("-hdf", metavar='hdf_file', type=str,
                        help="hdf file for conversion")

    parser.add_argument("-gdf", metavar='gdf_file', type=str,
                        help="result gdf file")

    parser.add_argument("-max_cell", metavar='max_cell', type=str,
                        help="result gdf file")

    parser.add_argument("-species", metavar='species', type=str,
                        help="one species to convert")

    args = parser.parse_args()

    hdf_to_gdf(args.hdf, args.gdf, args.max_cell, args.species)

