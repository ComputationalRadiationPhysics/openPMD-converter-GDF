from __future__ import division
from pylab import *
import h5py
import time
import struct
import os
import sys
import datetime

import re


def name_to_group(name, particles, size, f):
    dict_particles = {'x': ['position', 'x'], 'y': ['position', 'y'], 'zDD': ['position', 'z'],
                      'ID': ['none', 'none']}

    if dict_particles.get(name) != None:
        if dict_particles.get(name)[0] == 'none':
            value = fromfile(f, dtype=dtype('f8'), count=int(size / 8))
            particles.create_dataset(name, data=value)

        else:
            sub_name = str(dict_particles.get(name)[0])
            sub_group = particles.require_group(sub_name)
            value = fromfile(f, dtype=dtype('f8'), count=int(size / 8))
            sub_group.create_dataset(dict_particles.get(name)[1], data=value)
    else:
        value = fromfile(f, dtype=dtype('f8'), count=int(size / 8))


def gdf_to_hdf(gdf_file_directory, hdf_file_directory):
    print ('Converting .gdf to .hdf file with hierical layout.')       
    if os.path.exists(hdf_file_directory):
        os.remove(hdf_file_directory)
    hdf_f = h5py.File(hdf_file_directory, 'a')
	
	#Constants
    GDFID  = 94325877;
    GDFNAMELEN = 16;

    # Block types
    t_dir = 256  # Directory entry start
    t_edir = 512  # Directory entry end
    t_sval = 1024  # Single valued
    t_arr = 2048  # Array

    t_ascii = int('0001', 16)  # ASCII character
    t_s32 = int('0002', 16)  # Signed long
    t_dbl = int('0003', 16)  # Double
    t_null = int('0010', 16)  # No data
    with open(gdf_file_directory, 'rb') as f:  # Important to open in binary mode 'b' to work cross platform

        # Read the GDF main header

        gdf_id_check = struct.unpack('i', f.read(4))[0]
        if gdf_id_check != GDFID:
            raise RuntimeWarning('File directory is not a .gdf file')

        time_created = struct.unpack('i', f.read(4))[0]
        format_time = datetime.datetime.fromtimestamp(time_created)
        format_time = format_time.strftime("%Y-%m-%d %H:%M:%S %Z")
        hdf_f.attrs['date'] = format_time


        creator = list(f.read(GDFNAMELEN))
        new_creator = []
        for element in creator:
            new_creator.append(element)
        creator_name = []
        for element in new_creator:
            if element is 0:
                break
            else:
                creator_name.append(chr(element))
        hdf_f.attrs['software'] = ''.join(creator_name)


        dest = f.read(GDFNAMELEN)
        new_dest = []
        for element in dest:
            new_dest.append(element)

        destination = []
        for element in new_dest:
            if element is 0:
                break
            else:
                destination.append(chr(element))
        hdf_f.attrs['destination'] = ''.join(destination)

        # get other metadata about the GDF file
        major = struct.unpack('B', f.read(1))[0]
        minor = struct.unpack('B', f.read(1))[0]
        hdf_f.attrs['gdf_version'] = str(major) + '.' + str(minor)

        major = struct.unpack('B', f.read(1))[0]
        minor = struct.unpack('B', f.read(1))[0]
        hdf_f.attrs['software version'] = str(major) + '.' + str(minor)

        major = struct.unpack('B', f.read(1))[0]
        minor = struct.unpack('B', f.read(1))[0]
        hdf_f.attrs['destination_version'] = str(major) + '.' + str(minor)
        hdf_f.attrs['iterationEncoding'] = 'groupBased'
        hdf_f.attrs['iterationFormat'] = 'test_hierical_%T.h5'
        hdf_f.attrs['particlesPath'] = 'particles/'
        hdf_f.attrs['openPMD'] = '1.1.0'
        hdf_f.attrs['openPMDextension'] = '1'
        hdf_f.attrs['basePath'] = '/data/%T/'

        f.seek(2, 1)  # skip to next block


        iteration_number = 0
        data_group = hdf_f.create_group('data')
        iteration_number_group = data_group.create_group(str(iteration_number))
        fields_group = iteration_number_group.create_group('fields')
        particles_group = iteration_number_group.create_group('particles')

        # Read GDF data blocks
        lastarr = False
        while True:
            if f.read(1) == '':
                break
            f.seek(-1, 1)

            # Read GDF block header
            name = f.read(16)
            typee = struct.unpack('i', f.read(4))[0]

            size = struct.unpack('i', f.read(4))[0]

            # Get name
            name = name.split()[0]

            # Get block type
            dir = int(typee & t_dir > 0)
            edir = int(typee & t_edir > 0)
            sval = int(typee & t_sval > 0)
            arr = int(typee & t_arr > 0)

            dattype = typee & 255
            if lastarr and not arr:
                iteration_number += 1
                iteration_number_group = data_group.create_group(str(iteration_number))
                fields_group = iteration_number_group.create_group('fields')
                particles_group = iteration_number_group.create_group('particles')
            if sval:
                if dattype == t_dbl:
                    value = struct.unpack('d', f.read(8))[0]
                elif dattype == t_null:
                    pass
                elif dattype == t_ascii:
                    value = str(f.read(size))
                    value = value.strip(' \t\r\n\0')
                    try:
                        particles_group.create_dataset(name, data=value)
                    except RuntimeError:
                        del particles_group[name]
                elif dattype == t_s32:
                    value = struct.unpack('i', f.read(4))[0]
                else:
                    print('unknown datatype of value!!!')
                    print('name=', name)
                    print('type=', typee)
                    print('size=', size)
                    value = f.read(size)
            if arr:
                if dattype == t_dbl:
                    decode_name = name.decode('ascii', errors='ignore')
                    correct_name = re.sub(r'\W+', '', decode_name)
                    name_to_group(correct_name, particles_group, size, f)
                else:
                    print('unknown datatype of value!!!')
                    print('name=', name)
                    print('type=', typee)
                    print('size=', size)
                    value = f.read(size)
            lastarr = arr;
    f.close()
    hdf_f.close()
    print ('Converting .gdf to .hdf file with hierical layout... Complete.')
    
    print('Converting .gdf to .hdf file with hierical layout... Complete.')


def terminal_call(terminal_args):
    hierical_suffix = '_hierical'
    
    gdf_arg = False
    hdf_arg = False

    #
    for arg in terminal_args:
        if arg[-4:] == '.gdf':
            gdf_file_directory = arg
            gdf_arg = True
        elif arg[-4:] == '.hdf':
            hdf_file_directory = arg
            hdf_arg = True

    if gdf_arg:
        if os.path.exists(gdf_file_directory):
            if not hdf_arg:
                hdf_slab_file_directory = gdf_file_directory[:-4] + '.hdf'
                hdf_file_directory = gdf_file_directory[:-4] + hierical_suffix + '.hdf'
                print ('Destination .hdf directory not specified. Defaulting to ' + hdf_slab_file_directory)
            else:
                hdf_slab_file_directory = hdf_file_directory
                hdf_file_directory = hdf_slab_file_directory[:-4] + hierical_suffix + '.hdf'
            gdf_to_hdf(gdf_file_directory, hdf_file_directory)
        else:
            print ('The .gdf file does not exist to convert to .hdf')

if __name__ == "__main__":
    terminal_args = sys.argv
    terminal_call(terminal_args)
