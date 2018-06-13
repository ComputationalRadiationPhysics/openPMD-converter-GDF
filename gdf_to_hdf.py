from __future__ import division
from pylab import *
import h5py
import time
import struct
import os
import sys

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
        hdf_f.attrs['time_created'] = str(time_created) + ': ' + str(time.ctime(int(time_created)))


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
        hdf_f.attrs['creator_name'] = ''.join(creator_name)


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
        hdf_f.attrs['creator_version'] = str(major) + '.' + str(minor)

        data_group = hdf_f.create_group('data')

        # Initialise values to print progress to terminal
        file_size = os.stat(gdf_file_directory)[6]
        start_time = time.time()
        last_running_time = 0

        major = struct.unpack('B', f.read(1))[0]
        minor = struct.unpack('B', f.read(1))[0]
        hdf_f.attrs['destination_version'] = str(major) + '.' + str(minor)

        f.seek(2, 1)  # skip to next block

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

    f.close()
    hdf_f.close()
    print ('Converting .gdf to .hdf file with hierical layout... Complete.')
    
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
