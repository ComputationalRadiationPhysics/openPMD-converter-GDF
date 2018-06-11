
import h5py
import struct
import os
import sys
import time

def gdf_to_hdf(gdf_file_directory, hdf_file_directory):
    print ('Converting .gdf to .hdf file with hierical layout.')       
    if os.path.exists(hdf_file_directory):
        os.remove(hdf_file_directory)
    hdf_f = h5py.File(hdf_file_directory, 'a')
	
	#Constants
    GDFID  = 94325877;
    GDFNAMELEN = 16;

    with open(gdf_file_directory, 'rb') as f:
        
        gdf_id_check = struct.unpack('i', f.read(4))[0]
        if gdf_id_check != GDFID:
            raise RuntimeWarning('File directory is not a .gdf file')
        time_created = struct.unpack('i', f.read(4))[0]
        hdf_f.attrs['time_created'] = str(time_created) + ': ' + str(time.ctime(int(time_created)))

        # get creator name and use string part upto zero-character
        creator = list(f.read(GDFNAMELEN))
        print(type(f.read(GDFNAMELEN)))
        dest = f.read(GDFNAMELEN)

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


        f.seek(2, 1)  # skip to next block

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
