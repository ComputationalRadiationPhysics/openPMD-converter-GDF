""" Add particle patches to OpenPMD file"""

import argparse
import os
import h5py
from shutil import copyfile


def OpenPMD_add_patches(hdf_file_name, name_of_file_with_patches, grid_sizes, devices_numbers):
    copyfile(hdf_file_name, name_of_file_with_patches)

    file_with_patches = h5py.File(name_of_file_with_patches)
    hdf_file = h5py.File(hdf_file_name)


def add_patches(hdf_file, hdf_file_with_patches):
    """ Check correct of arguments"""

    name_of_file_with_patches = ''
    if hdf_file != '':
        if os.path.exists(hdf_file):
            name = hdf_file[:-4]
            idx_of_name = name.rfind('/')
            if idx_of_name != -1:
                name_of_file_with_patches = hdf_file_with_patches + hdf_file[idx_of_name + 1: -3] + 'with_patches.h5'
            else:
                name_of_file_with_patches = hdf_file_with_patches + hdf_file[:-3] + '.h5'
            OpenPMD_add_patches(hdf_file, name_of_file_with_patches)
        else:
            print('The .hdf file does not exist')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="add patches to OpenPMD file")
    parser.add_argument("-hdf", metavar='hdf_file', type=str,
                        help="hdf file without patches")
    parser.add_argument("-result", metavar='hdf_file_with_patches', type=str,
                        help="path to result file with patches")
    parser.add_argument("-gridSize", type=int, nargs=3,
                        metavar=('size_grid_x', 'size_grid_y', 'size_grid_z'),
                        help="Size of the simulation grid in cells as x y z")
    parser.add_argument("-devicesNumber", type=int, nargs=3,
                        metavar=('devices_number_x', 'devices_number_y', 'size_grid_z'),
                        help="Number of devices in each dimension (x,y,z)")

    args = parser.parse_args()

    converter(args.hdf, args.result)
