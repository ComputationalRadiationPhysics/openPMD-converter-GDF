""" Add particle patches to OpenPMD file"""

import argparse
import os
import h5py
from shutil import copyfile


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
    args = parser.parse_args()

    converter(args.hdf, args.result)
