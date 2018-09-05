""" Add particle patches to OpenPMD file"""

import argparse
import os
import h5py
from shutil import copyfile


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="add patches to OpenPMD file")
    parser.add_argument("-hdf", metavar='hdf_file', type=str,
                        help="hdf file without patches")
    parser.add_argument("-result", metavar='hdf_file_with_patches', type=str,
                        help="path to result file with patches")
    args = parser.parse_args()

    converter(args.hdf, args.result)
