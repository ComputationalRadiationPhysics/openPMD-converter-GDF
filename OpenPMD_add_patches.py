""" Add particle patches to OpenPMD file"""

import argparse
import os
import h5py
from shutil import copyfile
import re


class List_coorditates():
    """ Collect values from datasets in hdf file """

    def __init__(self):
        self.list_x = None
        self.list_y = None
        self.list_z = None

    def __call__(self, name, node):
        if name == 'position':
            for key in node.keys():
                if key == 'x':
                    print('find X!!!! ')
                    self.list_x = node[key][()]
                elif key == 'y':
                    print('find Y!!!! ')
                    self.list_y = node[key][()]
                elif key == 'z':
                    print('find Z!!!! ')
                    self.list_z = node[key][()]
        return None

def OpenPMD_add_patches(hdf_file_name, name_of_file_with_patches, grid_sizes, devices_numbers):
    copyfile(hdf_file_name, name_of_file_with_patches)

    file_with_patches = h5py.File(name_of_file_with_patches)
    hdf_file = h5py.File(hdf_file_name)
    particles_name = get_particles_name(hdf_file)
    hdf_datasets = Collect_Particles_Groups(particles_name)
    file_with_patches.visititems(hdf_datasets)
    for particles_group in hdf_datasets.particles_groups:
        add_patch_to_particle_group(particles_group)


class Particles_groups():
    """ Collect values from datasets in hdf file """

    def __init__(self, particles_name):
        self.particles_groups = []
        self.positions = []
        self.name_particles = particles_name

    def __call__(self, name, node):
        if isinstance(node, h5py.Group):
            name_idx = node.name.find(self.name_particles)
            if name_idx != -1:
                group_particles_name = node.name[name_idx + len(self.name_particles) + 1:]
                if group_particles_name.endswith('position'):
                    self.positions.append(node)
                if group_particles_name.find('/') == -1 and len(group_particles_name) != 0:
                    self.particles_groups.append(node)
        return None

class Particles_data():

    def __init__(self, list_x, splitting_x, max_x, min_x, list_y, splitting_y, max_y, min_y,
                 list_z=None, splitting_z=None, max_z=None, min_z=None):
        self.x_coord = list_x
        self.y_coord = list_y
        self.z_coord = list_z
        self.x_split = splitting_x
        self.y_split = splitting_y
        self.z_split = splitting_z
        self.x_max_coord = max_x
        self.y_max_coord = max_y
        self.z_max_coord = max_z
        self.x_min_coord = min_x
        self.y_min_coord = min_y
        self.z_min_coord = min_z
   
    def get_size_split(self):
        size = 0
        if self.z_split == None:
            size = self.x_split * self.y_split
        else:
            size = self.x_split * self.y_split * self.z_split
        return size

    def get_array_lenght(self):
        return len(self.x_coord)

    def get_patch_x(self, i):
        return get_positon(self.x_max_coord, self.x_min_coord, self.x_split, self.x_coord[i])

    def get_patch_y(self, i):
        return get_positon(self.y_max_coord, self.y_min_coord, self.y_split, self.y_coord[i])

    def get_patch_z(self, i):
        return get_positon(self.z_max_coord, self.z_min_coord, self.z_split, self.z_coord[i])

def add_patch_to_particle_group(group):

    patch_group = group.require_group('ParticlePatches')
    extent_group = patch_group.require_group('extent')
    offset_group = patch_group.require_group('offset')



def get_positon(max_coord, min_coord, separator, x_current):
    lenght = max_coord - min_coord
    return min(int((x_current - min_coord) * separator / lenght), separator - 1)


def get_particles_name(hdf_file):

    particles_name = ''
    if hdf_file.attrs.get('particlesPath') != None:
        particles_name = hdf_file.attrs.get('particlesPath')
    else:
        particles_name = 'particles'
    particles_name = decode_name(particles_name)
    return particles_name


def decode_name(attribute_name):
    """ Decode name from binary """

    decoding_name = attribute_name.decode('ascii', errors='ignore')
    decoding_name = re.sub(r'\W+', '', decoding_name)
    return decoding_name


def add_patches(hdf_file, hdf_file_with_patches, grid_sizes, devices_number):
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
            OpenPMD_add_patches(hdf_file, name_of_file_with_patches, grid_sizes, devices_number)
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
    add_patches(args.hdf, args.result, args.gridSize, args.devicesNumber)



