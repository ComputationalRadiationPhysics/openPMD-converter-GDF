# Converter between openPMD and GDF

## About

This software provides conversion between [hdf5 files](https://www.hdfgroup.org/HDF5/) marked up in [openPMD](https://github.com/openPMD/openPMD-standard)-conforming way and the [GDF file format](http://www.pulsar.nl/gpt/).

The software is written in python 3, python 2 is not supported. For the rest of the document by ```python``` we assume some python3 binary.

## Installing

Clone this repository by running
```
git clone https://github.com/ComputationalRadiationPhysics/openPMD-converter-GDF.git
```
or download an archive from the [github page](https://github.com/ComputationalRadiationPhysics/openPMD-converter-GDF) and unpack it.

The only dependency is [h5py](https://www.h5py.org/), which can be installed e.g., by
```
pip install h5py
```

## Converting from GDF to openPMD

To convert a GDF file into an openPMD-conforming hdf5 file run the ```gdf_to_hdf.py``` module as follows:
```
python gdf_to_hdf gdf_file [hdf5_file]
```
where 
* ```gdf_file``` is the path to an input GDF file;
* ```hdf5_file``` is the path to an output openPMD-conforming hdf5 file, by default ```result.h5```.

### Example

To run the script for the provided examples, run the following from a project directory:
```
python3 gdf_to_hdf.py -gdf examples/example_1.gdf -hdf examples/example_1.h5
python3 gdf_to_hdf.py -hdf examples/example_2.gdf -gdf examples/example_2.h5
```

## Converting from openPMD to GDF

To convert an openPMD-conforming hdf5 file into a GDF file run the ```hdf_to_gdf.py``` module as follows:
```
python3 hdf_to_gdf.py -hdf hdf5_file -gdf [gdf_file] -species (optional)
```
where parameters
* ```-hdf``` is the path to an input openPMD-conforming hdf5 file; 
* ```-gdf``` is the path to an output GDF file, by default ```hdf path + .cgf```
* ```-species``` chosen particle species.


### Example

To run the script for the provided example, run the following from a project directory:
```
python3 hdf_to_gdf.py examples/example_3.h5 examples/example_3.gdf

```
