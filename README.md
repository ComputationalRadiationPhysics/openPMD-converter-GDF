# Converter between openPMD and GDF

## About

This software provides conversion between [files in openPMD schema](https://github.com/openPMD/openPMD-standard) and the [GDF file format](http://www.pulsar.nl/gpt/).

The software is written in python 3, python 2 is not supported. For the rest of the document by `python` we assume some `python3` binary.

## Installing

Clone this repository by running
```bash
git clone https://github.com/ComputationalRadiationPhysics/openPMD-converter-GDF.git
```
or download an archive from the [github page](https://github.com/ComputationalRadiationPhysics/openPMD-converter-GDF) and unpack it.

The primary dependency is [openPMD-api](https://github.com/openPMD/openPMD-api), which can be installed e.g., by
```bash
python -m pip install -r requirements.txt
```
or for `conda` users via
```bash
conda install -c conda-forge openpmd-api matplotlib h5py
```

For further installation methods please see the [full list of available installation methods](https://openpmd-api.readthedocs.io/en/latest/install/install.html).

## Converting from GDF to openPMD

To convert a GDF file into an openPMD formatted file run the ```gdf_to_openPMD.py``` module as follows:
```bash
python gdf_to_openPMD -gdf_file gdf_file -openPMD_output file_in_openPMD_format
```
where 
* `gdf_file` is the path to an input GDF file;
* `file_in_openPMD_format` is the path to an output openPMD-format file.

The format is selected according to the file extension: current supported: `.h5` (HDF5), `.bp` (ADIOS1) or `.json` (JSON).

### Example

To run the script for the provided examples, run the following from a project directory:
```bash
python3 gdf_to_openPMD.py -gdf examples/example_1.gdf -openPMD_output examples/example_1.h5
python3 gdf_to_hdf.py -gdf examples/example_1.gdf -gdf examples/example_1.bp
```

## Converting from openPMD to GDF

To convert an openPMD-format file into a GDF file run the `openPMD_to_gdf.py` module as follows:
```bash
python3 openPMD_to_gdf.py -openPMD_input openPMD-format_file -gdf gdf_file -species (optional)
```
where parameters
* `-openPMD_input` is the path to an input openPMD format file; 
* `-gdf` is the path to an output GDF file, by default `openPMD_input path + .cgf`
* `-species` chosen particle species.

The format is selected according to the file extension: current supported: `.h5` (HDF5), `.bp` (ADIOS1) or `.json` (JSON).

### Example

To run the script for the provided example, run the following from a project directory:
```bash
python3 openPMD_to_gdf.py -openPMD_input examples/example_3.h5 -gdf examples/result4.gdf
```

### Limitations

Convertor does not work with datasets larger than 268435455, because of GDF standard limitations.

