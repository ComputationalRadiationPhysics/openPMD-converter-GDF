# converter between openPMD and GDF

## About

This script convert from openPMD format to GDF format. Written in python 3.

openPMD standart: https://github.com/openPMD/openPMD-standard



## Usage

This script written in python 3, so does not work on python 2. 
dependence: h5py
```
pip install h5py
```
## Convert from GDF to openPMD

```
python3 gdf_to_hdf gdf_file hdf_file
```
Covert from gdf file to hdf file. 

gdf_file -- gdf file

hdf_file -- result hdf file

### Example

```
gdf_to_hdf examples/example_1.gdf examples/result.hdf

```

## Convert from openPMD to GDF

```
python3 hdf_to_gdf hdf_file gdf_file
```
Covert from hdf file to gdf file. 

hdf_file -- hdf file

gdf_file --  result gdf file


### Example

```
gdf_to_hdf examples/examle_3.hdf examples/result.gdf

```



<!--- Скрипт на 3 питоне, на втором не работает. -->
<!--- зависимости, как поставить --->
<!--- 1 сценарий: строка запуска пример, что делает параметры. Дальше - примеры из папки примеров --->

<!--- 2 сценарий: строка запуска, параметры, пример. --->


