def main(file_names):
    gdf_path, hdf_path = files_from_args(file_names)
    converter(hdf_path, gdf_path)


if __name__ == "__main__":
    file_names = sys.argv
    main(file_names)
