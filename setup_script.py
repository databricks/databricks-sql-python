import subprocess
import os

def build_and_install_library(directory_name):

    previous_directory = os.getcwd()
    # Change directory to one level down
    os.chdir(directory_name)

    # Build the library using Poetry
    subprocess.run(['poetry', 'build'], check=True)

    # Get the name of the built .whl file
    dist_dir = 'dist'
    whl_files = [f for f in os.listdir(dist_dir) if f.endswith('.whl')]
    if not whl_files:
        raise FileNotFoundError("No .whl file found in the dist directory")

    # Install the package using pip3
    whl_file = os.path.join(dist_dir, whl_files[0])
    subprocess.run(['pip3', 'install', whl_file, '--force-reinstall'], check=True)

    os.chdir(previous_directory)



if __name__ == "__main__":
    # build_and_install_library("databricks_sql_connector_core")
    build_and_install_library("databricks_sql_connector")
    # build_and_install_library("databricks_sqlalchemy")